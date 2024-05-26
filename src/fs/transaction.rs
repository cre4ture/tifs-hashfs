use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut, Range, RangeInclusive};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use fuser::{FileAttr, FileType};
use futures::future::join_all;
use futures::FutureExt;
use num_format::{Buffer, Locale};
use tikv_client::{Backoff, Key, KvPair, RetryOptions, Transaction, TransactionOptions};
use tracing::{debug, instrument, trace};
use tikv_client::transaction::Mutation;
use uuid::Uuid;

use crate::fs::meta::StaticFsParameters;
use crate::fs::utils::stop_watch::AutoStopWatch;

use super::block::empty_block;
use super::dir::Directory;
use super::error::{FsError, Result};
use super::file_handler::FileHandler;
use super::hash_block::block_splitter::{BlockSplitterRead, BlockSplitterWrite};
use super::hash_block::helpers::UpdateIrregularBlock;
use super::index::Index;
use super::inode::{self, BlockAddress, Inode, TiFsHash};
use super::key::{ScopedKeyBuilder, ScopedKeyKind, ROOT_INODE};
use super::meta::Meta;
use super::mode::{as_file_kind, as_file_perm, make_mode};
use super::parsers;
use super::reply::{DirItem, StatFs};
use super::tikv_fs::{TiFsCaches, TiFsConfig, DIR_PARENT, DIR_SELF};
use super::transaction_client_mux::TransactionClientMux;


pub const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(300, 1000, 100);
pub const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(30, 500, 1000);
pub const PESSIMISTIC_BACKOFF: Backoff = Backoff::no_backoff();

pub struct Txn<'ol> {
    key_builder: &'ol ScopedKeyBuilder<'ol>,
    _instance_id: Uuid,
    txn: Transaction,
    raw: &'ol tikv_client::RawClient,
    fs_config: TiFsConfig,
    block_size: u64,            // duplicate of fs_config.block_size. Keep it to avoid refactoring efforts.
    max_blocks: Option<u64>,
    max_name_len: u32,
    caches: TiFsCaches,
}

impl<'a> Txn<'a> {
    const INLINE_DATA_THRESHOLD_BASE: u64 = 1 << 4;

    fn inline_data_threshold(&self) -> u64 {
        self.block_size / Self::INLINE_DATA_THRESHOLD_BASE
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    fn check_space_left(&self, meta: &Meta) -> Result<()> {
        match meta.last_stat {
            Some(ref stat) if stat.bavail == 0 => {
                Err(FsError::NoSpaceLeft(stat.bsize as u64 * stat.blocks))
            }
            _ => Ok(()),
        }
    }

    pub async fn begin_optimistic(
        key_builder: &'a ScopedKeyBuilder<'a>,
        instance_id: Uuid,
        client: &TransactionClientMux,
        raw: &'a tikv_client::RawClient,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
        max_name_len: u32,
    ) -> Result<Self> {
        let options = TransactionOptions::new_optimistic().use_async_commit();
        let options = options.retry_options(RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        });
        let txn: Transaction = client.give_one()
            .begin_with_options(options)
            .await?;
        Ok(Txn {
            key_builder,
            _instance_id: instance_id,
            txn,
            raw,
            fs_config: fs_config.clone(),
            block_size: fs_config.block_size,
            max_blocks: fs_config.max_size.map(|size| size / fs_config.block_size as u64),
            max_name_len,
            caches,
        })
    }

    pub async fn open(&mut self, ino: u64, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.read_inode(ino).await?;
        // publish opened state
        let key = self.key_builder.opened_inode(ino, use_id);
        self.put(key, &[]).await?;
        eprintln!("open-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn close(&mut self, ino: u64, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.read_inode(ino).await?;
        // de-publish opened state
        let key = self.key_builder.opened_inode(ino, use_id);
        self.delete(key).await?;
        eprintln!("close-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn read(&mut self, ino: u64, start: u64, size: u32) -> Result<Vec<u8>> {
        self.read_data(ino, start, Some(size as u64), self.fs_config.enable_atime).await
    }

    pub async fn write(&mut self, ino: u64, handler: FileHandler, offset: i64, data: Bytes) -> Result<usize> {
        let start = handler.cursor as i64 + offset;
        if start < 0 {
            return Err(FsError::InvalidOffset { ino, offset: start });
        }

        self.write_data(ino, start as u64, data).await
    }

    pub async fn reserve_new_ino(&mut self) -> Result<u64> {
        let mut meta = self
            .read_meta()
            .await?
            .unwrap_or_else(|| Meta::new(self.block_size as u64, StaticFsParameters{
                hashed_blocks: self.fs_config.hashed_blocks
            }));

        self.check_space_left(&meta)?;

        let ino = meta.inode_next;
        meta.inode_next += 1;

        debug!("get ino({})", ino);
        self.save_meta(&meta).await?;
        Ok(ino)
    }

    pub fn initialize_inode(block_size: u64, ino: u64, mode: u32, gid: u32, uid: u32, rdev: u32,) -> Inode {
        let file_type = as_file_kind(mode);
        let inode = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: file_type,
            perm: as_file_perm(mode),
            nlink: 1,
            uid,
            gid,
            rdev,
            blksize: block_size as u32,
            flags: 0,
        }
        .into();

        debug!("made inode ({:?})", &inode);
        inode
    }

    pub async fn check_if_dir_entry_exists(&mut self, parent: u64, name: &ByteString) -> Result<bool> {
        Ok(self.get_index(parent, name.clone()).await?.is_some())
    }

    pub async fn connect_inode_to_directory(&mut self, parent: u64, name: &ByteString, inode: &Inode) -> Result<()> {
        if parent >= ROOT_INODE {
            if self.check_if_dir_entry_exists(parent, name).await? {
                return Err(FsError::FileExist {
                    file: name.to_string(),
                });
            }
            self.set_index(parent, name.clone(), inode.ino).await?;

            let mut dir = self.read_dir(parent).await?;
            debug!("read dir({:?})", &dir);

            dir.push(DirItem {
                ino: inode.ino,
                name: name.to_string(),
                typ: inode.kind,
            });

            self.save_dir(parent, &dir).await?;
            // TODO: update attributes of directory
        }
        Ok(())
    }

    pub async fn make_inode(
        &mut self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        rdev: u32,
    ) -> Result<Arc<Inode>> {
        let ino = self.reserve_new_ino().await?;

        let inode = Self::initialize_inode(self.block_size, ino, mode, gid, uid, rdev);

        self.connect_inode_to_directory(parent, &name, &inode).await?;

        let ptr = Arc::new(inode);

        self.save_inode(&ptr).await?;
        Ok(ptr)
    }

    pub async fn get_index(&mut self, parent: u64, name: ByteString) -> Result<Option<u64>> {
        let key = self.key_builder.index(parent, &name);
        self.get(key)
            .await
            .map_err(FsError::from)
            .and_then(|value| {
                value
                    .map(|data| Ok(Index::deserialize(&data)?.ino))
                    .transpose()
            })
    }

    pub async fn set_index(&mut self, parent: u64, name: ByteString, ino: u64) -> Result<()> {
        let key = self.key_builder.index(parent, &name);
        let value = Index::new(ino).serialize()?;
        Ok(self.put(key, value).await?)
    }

    pub async fn remove_index(&mut self, parent: u64, name: ByteString) -> Result<()> {
        let key = self.key_builder.index(parent, &name);
        Ok(self.delete(key).await?)
    }

    pub async fn read_inode_uncached(&mut self, ino: u64) -> Result<Arc<Inode>> {
        let key = self.key_builder.inode(ino);
        let value = self
            .get(key)
            .await?
            .ok_or(FsError::InodeNotFound { inode: ino })?;
        let inode = Arc::new(Inode::deserialize(&value)?);
        self.caches.inode.insert(ino, (Instant::now(), inode.clone())).await;
        Ok(inode)
    }

    pub async fn read_inode(&mut self, ino: u64) -> Result<Arc<Inode>> {
        if let Some((time, value)) = self.caches.inode.get(&ino).await {
            if time.elapsed().as_secs() < 2 {
                return Ok(value);
            } else {
                self.caches.inode.remove(&ino).await;
            }
        }

        self.read_inode_uncached(ino).await
    }

    pub async fn save_inode(&mut self, inode: &Arc<Inode>) -> Result<()> {
        let key = self.key_builder.inode(inode.ino);

        if inode.nlink == 0 && inode.opened_fh == 0 {
            self.delete(key).await?;
            self.caches.inode.remove(&inode.ino).await;
        } else {
            self.put(key, inode.serialize()?).await?;
            self.caches.inode.insert(inode.ino, (Instant::now(), inode.clone())).await;
            debug!("save inode: {:?}", inode);
        }
        Ok(())
    }

    pub async fn remove_inode(&mut self, ino: u64) -> Result<()> {
        let key = self.key_builder.inode(ino);
        self.delete(key).await?;
        self.caches.inode.remove(&ino).await;
        Ok(())
    }

    pub async fn read_meta(&mut self) -> Result<Option<Meta>> {
        let key = self.key_builder.meta();
        let opt_data = self.get(key).await?;
        opt_data.map(|data| Meta::deserialize(&data)).transpose()
    }

    pub async fn save_meta(&mut self, meta: &Meta) -> Result<()> {
        let key = self.key_builder.meta();
        self.put(key, meta.serialize()?).await?;
        Ok(())
    }

    async fn transfer_inline_data_to_block(&mut self, inode: &mut Inode) -> Result<()> {
        debug_assert!(inode.size <= self.inline_data_threshold());
        let key = self.key_builder.block(inode.ino, 0);
        let mut data = inode.inline_data.clone().unwrap();
        data.resize(self.block_size as usize, 0);
        self.put(key, data).await?;
        inode.inline_data = None;
        Ok(())
    }

    async fn write_inline_data(
        &mut self,
        inode: &mut Inode,
        start: u64,
        data: &[u8],
    ) -> Result<usize> {
        debug_assert!(inode.size <= self.inline_data_threshold());
        let size = data.len() as u64;
        debug_assert!(
            start + size <= self.inline_data_threshold(),
            "{} + {} > {}",
            start,
            size,
            self.inline_data_threshold()
        );

        let size = data.len();
        let start = start as usize;

        let mut inlined = inode.inline_data.take().unwrap_or_else(Vec::new);
        if start + size > inlined.len() {
            inlined.resize(start + size, 0);
        }
        inlined[start..start + size].copy_from_slice(data);

        inode.atime = SystemTime::now();
        inode.mtime = SystemTime::now();
        inode.ctime = SystemTime::now();
        inode.set_size(inlined.len() as u64, self.block_size);
        inode.inline_data = Some(inlined);
        self.save_inode(&Arc::new(inode.clone())).await?;

        Ok(size)
    }

    async fn read_inline_data(
        &mut self,
        inode: &Inode,
        start: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        debug_assert!(inode.size <= self.inline_data_threshold());

        let start = start as usize;
        let size = size as usize;

        let inlined = inode.inline_data.as_ref().unwrap();
        debug_assert!(inode.size as usize == inlined.len());
        let mut data = vec![0; size];
        if inlined.len() > start {
            let to_copy = size.min(inlined.len() - start);
            data[..to_copy].copy_from_slice(&inlined[start..start + to_copy]);
        }
        Ok(data)
    }

    async fn hb_read_data(&mut self, ino: u64, start: u64, size: u64) -> Result<Vec<u8>> {

        let bs = BlockSplitterRead::new(self.block_size, start, size);
        let block_range = bs.block_range();

        let block_hashes = self.hb_get_block_hash_list_by_block_range(ino, block_range.clone()).await?;
        //eprintln!("block_hashes(count: {}): {:?}", block_hashes.len(), block_hashes);
        let block_hashes_set = HashSet::from_iter(block_hashes.values().cloned());
        let blocks_data = if self.fs_config.raw_hashed_blocks {
            parsers::hb_get_block_data_by_hashes(&self.fs_config, &self.caches, self.key_builder, &block_hashes_set,
                |keys| {
                    self.raw.batch_get(keys).boxed()
                }).await?
        } else {
            self.hb_get_block_data_by_hashes(&block_hashes_set).await?
        };

        let result = parsers::hb_read_from_blocks(ino, &block_range, &bs, &block_hashes, &blocks_data)?;

        let mut buf_start = Buffer::default();
        buf_start.write_formatted(&start, &Locale::en);
        eprintln!("hb_read_data(ino: {ino}, start:{buf_start}, size: {size}) - block_size: {}, blocks_count: {}, range: [{}..{}[ -> {} read", bs.block_size, bs.block_count, block_range.start, block_range.end, result.len());

        if result.len() < size as usize {
            let block_data_lengths = blocks_data.iter().map(|(key, data)|(key, data.len())).collect::<Vec<_>>();
            eprintln!("incomplete read - (ino: {ino}, start:{buf_start}, size: {size}): len:{}, block_hashes:{:?}, block_lengths:{:?}", result.len(), block_hashes, block_data_lengths);
        }

        Ok(result)
    }

    async fn read_data_traditional(&mut self, ino: u64, start: u64, size: u64) -> Result<Vec<u8>> {
        let target = start + size;
        let start_block = start / self.block_size as u64;
        let end_block = (target + self.block_size as u64 - 1) / self.block_size as u64;

        let block_range = self.key_builder.block_range(ino, start_block..end_block);
        let pairs = self
            .scan(
                block_range,
                (end_block - start_block) as u32,
            )
            .await?;

        let mut data = pairs
            .enumerate()
            .flat_map(|(i, pair)| {
                let key = if let Ok(ScopedKeyKind::Block { ino: _, block }) =
                    self.key_builder.parse(pair.key().into()).map(|x|x.key_type)
                {
                    block
                } else {
                    unreachable!("the keys from scanning should be always valid block keys")
                };
                let value = pair.into_value();
                (start_block as usize + i..key as usize)
                    .map(|_| empty_block(self.block_size))
                    .chain(vec![value])
            })
            .enumerate()
            .fold(
                Vec::with_capacity(
                    ((end_block - start_block) * self.block_size - start % self.block_size)
                        as usize,
                ),
                |mut data, (i, value)| {
                    let mut slice = value.as_slice();
                    if i == 0 {
                        slice = &slice[(start % self.block_size) as usize..]
                    }

                    data.extend_from_slice(slice);
                    data
                },
            );

        data.resize(size as usize, 0);
        Ok(data)
    }

    pub async fn read_data(
        &mut self,
        ino: u64,
        start: u64,
        chunk_size: Option<u64>,
        update_atime: bool,
    ) -> Result<Vec<u8>> {
        let attr = self.read_inode(ino).await?;
        if start >= attr.size {
            return Ok(Vec::new());
        }

        let max_size = attr.size - start;
        let size = chunk_size.unwrap_or(max_size).min(max_size);

        if update_atime {
            let mut attr = attr.deref().clone();
            attr.atime = SystemTime::now();
            self.save_inode(&Arc::new(attr)).await?;
        }

        if attr.inline_data.is_some() {
            return self.read_inline_data(&attr, start, size).await;
        }

        if self.fs_config.hashed_blocks {
            self.hb_read_data(ino, start, size).await
        } else {
            self.read_data_traditional(ino, start, size).await
        }
    }

    pub async fn clear_data(&mut self, ino: u64) -> Result<u64> {
        let mut attr = self.read_inode(ino).await?.deref().clone();
        let end_block = (attr.size + self.block_size - 1) / self.block_size;

        for block in 0..end_block {
            let key = self.key_builder.block(ino, block);
            self.delete(key).await?;
        }

        let clear_size = attr.size;
        attr.size = 0;
        attr.atime = SystemTime::now();
        self.save_inode(&Arc::new(attr)).await?;
        Ok(clear_size)
    }

    pub async fn write_blocks_traditional(&mut self, ino: u64, start: u64, data: &Bytes) -> Result<()> {

        let mut block_index = start / self.block_size;
        let start_key = self.key_builder.block(ino, block_index);
        let start_index = (start % self.block_size) as usize;

        let first_block_size = self.block_size as usize - start_index;

        let (first_block, mut rest) = data.split_at(first_block_size.min(data.len()));

        let mut start_value = self
            .get(start_key)
            .await?
            .unwrap_or_else(|| empty_block(self.block_size));

        start_value[start_index..start_index + first_block.len()].copy_from_slice(first_block);

        self.put(start_key, start_value).await?;

        while !rest.is_empty() {
            block_index += 1;
            let key = self.key_builder.block(ino, block_index);
            let (curent_block, current_rest) =
                rest.split_at((self.block_size as usize).min(rest.len()));
            let mut value = curent_block.to_vec();
            if value.len() < self.block_size as usize {
                let mut last_value = self
                    .get(key)
                    .await?
                    .unwrap_or_else(|| empty_block(self.block_size));
                last_value[..value.len()].copy_from_slice(&value);
                value = last_value;
            }
            self.put(key, value).await?;
            rest = current_rest;
        }

        Ok(())
    }

    pub async fn hb_get_block_hash_list_by_block_range(&mut self, ino: u64, block_range: Range<u64>) -> Result<HashMap<BlockAddress, inode::Hash>>
    {
        Ok(parsers::hb_get_block_hash_list_by_block_range(
            &self.key_builder, ino, block_range, |range, cnt| {
                self.scan(range, cnt).map(|x| {
                    x.map(|y|y.collect::<Vec<_>>())
                }).boxed()
        }).await?)
    }

    pub async fn hb_get_block_data_by_hashes(&mut self, hash_list: &HashSet<TiFsHash>) -> Result<HashMap<inode::Hash, Arc<Vec<u8>>>>
    {
        Ok(parsers::hb_get_block_data_by_hashes(
            &self.fs_config.clone(),
            &self.caches.clone(),
            &self.key_builder,
            hash_list,
            |keys| {
                self.txn.batch_get(keys).map(|r| {
                    r.map(|iter| {
                        iter.collect::<Vec<_>>()
                    })
                }).boxed()
            }).await?)
    }

    pub async fn hb_write_data(&mut self, inode: &mut Inode, start: u64, data: &Bytes) -> Result<bool> {

        let mut watch = AutoStopWatch::start("hb_wrt");
        let bs = BlockSplitterWrite::new(self.block_size, start, &data);
        let block_range = bs.get_range();

        let hash_list_prev = self.hb_get_block_hash_list_by_block_range(inode.ino, block_range.clone()).await?;
        let input_block_hashes = hash_list_prev.len();
        watch.sync("hp");

        let mut pre_data_hash_request = HashSet::<inode::Hash>::new();
        let first_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            inode.ino, bs.first_data, bs.first_data_start_position, &hash_list_prev, &mut pre_data_hash_request
        );
        let last_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            inode.ino, bs.last_data, 0, &hash_list_prev, &mut pre_data_hash_request
        );

        let pre_data = self.hb_get_block_data_by_hashes(&pre_data_hash_request).await?;
        watch.sync("pd");
        let mut new_blocks = HashMap::new();
        let mut new_block_hashes = HashMap::new();

        first_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);
        last_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);

        for (index, chunk) in bs.mid_data.data.chunks(self.block_size as usize).enumerate() {
            let hash = blake3::hash(chunk);
            new_blocks.insert(hash, Arc::new(chunk.to_vec()));
            new_block_hashes.insert(BlockAddress{ino: inode.ino, index: bs.mid_data.block_index + index as u64}, hash);
        }
        watch.sync("h");

        for (k, new_block) in &new_blocks {
            self.caches.block.insert(*k, new_block.clone()).await;
        }
        watch.sync("ca");

        if self.fs_config.validate_writes {
            let ks = new_blocks.keys().map(|x|x.to_owned()).collect::<Vec<_>>();
            for hash in ks {
                let key = self.key_builder.hashed_block(&hash);
                let key_range: RangeInclusive<Key> = key.into()..=key.into();
                let result = if self.fs_config.raw_hashed_blocks {
                    self.raw.scan_keys(key_range, 1).await?
                } else {
                    self.scan_keys(key_range, 1).await?.collect()
                };
                if result.len() > 0 {
                    new_blocks.remove(&hash);
                }
            }
            watch.sync("exv");
        } else {
            let exists_keys_request = new_blocks.keys().map(|k| self.key_builder.hashed_block_exists(k)).collect::<Vec<_>>();
            let exists_keys_response = self.batch_get(exists_keys_request).await?.collect::<Vec<_>>();
            for KvPair(key, _) in exists_keys_response.into_iter() {
                let key = (&key).into();
                let hash = self.key_builder.parse_key_hashed_block(key).ok_or(FsError::UnknownError("failed parsing hash from response".into()))?;
                new_blocks.remove(&hash);
            }
            watch.sync("exb");
        }

        let mut mutations_txn = Vec::<Mutation>::new();
        let mut mutations_raw = Vec::<KvPair>::new();
        // upload new blocks:
        let written_blocks = new_blocks.len();
        for (k, new_block) in new_blocks {
            if self.fs_config.raw_hashed_blocks {
                mutations_raw.push(KvPair(self.key_builder.hashed_block(&k).into(), new_block.deref().clone()));
            } else {
                mutations_txn.push(Mutation::Put(self.key_builder.hashed_block(&k).into(), new_block.deref().clone()));
            }
            mutations_txn.push(Mutation::Put(self.key_builder.hashed_block_exists(&k).into(), vec![]));
        }

        let raw_cnt = mutations_raw.len();
        if raw_cnt > 0 {
            if self.fs_config.batch_raw_block_write {
                self.raw.batch_put_with_ttl(mutations_raw, vec![0; raw_cnt]).await
                    .map_err(|e| {
                        eprintln!("batch-hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,({input_block_hashes} skipped),wr_blk:{raw_cnt}raw/?-err:{e:?}", inode.ino, start, data.len(), self.fs_config.block_size, block_range.end - block_range.start, block_range.start, block_range.end);
                        e
                    })?;
            } else {
                let futures = mutations_raw.into_iter().map(|KvPair(k,v)| {
                    self.raw.put(k, v)
                });
                for result in join_all(futures).await {
                    result.map_err(|e| {
                        eprintln!("single-hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,({input_block_hashes} skipped),wr_blk:{raw_cnt}raw/?-err:{e:?}", inode.ino, start, data.len(), self.fs_config.block_size, block_range.end - block_range.start, block_range.start, block_range.end);
                        e
                    })?;
                }
            }
            watch.sync("rawbp");
        }

        // remove outdated blocks:
        // TODO!

        // filter out unchanged blocks:
        let mut skipped_new_block_hashes = 0;
        for (address, prev_block_hash) in hash_list_prev.iter() {
            if let Some(new_block_hash) = new_block_hashes.get(&address) {
                if prev_block_hash == new_block_hash {
                    new_block_hashes.remove(&address);
                    skipped_new_block_hashes += 1;
                }
            }
        }

        // write new block mapping:
        let written_block_hashes = new_block_hashes.len();
        for (k, new_hash) in new_block_hashes {
            mutations_txn.push(Mutation::Put(self.key_builder.block_hash(k).into(), new_hash.as_bytes().to_vec()));
        }

        watch.sync("pm");

        // execute all
        let was_modified = mutations_txn.len() > 0;
        if was_modified {
            self.batch_mutate(mutations_txn).await?;
            watch.sync("m");
        }

        eprintln!("hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,wr_bl_hash:{written_block_hashes}({skipped_new_block_hashes}/{input_block_hashes} skipped),wr_blk:{raw_cnt}raw/{written_blocks}", inode.ino, start, data.len(), bs.block_size, block_range.end - block_range.start, block_range.start, block_range.end);

        Ok(was_modified)
    }

    #[instrument(skip(self, data))]
    pub async fn write_data(&mut self, ino: u64, start: u64, data: Bytes) -> Result<usize> {
        let mut watch = AutoStopWatch::start("write_data");
        debug!("write data at ({})[{}]", ino, start);
        //let meta = self.read_meta().await?.unwrap(); // TODO: is this needed?
        //self.check_space_left(&meta)?;

        let mut inode = self.read_inode(ino).await?.deref().clone();
        let size = data.len();
        let target = start + size as u64;
        watch.sync("read_inode");

        if inode.inline_data.is_some() && target > self.inline_data_threshold() {
            self.transfer_inline_data_to_block(&mut inode).await?;
            watch.sync("transfer_inline");
        }

        if (inode.inline_data.is_some() || inode.size == 0)
            && target <= self.inline_data_threshold()
        {
            let result = self.write_inline_data(&mut inode, start, &data).await;
            watch.sync("write_inline");
            return result;
        }

        let content_was_modified = if self.fs_config.hashed_blocks {
            self.hb_write_data(&mut inode, start, &data).await?
        } else {
            self.write_blocks_traditional(ino, start, &data).await?;
            true
        };

        watch.sync("write impl");

        let size_changed = inode.size < target;
        if size_changed || (self.fs_config.enable_mtime && content_was_modified) {
            inode.atime = SystemTime::now();
            inode.mtime = SystemTime::now();
            // inode.ctime = SystemTime::now(); TODO: bug?
            inode.set_size(inode.size.max(target), self.block_size);
            self.save_inode(&Arc::new(inode)).await?;
            watch.sync("save inode");
        }
        Ok(size)
    }

    pub fn set_fresh_inode_to_link(inode: &mut Inode, data: Bytes) {
        debug_assert!(inode.file_attr.kind == FileType::Symlink);
        inode.inline_data = Some(data.to_vec());
        inode.size = data.len() as u64;
        inode.blocks = 1;
    }

    pub async fn write_link(&mut self, inode: &mut Inode, data: Bytes) -> Result<usize> {
        debug_assert!(inode.file_attr.kind == FileType::Symlink);
        inode.inline_data = None;
        inode.set_size(0, self.block_size);
        self.write_inline_data(inode, 0, &data).await
    }

    pub async fn read_link(&mut self, ino: u64) -> Result<Vec<u8>> {
        let inode = self.read_inode(ino).await?;
        debug_assert!(inode.file_attr.kind == FileType::Symlink);
        let size = inode.size;
        if self.fs_config.enable_atime {
            let mut inode = inode.deref().clone();
            inode.atime = SystemTime::now();
            self.save_inode(&Arc::new(inode)).await?;
        }
        self.read_inline_data(&inode, 0, size).await
    }

    pub async fn link(&mut self, ino: u64, newparent: u64, newname: ByteString) -> Result<Arc<Inode>> {
        if let Some(old_ino) = self.get_index(newparent, newname.clone()).await? {
            let inode = self.read_inode(old_ino).await?;
            match inode.kind {
                FileType::Directory => self.rmdir(newparent, newname.clone()).await?,
                _ => self.unlink(newparent, newname.clone()).await?,
            }
        }
        self.set_index(newparent, newname.clone(), ino).await?;

        let mut inode = self.read_inode(ino).await?.deref().clone();
        let mut dir = self.read_dir(newparent).await?;

        dir.push(DirItem {
            ino,
            name: newname.to_string(),
            typ: inode.kind,
        });

        self.save_dir(newparent, &dir).await?;
        inode.nlink += 1;
        inode.ctime = SystemTime::now();
        let ptr = Arc::new(inode);
        self.save_inode(&ptr).await?;
        Ok(ptr)
    }

    pub async fn unlink(&mut self, parent: u64, name: ByteString) -> Result<()> {
        match self.get_index(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(ino) => {
                self.remove_index(parent, name.clone()).await?;
                let parent_dir = self.read_dir(parent).await?;
                let new_parent_dir: Directory = parent_dir
                    .into_iter()
                    .filter(|item| item.name != *name)
                    .collect();
                self.save_dir(parent, &new_parent_dir).await?;

                let mut inode = self.read_inode(ino).await?.deref().clone();
                inode.nlink -= 1;
                inode.ctime = SystemTime::now();
                self.save_inode(&Arc::new(inode)).await?;
                Ok(())
            }
        }
    }

    pub async fn rmdir(&mut self, parent: u64, name: ByteString) -> Result<()> {
        match self.get_index(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(ino) => {
                if self
                    .read_dir(ino)
                    .await?
                    .iter()
                    .any(|i| DIR_SELF != i.name && DIR_PARENT != i.name)
                {
                    let name_str = name.to_string();
                    debug!("dir({}) not empty", &name_str);
                    return Err(FsError::DirNotEmpty { dir: name_str });
                }

                self.unlink(ino, DIR_SELF).await?;
                self.unlink(ino, DIR_PARENT).await?;
                self.unlink(parent, name).await
            }
        }
    }

    pub async fn lookup(&mut self, parent: u64, name: ByteString) -> Result<u64> {
        self.get_index(parent, name.clone())
            .await?
            .ok_or_else(|| FsError::FileNotFound {
                file: name.to_string(),
            })
    }

    pub async fn fallocate(&mut self, inode: &mut Inode, offset: i64, length: i64) -> Result<()> {
        let target_size = (offset + length) as u64;
        if target_size <= inode.size {
            return Ok(());
        }

        if inode.inline_data.is_some() {
            if target_size <= self.inline_data_threshold() {
                let original_size = inode.size;
                let data = vec![0; (target_size - original_size) as usize];
                self.write_inline_data(inode, original_size, &data).await?;
                return Ok(());
            } else {
                self.transfer_inline_data_to_block(inode).await?;
            }
        }

        inode.set_size(target_size, self.block_size);
        inode.mtime = SystemTime::now();
        self.save_inode(&Arc::new(inode.clone())).await?;
        Ok(())
    }

    pub async fn mkdir(
        &mut self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
    ) -> Result<Arc<Inode>> {
        let dir_mode = make_mode(FileType::Directory, mode as _);
        let inode = self.make_inode(parent, name, dir_mode, gid, uid, 0).await?;
        self.save_dir(inode.ino, &Directory::new()).await?;
        self.link(inode.ino, inode.ino, DIR_SELF).await?;
        if parent >= ROOT_INODE {
            self.link(parent, inode.ino, DIR_PARENT).await?;
        }
        self.read_inode(inode.ino).await
    }

    pub async fn read_dir(&mut self, ino: u64) -> Result<Directory> {
        let key = self.key_builder.block(ino, 0);
        let data = self
            .get(key)
            .await?
            .ok_or(FsError::BlockNotFound {
                inode: ino,
                block: 0,
            })?;
        trace!("read data: {}", String::from_utf8_lossy(&data));
        super::dir::decode(&data)
    }

    pub async fn save_dir(&mut self, ino: u64, dir: &[DirItem]) -> Result<Arc<Inode>> {
        let data = super::dir::encode(dir)?;
        let mut inode = self.read_inode(ino).await?.deref().clone();
        inode.set_size(data.len() as u64, self.block_size);
        inode.atime = SystemTime::now();
        inode.mtime = SystemTime::now();
        inode.ctime = SystemTime::now();
        let ptr = Arc::new(inode);
        self.save_inode(&ptr).await?;
        let key = self.key_builder.block(ino, 0);
        self.put(key, data).await?;
        Ok(ptr)
    }

    pub async fn statfs(&mut self) -> Result<StatFs> {
        let bsize = self.block_size as u32;
        let mut meta = self
            .read_meta()
            .await?
            .expect("meta should not be none after fs initialized");
        let next_inode = meta.inode_next;
        let range = self.key_builder.inode_range(ROOT_INODE..next_inode);
        let (used_blocks, files) = self
            .scan(
                range,
                (next_inode - ROOT_INODE) as u32,
            )
            .await?
            .map(|pair| Inode::deserialize(pair.value()))
            .try_fold((0, 0), |(blocks, files), inode| {
                Ok::<_, FsError>((blocks + inode?.blocks, files + 1))
            })?;
        let ffree = std::u64::MAX - next_inode;
        let bfree = match self.max_blocks {
            Some(max_blocks) if max_blocks > used_blocks => max_blocks - used_blocks,
            Some(_) => 0,
            None => std::u64::MAX,
        };
        let blocks = match self.max_blocks {
            Some(max_blocks) => max_blocks,
            None => used_blocks,
        };

        let stat = StatFs::new(
            blocks,
            bfree,
            bfree,
            files,
            ffree,
            bsize,
            self.max_name_len,
            0,
        );
        trace!("statfs: {:?}", stat);
        meta.last_stat = Some(stat.clone());
        self.save_meta(&meta).await?;
        Ok(stat)
    }
}

impl<'a> Deref for Txn<'a> {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl<'a> DerefMut for Txn<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}
