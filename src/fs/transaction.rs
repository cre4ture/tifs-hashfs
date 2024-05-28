use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
use std::mem;
use std::ops::{Deref, Range, RangeInclusive};
use std::sync::{Arc, Weak};
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use fuser::{FileAttr, FileType};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, TryFutureExt};
use multimap::MultiMap;
use num_format::{Buffer, Locale};
use tikv_client::{Backoff, BoundRange, Key, KvPair, RetryOptions, Timestamp, Transaction, TransactionOptions, Value};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, instrument, trace};
use tikv_client::transaction::Mutation;
use uuid::Uuid;

use crate::fs::meta::StaticFsParameters;
use crate::fs::utils::stop_watch::AutoStopWatch;

use super::block::empty_block;
use super::dir::Directory;
use super::error::{FsError, Result};
use super::file_handler::FileHandler;
use super::fs_config::TiFsConfig;
use super::hash_block::block_splitter::{BlockSplitterRead, BlockSplitterWrite};
use super::hash_block::helpers::UpdateIrregularBlock;
use super::hashed_block::HashedBlock;
use super::index::Index;
use super::inode::{self, BlockAddress, Inode, TiFsHash};
use super::key::{ScopedKeyBuilder, ScopedKeyKind, ROOT_INODE};
use super::meta::Meta;
use super::mode::{as_file_kind, as_file_perm, make_mode};
use super::parsers;
use super::reply::{DirItem, StatFs};
use super::tikv_fs::{TiFsCaches, DIR_PARENT, DIR_SELF};
use super::transaction_client_mux::TransactionClientMux;

use tikv_client::Result as TiKvResult;


pub const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(300, 1000, 100);
pub const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(30, 500, 1000);
pub const PESSIMISTIC_BACKOFF: Backoff = Backoff::no_backoff();

pub fn make_chunks_hash_map<K,V>(input: HashMap<K,V>, max_chunk_size: usize) -> VecDeque<HashMap<K,V>>
where
    K: std::cmp::Eq,
    K: std::hash::Hash,
{
    input.into_iter().fold(VecDeque::new(), |mut a,(k, v)|{
        let last = a.back_mut();
        if let Some(last) = last {
            if last.len() < max_chunk_size {
                last.insert(k, v);
                return a;
            }
        }
        let mut new_entry = HashMap::new();
        new_entry.insert(k, v);
        a.push_back(new_entry);
        a
    })
}

pub struct AsyncParallelPipeStage<F: Future + Send> {
    pub in_progress_limit: usize,
    pub in_progress: FuturesUnordered<JoinHandle<F::Output>>,
    pub results: VecDeque<<F as Future>::Output>,
    total: usize,
}

impl<F: Future + Send> AsyncParallelPipeStage<F>
where
    F: 'static,
    F::Output: Send + 'static,
{
    pub fn new(in_progress_limit: usize) -> Self {
        Self {
            in_progress_limit,
            in_progress: FuturesUnordered::new(),
            results: VecDeque::new(),
            total: 0
        }
    }

    async fn push(&mut self, future: F) {
        self.total += 1;
        self.in_progress.push(tokio::spawn(future));

        if self.in_progress.len() >= self.in_progress_limit {
            let done = self.in_progress.next().await.unwrap().unwrap();
            self.results.push_back(done);
        }
    }

    async fn wait_finish_all(&mut self) {
        for fut in mem::take(&mut self.in_progress).into_iter() {
            let done = fut.await.unwrap();
            self.results.push_back(done);
        }
    }

    fn get_total(&self) -> usize {
        self.total
    }
}

pub struct Txn {
    weak: Weak<Self>,
    _instance_id: Uuid,
    txn: Option<RwLock<Transaction>>,
    raw: Arc<tikv_client::RawClient>,
    raw_txn: Option<TxnArc>,
    fs_config: TiFsConfig,
    block_size: u64,            // duplicate of fs_config.block_size. Keep it to avoid refactoring efforts.
    max_blocks: Option<u64>,
    max_name_len: u32,
    caches: TiFsCaches,
}

pub type TxnArc = Arc<Txn>;

impl Txn {
    const INLINE_DATA_THRESHOLD_BASE: u64 = 1 << 4;

    fn inline_data_threshold(&self) -> u64 {
        self.block_size / Self::INLINE_DATA_THRESHOLD_BASE
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    fn check_space_left(self: TxnArc, meta: &Meta) -> Result<()> {
        match meta.last_stat {
            Some(ref stat) if stat.bavail == 0 => {
                Err(FsError::NoSpaceLeft(stat.bsize as u64 * stat.blocks))
            }
            _ => Ok(()),
        }
    }

    pub async fn begin_optimistic(
        instance_id: Uuid,
        client: &TransactionClientMux,
        raw: Arc<tikv_client::RawClient>,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
        max_name_len: u32,
    ) -> Result<TxnArc> {
        let txn = if fs_config.pure_raw { None } else {
            let options = TransactionOptions::new_optimistic().use_async_commit();
            let options = options.retry_options(RetryOptions {
                region_backoff: DEFAULT_REGION_BACKOFF,
                lock_backoff: OPTIMISTIC_BACKOFF,
            });
            Some(client.give_one().begin_with_options(options).await?)
        }.map(|x|RwLock::new(x));
        Ok(TxnArc::new_cyclic(|weak| { Self {
                weak: weak.clone(),
                _instance_id: instance_id,
                txn,
                raw: raw.clone(),
                raw_txn: Some(Txn::pure_raw(instance_id, raw, fs_config, caches.clone(), max_name_len)),
                fs_config: fs_config.clone(),
                block_size: fs_config.block_size,
                max_blocks: fs_config.max_size.map(|size| size / fs_config.block_size as u64),
                max_name_len,
                caches,
            }
        }))
    }

    pub fn pure_raw(
        instance_id: Uuid,
        raw: Arc<tikv_client::RawClient>,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
        max_name_len: u32,
    ) -> TxnArc {
        Arc::new_cyclic(|weak| Txn {
            weak: weak.clone(),
            _instance_id: instance_id,
            txn: None,
            raw,
            raw_txn: None,
            fs_config: fs_config.clone(),
            block_size: fs_config.block_size,
            max_blocks: fs_config.max_size.map(|size| size / fs_config.block_size as u64),
            max_name_len,
            caches,
        })
    }

    pub async fn get(self: TxnArc, key: impl Into<Key>) -> TiKvResult<Option<Value>> {
        if let Some(txn) = &self.txn {
            txn.write().await.get(key).await
        } else {
            self.raw.get(key).await
        }
    }

    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> TiKvResult<Vec<KvPair>> {
        if let Some(txn) = &self.txn {
            txn.write().await.batch_get(keys).await.map(|iter|iter.collect::<Vec<_>>())
        } else {
            self.raw.batch_get(keys).await
        }
    }

    pub async fn put(self: TxnArc, key: impl Into<Key>, value: impl Into<Value>) -> TiKvResult<()> {
        if let Some(txn) = &self.txn {
            txn.write().await.put(key, value).await
        } else {
            self.raw.put(key, value).await
        }
    }

    pub async fn batch_put(me: Arc<Self>, pairs: Vec<KvPair>) -> TiKvResult<()> {
        if let Some(txn) = &me.txn {
            let mutations = pairs.into_iter().map(|KvPair(k,v)| Mutation::Put(k, v));
            txn.write().await.batch_mutate(mutations).await
        } else {
            me.raw.batch_put(pairs).await
        }
    }

    pub async fn delete(self: TxnArc, key: impl Into<Key>) -> TiKvResult<()> {
        if let Some(txn) = &self.txn {
            txn.write().await.delete(key).await
        } else {
            self.raw.delete(key).await
        }
    }

    pub async fn batch_mutate(self: TxnArc, mutations: impl IntoIterator<Item = Mutation>) -> TiKvResult<()> {
        if let Some(txn) = &self.txn {
            txn.write().await.batch_mutate(mutations).await
        } else {
            let mut deletes = Vec::new();
            for entry in mutations.into_iter() {
                match entry {
                    Mutation::Delete(key) => deletes.push(key),
                    Mutation::Put(key, value) => {
                        let clone = self.raw.clone();
                        tokio::spawn((async move || {
                            let clone2 = clone;
                            let key2 = key;
                            let value2 = value;
                            clone2.put(key2, value2).await
                        })());
                    }
                }
            };
            if deletes.len() > 0 {
                self.raw.batch_delete(deletes).await?;
            }
            Ok(())
        }
    }

    pub async fn scan(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> TiKvResult<Vec<KvPair>> {
        if let Some(txn) = &self.txn {
            txn.write().await.scan(range, limit).await.map(|iter|iter.collect::<Vec<_>>())
        } else {
            self.raw.scan(range, limit).await
        }
    }

    pub async fn scan_keys(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> TiKvResult<Vec<Key>> {
        if let Some(txn) = &self.txn {
            txn.write().await.scan_keys(range, limit).await.map(|iter|iter.collect::<Vec<_>>())
        } else {
            self.raw.scan_keys(range, limit).await
        }
    }

    pub async fn commit(&self) -> TiKvResult<Option<Timestamp>> {
        if let Some(txn) = &self.txn {
            txn.write().await.commit().await
        } else {
            Ok(Some(Timestamp::default()))
        }
    }

    pub async fn rollback(&self) -> TiKvResult<()> {
        if let Some(txn) = &self.txn {
            txn.write().await.rollback().await
        } else {
            Ok(())
        }
    }

    pub async fn open(self: Arc<Self>, ino: u64, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.clone().put(key, &[]).await?;
        eprintln!("open-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn close(self: Arc<Self>, ino: u64, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // de-publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.clone().delete(key).await?;
        eprintln!("close-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn read(self: Arc<Self>, ino: u64, start: u64, size: u32) -> Result<Vec<u8>> {
        self.clone().read_data(ino, start, Some(size as u64), self.fs_config.enable_atime).await
    }

    pub async fn reserve_new_ino(self: TxnArc) -> Result<u64> {
        let mut meta = self
            .clone().read_meta()
            .await?
            .unwrap_or_else(|| Meta::new(self.block_size as u64, StaticFsParameters{
                hashed_blocks: self.fs_config.hashed_blocks
            }));

        self.clone().check_space_left(&meta)?;

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

    pub async fn check_if_dir_entry_exists(self: Arc<Self>, parent: u64, name: &ByteString) -> Result<bool> {
        Ok(self.get_index(parent, name.clone()).await?.is_some())
    }

    pub async fn connect_inode_to_directory(self: Arc<Self>, parent: u64, name: &ByteString, inode: &Inode) -> Result<()> {
        if parent >= ROOT_INODE {
            if self.clone().check_if_dir_entry_exists(parent, name).await? {
                return Err(FsError::FileExist {
                    file: name.to_string(),
                });
            }
            self.clone().set_index(parent, name.clone(), inode.ino).await?;

            let mut dir = self.clone().read_dir(parent).await?;
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
        self: TxnArc,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        rdev: u32,
    ) -> Result<Arc<Inode>> {
        let ino = self.clone().reserve_new_ino().await?;

        let inode = Self::initialize_inode(self.block_size, ino, mode, gid, uid, rdev);

        self.clone().connect_inode_to_directory(parent, &name, &inode).await?;

        let ptr = Arc::new(inode);

        self.save_inode(&ptr).await?;
        Ok(ptr)
    }

    pub async fn get_index(self: Arc<Self>, parent: u64, name: ByteString) -> Result<Option<u64>> {
        let key = Key::from(self.key_builder().index(parent, &name));
        self.clone().get(key)
            .await
            .map_err(FsError::from)
            .and_then(|value| {
                value
                    .map(|data| Ok(Index::deserialize(&data)?.ino))
                    .transpose()
            })
    }

    pub async fn set_index(self: TxnArc, parent: u64, name: ByteString, ino: u64) -> Result<()> {
        let key = Key::from(self.key_builder().index(parent, &name));
        let value = Index::new(ino).serialize()?;
        Ok(self.clone().put(key, value).await?)
    }

    pub async fn remove_index(self: TxnArc, parent: u64, name: ByteString) -> Result<()> {
        let key = Key::from(self.key_builder().index(parent, &name));
        Ok(self.clone().delete(key).await?)
    }

    pub async fn read_inode_uncached(self: TxnArc, ino: u64) -> Result<Arc<Inode>> {
        let key = Key::from(self.key_builder().inode(ino));
        let value = self.clone()
            .get(key)
            .await?
            .ok_or(FsError::InodeNotFound { inode: ino })?;
        let inode = Arc::new(Inode::deserialize(&value)?);
        self.caches.inode.insert(ino, (Instant::now(), inode.clone())).await;
        Ok(inode)
    }

    pub async fn read_inode(self: TxnArc, ino: u64) -> Result<Arc<Inode>> {
        if let Some((time, value)) = self.caches.inode.get(&ino).await {
            if time.elapsed().as_secs() < 2 {
                return Ok(value);
            } else {
                self.caches.inode.remove(&ino).await;
            }
        }

        self.read_inode_uncached(ino).await
    }

    pub async fn read_inode_arc(self: Arc<Self>, ino: u64) -> Result<Arc<Inode>> {
        self.read_inode(ino).await
    }

    pub async fn save_inode(self: TxnArc, inode: &Arc<Inode>) -> Result<()> {
        let key = Key::from(self.key_builder().inode(inode.ino));

        if inode.nlink == 0 && inode.opened_fh == 0 {
            self.clone().delete(key).await?;
            self.caches.inode.remove(&inode.ino).await;
        } else {
            self.clone().put(key, inode.serialize()?).await?;
            self.caches.inode.insert(inode.ino, (Instant::now(), inode.clone())).await;
            debug!("save inode: {:?}", inode);
        }
        Ok(())
    }

    pub async fn remove_inode(self: TxnArc, ino: u64) -> Result<()> {
        let key = Key::from(self.key_builder().inode(ino));
        self.clone().delete(key).await?;
        self.caches.inode.remove(&ino).await;
        Ok(())
    }

    pub fn key_builder(&self) -> ScopedKeyBuilder {
        ScopedKeyBuilder::new(&self.fs_config.key_prefix)
    }

    pub async fn read_meta(self: TxnArc) -> Result<Option<Meta>> {
        let key = Key::from(self.key_builder().meta());
        let opt_data = self.clone().get(key).await?;
        opt_data.map(|data| Meta::deserialize(&data)).transpose()
    }

    pub async fn save_meta(self: TxnArc, meta: &Meta) -> Result<()> {
        let key = Key::from(self.key_builder().meta());
        self.clone().put(key, meta.serialize()?).await?;
        Ok(())
    }

    async fn transfer_inline_data_to_block(self: TxnArc, inode: &mut Inode) -> Result<()> {
        debug_assert!(inode.size <= self.inline_data_threshold());
        let key = Key::from(self.key_builder().block(inode.ino, 0));
        let mut data = inode.inline_data.clone().unwrap();
        data.resize(self.block_size as usize, 0);
        self.clone().put(key, data).await?;
        inode.inline_data = None;
        Ok(())
    }

    async fn write_inline_data(
        self: TxnArc,
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
        self.clone().save_inode(&Arc::new(inode.clone())).await?;

        Ok(size)
    }

    async fn read_inline_data(
        &self,
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

    async fn hb_read_data(self: TxnArc, ino: u64, start: u64, size: u64) -> Result<Vec<u8>> {

        let bs = BlockSplitterRead::new(self.block_size, start, size);
        let block_range = bs.block_range();

        let block_hashes = self.clone().hb_get_block_hash_list_by_block_range(ino, block_range.clone()).await?;
        //eprintln!("block_hashes(count: {}): {:?}", block_hashes.len(), block_hashes);
        let block_hashes_set = HashSet::from_iter(block_hashes.values().cloned());
        let blocks_data = if self.fs_config.raw_hashed_blocks {
            self.raw_txn.as_ref().unwrap().clone().hb_get_block_data_by_hashes(&block_hashes_set).await?
        } else {
            self.clone().hb_get_block_data_by_hashes(&block_hashes_set).await?
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

    async fn read_data_traditional(self: TxnArc, ino: u64, start: u64, size: u64) -> Result<Vec<u8>> {
        let target = start + size;
        let start_block = start / self.block_size as u64;
        let end_block = (target + self.block_size as u64 - 1) / self.block_size as u64;

        let block_range = self.key_builder().block_range(ino, start_block..end_block);
        let pairs = self
            .scan(
                block_range,
                (end_block - start_block) as u32,
            )
            .await?;

        let mut data = pairs.into_iter()
            .enumerate()
            .flat_map(|(i, pair)| {
                let key = if let Ok(ScopedKeyKind::Block { ino: _, block }) =
                    self.key_builder().parse(pair.key().into()).map(|x|x.key_type)
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
        self: Arc<Self>,
        ino: u64,
        start: u64,
        chunk_size: Option<u64>,
        update_atime: bool,
    ) -> Result<Vec<u8>> {
        let attr = self.clone().read_inode(ino).await?;
        if start >= attr.size {
            return Ok(Vec::new());
        }

        let max_size = attr.size - start;
        let size = chunk_size.unwrap_or(max_size).min(max_size);

        if update_atime {
            let mut attr = attr.deref().clone();
            attr.atime = SystemTime::now();
            self.clone().save_inode(&Arc::new(attr)).await?;
        }

        if attr.inline_data.is_some() {
            return self.clone().read_inline_data(&attr, start, size).await;
        }

        if self.fs_config.hashed_blocks {
            self.hb_read_data(ino, start, size).await
        } else {
            self.read_data_traditional(ino, start, size).await
        }
    }

    pub async fn clear_data(self: TxnArc, ino: u64) -> Result<u64> {
        let mut attr = self.clone().read_inode(ino).await?.deref().clone();
        let end_block = (attr.size + self.block_size - 1) / self.block_size;

        for block in 0..end_block {
            let key = Key::from(self.key_builder().block(ino, block));
            self.clone().delete(key).await?;
        }

        let clear_size = attr.size;
        attr.size = 0;
        attr.atime = SystemTime::now();
        self.save_inode(&Arc::new(attr)).await?;
        Ok(clear_size)
    }

    pub async fn write_blocks_traditional(self: TxnArc, ino: u64, start: u64, data: &Bytes) -> Result<()> {

        let mut block_index = start / self.block_size;
        let start_key = Key::from(self.key_builder().block(ino, block_index));
        let start_index = (start % self.block_size) as usize;

        let first_block_size = self.block_size as usize - start_index;

        let (first_block, mut rest) = data.split_at(first_block_size.min(data.len()));

        let mut start_value = self.clone()
            .get(start_key.clone())
            .await?
            .unwrap_or_else(|| empty_block(self.block_size));

        start_value[start_index..start_index + first_block.len()].copy_from_slice(first_block);

        self.clone().put(start_key, start_value).await?;

        while !rest.is_empty() {
            block_index += 1;
            let key = Key::from(self.key_builder().block(ino, block_index));
            let (curent_block, current_rest) =
                rest.split_at((self.block_size as usize).min(rest.len()));
            let mut value = curent_block.to_vec();
            if value.len() < self.block_size as usize {
                let mut last_value = self.clone()
                    .get(key.clone())
                    .await?
                    .unwrap_or_else(|| empty_block(self.block_size));
                last_value[..value.len()].copy_from_slice(&value);
                value = last_value;
            }
            self.clone().put(key, value).await?;
            rest = current_rest;
        }

        Ok(())
    }

    pub async fn hb_get_block_hash_list_by_block_range(self: TxnArc, ino: u64, block_range: Range<u64>
    ) -> Result<HashMap<BlockAddress, inode::Hash>>
    {
        let range = self.key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.scan(
                range,
                block_range.count() as u32,
            )
            .await?;
        Ok(iter.into_iter().filter_map(|pair| {
            let Some(key) = self.key_builder().parse_key_block_address(pair.key().into()) else {
                tracing::error!("failed parsing block address from response 1");
                return None;
            };
            let Some(hash) = ScopedKeyBuilder::parse_hash_from_dyn_sized_array(pair.value()) else {
                tracing::error!("failed parsing hash value from response 2");
                return None;
            };
            Some((key, hash))
        }).collect())
    }

    pub async fn hb_get_block_data_by_hashes(self: TxnArc, hash_list: &HashSet<TiFsHash>) -> Result<HashMap<inode::Hash, Arc<Vec<u8>>>>
    {
        let mut watch = AutoStopWatch::start("get_hash_blocks_data");

        let mut result = HashMap::new();
        let mut keys = Vec::<Key>::new();
        for hash in hash_list {
            if let Some(cached) = self.caches.block.get(hash).await {
                result.insert(*hash, cached);
            } else {
                keys.push(self.key_builder().hashed_block(hash).into());
            }
        }

        watch.sync("cached");

        let mut uncached_blocks = HashMap::new();
        let rcv_data_list = self.batch_get(keys).await?;
        for pair in rcv_data_list {
            if let Some(hash) = self.key_builder().parse_key_hashed_block(pair.key().into()) {
                let value = Arc::new(pair.into_value());
                uncached_blocks.insert(hash, value.clone());
                self.caches.block.insert(hash, value).await;
            } else {
                tracing::error!("failed parsing hash from response!");
            }
        }

        watch.sync("fetch");

        if self.fs_config.validate_read_hashes {
            for (hash, value) in uncached_blocks.iter() {
                let actual_hash = HashedBlock::calculate_hash(&value);
                if hash != &actual_hash {
                    return Err(FsError::ChecksumMismatch{hash: hash.clone(), actual_hash});
                }
            }

            watch.sync("validate");
        }

        result.extend(uncached_blocks.into_iter());

        watch.sync("done");

        Ok(result)
    }

    pub async fn hb_filter_existent_blocks(self: Arc<Self>, mut new_blocks: HashMap<TiFsHash, Arc<Vec<u8>>>) -> Result<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        if self.fs_config.existence_check {
            if self.fs_config.validate_writes {
                let key_list = new_blocks.keys().cloned().collect::<Vec<_>>();
                for hash in key_list {
                    let key = self.key_builder().hashed_block(&hash);
                    let key_range: RangeInclusive<Key> = key.into()..=key.into();
                    let result = if self.fs_config.raw_hashed_blocks {
                        self.raw.scan_keys(key_range, 1).await?
                    } else {
                        self.scan_keys(key_range, 1).await?
                    };
                    if result.len() > 0 {
                        new_blocks.remove(&hash);
                    }
                }
            } else {
                let exists_keys_request = new_blocks.keys().map(|k| self.key_builder().hashed_block_exists(k)).collect::<Vec<_>>();
                let exists_keys_response = self.batch_get(exists_keys_request).await?;
                for KvPair(key, _) in exists_keys_response.into_iter() {
                    let key = (&key).into();
                    let hash = self.key_builder().parse_key_hashed_block(key).ok_or(FsError::UnknownError("failed parsing hash from response".into()))?;
                    new_blocks.remove(&hash);
                }
            }
        }
        Ok(new_blocks)
    }


    pub async fn hb_upload_new_blocks(self: Arc<Self>, new_blocks: HashMap<TiFsHash, Arc<Vec<u8>>>) -> Result<()> {
        let mut mutations_txn = Vec::<Mutation>::new();
        let mut mutations_raw = Vec::<KvPair>::new();

        let new_blocks_len = new_blocks.len();
        for (k, new_block) in new_blocks {
            if self.fs_config.raw_hashed_blocks {
                mutations_raw.push(KvPair(self.key_builder().hashed_block(&k).into(), new_block.deref().clone()));
            } else {
                mutations_txn.push(Mutation::Put(self.key_builder().hashed_block(&k).into(), new_block.deref().clone()));
            }
            mutations_txn.push(Mutation::Put(self.key_builder().hashed_block_exists(&k).into(), vec![]));
        }

        let raw_cnt = mutations_raw.len();
        if raw_cnt > 0 {
            if self.fs_config.batch_raw_block_write {
                self.raw.batch_put_with_ttl(mutations_raw, vec![0; raw_cnt]).await
                    .map_err(|e| {
                        eprintln!("batch-hb_write_data(blocks: {}", new_blocks_len);
                        e
                    })?;
            } else {
                for KvPair(k,v) in mutations_raw.into_iter() {
                    let raw_clone = self.raw.clone();
                    let packed = async move ||{
                        let k2 = k;
                        let v2 = v;
                        let raw_clone2 = raw_clone;
                        raw_clone2.put(k2, v2).await
                    };
                    tokio::spawn(packed());
                }
            }
        }

        if mutations_txn.len() > 0 {
            self.batch_mutate(mutations_txn).await?;
        }

        Ok(())
    }

    pub async fn hb_upload_new_block_addresses(me: Arc<Self>, blocks_to_assign: VecDeque<(TiFsHash, Vec<BlockAddress>)>) -> Result<()> {
        let mut kv_pairs = Vec::<KvPair>::new();
        for (hash, addresses) in blocks_to_assign {
            for addr in addresses {
                kv_pairs.push(KvPair(me.key_builder().block_hash(addr).into(), hash.as_bytes().to_vec()));
            }
        }
        Ok(Self::batch_put(me, kv_pairs).await?)
    }

    pub async fn hb_write_data(self: Arc<Self>, fh: Arc<FileHandler>, start: u64, data: Bytes) -> Result<bool> {

        let mut watch = AutoStopWatch::start("hb_wrt");
        let bs = BlockSplitterWrite::new(self.block_size, start, &data);
        let block_range = bs.get_range();
        let ino = fh.ino();

        let hash_list_prev = self.clone().hb_get_block_hash_list_by_block_range(ino, block_range.clone()).await?;
        let input_block_hashes = hash_list_prev.len();
        watch.sync("hp");

        let mut pre_data_hash_request = HashSet::<inode::Hash>::new();
        let first_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            ino, bs.first_data, bs.first_data_start_position, &hash_list_prev, &mut pre_data_hash_request
        );
        let last_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            ino, bs.last_data, 0, &hash_list_prev, &mut pre_data_hash_request
        );

        let pre_data = self.clone().hb_get_block_data_by_hashes(&pre_data_hash_request).await?;
        watch.sync("pd");
        let mut new_blocks = HashMap::new();
        let mut new_block_hashes = HashMap::new();

        first_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);
        last_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);

        for (index, chunk) in bs.mid_data.data.chunks(self.block_size as usize).enumerate() {
            let hash = blake3::hash(chunk);
            new_blocks.insert(hash, Arc::new(chunk.to_vec()));
            new_block_hashes.insert(BlockAddress{ino, index: bs.mid_data.block_index + index as u64}, hash);
        }
        watch.sync("h");

        for (k, new_block) in &new_blocks {
            self.caches.block.insert(*k, new_block.clone()).await;
        }
        watch.sync("ca");

        let mut parallel_executor = AsyncParallelPipeStage::new(8);

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
        let new_block_hashes_len = new_block_hashes.len();

        let mut mm = new_block_hashes.into_iter().map(|(k,v)| (v,k)).collect::<MultiMap<_,_>>();

        let chunks: VecDeque<HashMap<TiFsHash, Arc<Vec<u8>>>> = make_chunks_hash_map(new_blocks, 4);

        for chunk in chunks {
            let blocks_to_assign = chunk.keys().filter_map(|hash| {
                let values = mm.remove(hash);
                values.map(|values|(hash.clone(), values))
            }).collect::<VecDeque<_>>();

            let r = self.weak.upgrade().unwrap();
            let r2 = r.clone();

            let fut = Self::hb_filter_existent_blocks(r.clone(), chunk)
            .and_then(move |remaining_new_blocks|{
                Self::hb_upload_new_blocks(r, remaining_new_blocks)
            }).and_then(move |_|{
                Self::hb_upload_new_block_addresses(r2, blocks_to_assign)
            });

            parallel_executor.push(fut).await;
        }

        // remove outdated blocks:
        // TODO!

        parallel_executor.wait_finish_all().await;
        let total_jobs = parallel_executor.get_total();

        watch.sync("pm");

        eprintln!("hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,jobs:{total_jobs}({skipped_new_block_hashes}/{input_block_hashes} skipped)", ino, start, data.len(), bs.block_size, block_range.end - block_range.start, block_range.start, block_range.end);

        let was_modified = new_block_hashes_len > 0;
        Ok(was_modified)
    }

    #[instrument(skip(self, data))]
    pub async fn write(self: TxnArc, fh: Arc<FileHandler>, start: u64, data: Bytes) -> Result<usize> {
        let mut watch = AutoStopWatch::start("write_data");
        let ino = fh.ino();
        debug!("write data at ({})[{}]", ino, start);
        //let meta = self.read_meta().await?.unwrap(); // TODO: is this needed?
        //self.check_space_left(&meta)?;

        let mut inode = self.clone().read_inode(ino).await?.deref().clone();
        let size = data.len();
        let target = start + size as u64;
        watch.sync("read_inode");

        if inode.inline_data.is_some() && target > self.inline_data_threshold() {
            self.clone().transfer_inline_data_to_block(&mut inode).await?;
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
            self.clone().hb_write_data(fh, start, data).await?
        } else {
            self.clone().write_blocks_traditional(ino, start, &data).await?;
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

    pub async fn write_link(self: TxnArc, inode: &mut Inode, data: Bytes) -> Result<usize> {
        debug_assert!(inode.file_attr.kind == FileType::Symlink);
        inode.inline_data = None;
        inode.set_size(0, self.block_size);
        self.write_inline_data(inode, 0, &data).await
    }

    pub async fn read_link(self: TxnArc, ino: u64) -> Result<Vec<u8>> {
        let inode = self.clone().read_inode(ino).await?;
        debug_assert!(inode.file_attr.kind == FileType::Symlink);
        let size = inode.size;
        if self.fs_config.enable_atime {
            let mut inode = inode.deref().clone();
            inode.atime = SystemTime::now();
            self.clone().save_inode(&Arc::new(inode)).await?;
        }
        self.read_inline_data(&inode, 0, size).await
    }

    pub async fn link(self: TxnArc, ino: u64, newparent: u64, newname: ByteString) -> Result<Arc<Inode>> {
        if let Some(old_ino) = self.clone().get_index(newparent, newname.clone()).await? {
            let inode = self.clone().read_inode(old_ino).await?;
            match inode.kind {
                FileType::Directory => self.clone().rmdir(newparent, newname.clone()).await?,
                _ => self.clone().unlink(newparent, newname.clone()).await?,
            }
        }
        self.clone().set_index(newparent, newname.clone(), ino).await?;

        let mut inode = self.clone().read_inode(ino).await?.deref().clone();
        let mut dir = self.clone().read_dir(newparent).await?;

        dir.push(DirItem {
            ino,
            name: newname.to_string(),
            typ: inode.kind,
        });

        self.clone().save_dir(newparent, &dir).await?;
        inode.nlink += 1;
        inode.ctime = SystemTime::now();
        let ptr = Arc::new(inode);
        self.save_inode(&ptr).await?;
        Ok(ptr)
    }

    pub async fn unlink(self: TxnArc, parent: u64, name: ByteString) -> Result<()> {
        match self.clone().get_index(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(ino) => {
                self.clone().remove_index(parent, name.clone()).await?;
                let parent_dir = self.clone().read_dir(parent).await?;
                let new_parent_dir: Directory = parent_dir
                    .into_iter()
                    .filter(|item| item.name != *name)
                    .collect();
                self.clone().save_dir(parent, &new_parent_dir).await?;

                let mut inode = self.clone().read_inode(ino).await?.deref().clone();
                inode.nlink -= 1;
                inode.ctime = SystemTime::now();
                self.clone().save_inode(&Arc::new(inode)).await?;
                Ok(())
            }
        }
    }

    pub async fn rmdir(self: TxnArc, parent: u64, name: ByteString) -> Result<()> {
        match self.clone().get_index(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(ino) => {
                if self.clone()
                    .read_dir(ino)
                    .await?
                    .iter()
                    .any(|i| DIR_SELF != i.name && DIR_PARENT != i.name)
                {
                    let name_str = name.to_string();
                    debug!("dir({}) not empty", &name_str);
                    return Err(FsError::DirNotEmpty { dir: name_str });
                }

                self.clone().unlink(ino, DIR_SELF).await?;
                self.clone().unlink(ino, DIR_PARENT).await?;
                self.clone().unlink(parent, name).await
            }
        }
    }

    pub async fn lookup(self: TxnArc, parent: u64, name: ByteString) -> Result<u64> {
        self.get_index(parent, name.clone())
            .await?
            .ok_or_else(|| FsError::FileNotFound {
                file: name.to_string(),
            })
    }

    pub async fn fallocate(self: TxnArc, inode: &mut Inode, offset: i64, length: i64) -> Result<()> {
        let target_size = (offset + length) as u64;
        if target_size <= inode.size {
            return Ok(());
        }

        if inode.inline_data.is_some() {
            if target_size <= self.inline_data_threshold() {
                let original_size = inode.size;
                let data = vec![0; (target_size - original_size) as usize];
                self.clone().write_inline_data(inode, original_size, &data).await?;
                return Ok(());
            } else {
                self.clone().transfer_inline_data_to_block(inode).await?;
            }
        }

        inode.set_size(target_size, self.block_size);
        inode.mtime = SystemTime::now();
        self.save_inode(&Arc::new(inode.clone())).await?;
        Ok(())
    }

    pub async fn mkdir(
        self:  TxnArc,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
    ) -> Result<Arc<Inode>> {
        let dir_mode = make_mode(FileType::Directory, mode as _);
        let inode = self.clone().make_inode(parent, name, dir_mode, gid, uid, 0).await?;
        self.clone().save_dir(inode.ino, &Directory::new()).await?;
        self.clone().link(inode.ino, inode.ino, DIR_SELF).await?;
        if parent >= ROOT_INODE {
            self.clone().link(parent, inode.ino, DIR_PARENT).await?;
        }
        self.clone().read_inode(inode.ino).await
    }

    pub async fn read_dir(self: Arc<Self>, ino: u64) -> Result<Directory> {
        let key = Key::from(self.key_builder().block(ino, 0));
        let data = self.clone()
            .get(key)
            .await?
            .ok_or(FsError::BlockNotFound {
                inode: ino,
                block: 0,
            })?;
        trace!("read data: {}", String::from_utf8_lossy(&data));
        super::dir::decode(&data)
    }

    pub async fn save_dir(self: TxnArc, ino: u64, dir: &[DirItem]) -> Result<Arc<Inode>> {
        let data = super::dir::encode(dir)?;
        let mut inode = self.clone().read_inode(ino).await?.deref().clone();
        inode.set_size(data.len() as u64, self.block_size);
        inode.atime = SystemTime::now();
        inode.mtime = SystemTime::now();
        inode.ctime = SystemTime::now();
        let ptr = Arc::new(inode);
        self.clone().save_inode(&ptr).await?;
        let key = Key::from(self.key_builder().block(ino, 0));
        self.clone().put(key, data).await?;
        Ok(ptr)
    }

    pub async fn statfs(self: TxnArc) -> Result<StatFs> {
        let bsize = self.block_size as u32;
        let mut meta = self
            .clone().read_meta()
            .await?
            .expect("meta should not be none after fs initialized");
        let next_inode = meta.inode_next;
        let range = self.key_builder().inode_range(ROOT_INODE..next_inode);
        let (used_blocks, files) = self
            .scan(
                range,
                (next_inode - ROOT_INODE) as u32,
            )
            .await?
            .into_iter().map(|pair| Inode::deserialize(pair.value()))
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
