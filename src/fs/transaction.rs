use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
use std::ops::{Deref, Range};
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use bytes::Bytes;
use bytestring::ByteString;
use fuser::TimeOrNow;
use multimap::MultiMap;
use num_bigint::BigUint;
use num_format::{Buffer, Locale, ToFormattedString};
use num_traits::FromPrimitive;
use tikv_client::Backoff;
use tracing::{debug, instrument, trace, Level};
use uuid::Uuid;

use crate::fs::inode::{StorageDirItemKind, StorageIno};
use crate::fs::key::MAX_NAME_LEN;
use crate::fs::meta::MetaStatic;
use crate::fs::utils::stop_watch::AutoStopWatch;
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

use super::dir::StorageDirectory;
use super::error::{FsError, TiFsResult};
use super::file_handler::FileHandler;
use super::fs_config::TiFsConfig;
use super::hash_block::block_splitter::{BlockSplitterRead, BlockSplitterWrite};
use super::hash_block::helpers::UpdateIrregularBlock;
use super::hash_fs_interface::{BlockIndex, GotOrMade, HashFsInterface};
use super::inode::{InoAccessTime, InoDescription, InoSize, ParentStorageIno, StorageDirItem, InoStorageFileAttr, StorageFilePermission, TiFsHash};
use super::mode::as_file_perm;
use super::parsers;
use super::reply::StatFs;
use super::tikv_fs::TiFsCaches;

pub const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(300, 1000, 100);
pub const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(30, 500, 1000);
pub const PESSIMISTIC_BACKOFF: Backoff = Backoff::no_backoff();
pub const MAX_TIKV_SCAN_LIMIT: u32 = 10240;

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

pub struct HashedBlockMetaData {
    pub existing: BTreeSet<Uuid>,
    pub named_usages: HashSet<Uuid>,
    pub counters: HashSet<Uuid>,
}

impl HashedBlockMetaData {
    pub fn new() -> Self {
        Self {
            existing: BTreeSet::new(),
            named_usages: HashSet::new(),
            counters: HashSet::new(),
        }
    }
}

pub struct Txn {
    pub weak: Weak<Self>,
    _instance_id: Uuid,
    pub hash_fs: Arc<dyn HashFsInterface>,
    fs_config: TiFsConfig,
    block_size: u64,            // duplicate of fs_config.block_size. Keep it to avoid refactoring efforts.
    caches: TiFsCaches,
}

pub type TxnArc = Arc<Txn>;

impl Txn {
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn fs_config(&self) -> TiFsConfig {
        self.fs_config.clone()
    }

//    fn check_space_left(self: TxnArc, meta: &MetaMutable) -> TiFsResult<()> {
//        match meta.last_stat {
//            Some(ref stat) if stat.bavail == 0 => {
//                Err(FsError::NoSpaceLeft(stat.bsize as u64 * stat.blocks))
//            }
//            _ => Ok(()),
//        }
//    }

    pub async fn begin_on_hash_fs_interface(
        hash_fs: Arc<dyn HashFsInterface>,
        instance_id: Uuid,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
    ) -> TiFsResult<Arc<Self>> {
        Ok(TxnArc::new_cyclic(|weak| { Self {
                weak: weak.clone(),
                _instance_id: instance_id,
                hash_fs,
                fs_config: fs_config.clone(),
                block_size: fs_config.block_size,
                caches,
            }
        }))
    }

//    pub async fn begin_optimistic(
//        instance_id: Uuid,
//        client: Arc<TransactionClientMux>,
//        raw: Arc<tikv_client::RawClient>,
//        fs_config: &TiFsConfig,
//        caches: TiFsCaches,
//    ) -> TiFsResult<TxnArc> {
//        let txn = if fs_config.pure_raw { None } else {
//            let options = TransactionOptions::new_optimistic().use_async_commit();
//            let options = options.retry_options(RetryOptions {
//                region_backoff: DEFAULT_REGION_BACKOFF,
//                lock_backoff: OPTIMISTIC_BACKOFF,
//            });
//            Some(client.give_one_transaction(&options).await?)
//        }.map(|x|RwLock::new(x));
//        Ok(TxnArc::new_cyclic(|weak| { Self {
//                weak: weak.clone(),
//                _instance_id: instance_id,
//                f_txn: TikvBasedHashFs::new_arc(
//                    fs_config.clone(),
//                    FlexibleTransaction::new_txn(
//                        client, txn, raw, fs_config.clone()),
//                ),
//                fs_config: fs_config.clone(),
//                block_size: fs_config.block_size,
//                caches,
//            }
//        }))
//    }

//    pub fn pure_raw(
//        instance_id: Uuid,
//        client: Arc<TransactionClientMux>,
//        raw: Arc<tikv_client::RawClient>,
//        fs_config: &TiFsConfig,
//        caches: TiFsCaches,
//    ) -> TxnArc {
//        Arc::new_cyclic(|weak| Txn {
//            weak: weak.clone(),
//            _instance_id: instance_id,
//            f_txn: TikvBasedHashFs::new_arc(
//                fs_config.clone(), FlexibleTransaction::new_pure_raw(
//                client, raw, fs_config.clone())
//            ),
//            fs_config: fs_config.clone(),
//            block_size: fs_config.block_size,
//            caches,
//        })
//    }

    pub async fn open(self: Arc<Self>, ino: StorageIno) -> TiFsResult<Uuid> {
        Ok(self.hash_fs.inode_open(ino).await?)
    }

    pub async fn close(self: Arc<Self>, ino: StorageIno, use_id: Uuid) -> TiFsResult<()> {
       Ok(self.hash_fs.inode_close(ino, use_id).await?)
    }

    #[instrument(skip(self))]
    pub async fn read_hash_of_file(self: TxnArc, ino: StorageIno, offset: u64, size: u64) -> TiFsResult<Vec<u8>> {

        let hash_size = self.fs_config.hash_len as u64;
        let offset = offset.clamp(0, hash_size);
        let remaining = hash_size - offset;
        let size = size.clamp(0, remaining);

        if size == 0 {
            return Ok(vec![]);
        }

        let the_hash = self.hash_fs.file_get_hash(ino).await?;

        trace!("precalculated_hash: {the_hash:x?}");
        let mut result = Vec::with_capacity(self.fs_config.hash_len);
        result.extend_from_slice(&the_hash[offset as usize..(offset+size) as usize]);
        Ok(result)
    }

    pub async fn read(self: Arc<Self>, ino: StorageIno, start: u64, size: u32) -> TiFsResult<Vec<u8>> {
        self.clone().read_data(ino, start, Some(size as u64)).await
    }

    pub async fn connect_inode_to_directory(
        self: Arc<Self>,
        parent: ParentStorageIno,
        name: &ByteString,
        ino: &StorageIno
    ) -> TiFsResult<()> {
        self.hash_fs.directory_add_child_checked_existing_inode(
            parent,
            name.clone(),
            *ino,
        ).await?;
        Ok(())
    }

    pub async fn directory_add_child_checked_new_inode(
        self: TxnArc,
        parent: ParentStorageIno,
        name: ByteString,
        typ: StorageDirItemKind,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
        rdev: u32,
        inline_data: Option<Vec<u8>>,
    ) -> TiFsResult<GotOrMade<StorageDirItem>> {
        self.hash_fs.directory_add_child_checked_new_inode(
            parent, name, typ, perm, gid, uid, rdev, inline_data).await.map_err(|e|e.into())
    }

    #[tracing::instrument(skip(self))]
    pub async fn directory_remove_child(self: TxnArc, parent: ParentStorageIno, name: ByteString) -> TiFsResult<()> {
        Ok(self.hash_fs.directory_remove_child_file(parent, name).await?)
    }

    pub async fn read_static_meta(self: TxnArc) -> TiFsResult<Option<MetaStatic>> {
        Ok(Some(self.hash_fs.as_ref().meta_static_read().await?))
    }

    async fn hb_read_data(self: TxnArc, ino: StorageIno, start: u64, chunk_size: Option<u64>) -> TiFsResult<Vec<u8>> {

        let size = chunk_size.unwrap_or(self.fs_config.block_size);
        let block_hashes = self.hash_fs.inode_read_block_hashes_data_range(ino, start, size).await?;

        //tracing::debug!("block_hashes(count: {}): {:?}", block_hashes.len(), block_hashes);
        let block_hashes_set = HashSet::from_iter(block_hashes.values().cloned());
        let bs = BlockSplitterRead::new(self.fs_config.block_size, start, size);
        let blocks_data = self.clone().hb_get_block_data_by_hashes_cached(&block_hashes_set).await?;

        let result = parsers::hb_read_from_blocks(&bs, &block_hashes, &blocks_data)?;

        if tracing::enabled!(Level::DEBUG) {
            let mut buf_start = Buffer::default();
            buf_start.write_formatted(&start, &Locale::en);
            tracing::debug!("hb_read_data(ino: {ino}, start:{buf_start}, size: {size}) - block_size: {}, blocks_count: {}, range: [{}..{}] -> {} read", bs.block_size, bs.block_count, bs.first_block_index, bs.end_block_index-1, result.len());

            if result.len() < size as usize {
                let block_data_lengths = blocks_data.iter().map(|(key, data)|(key, data.len())).collect::<Vec<_>>();
                tracing::debug!("incomplete read - (ino: {ino}, start:{buf_start}, size: {size}): len:{}, block_hashes:{:?}, block_lengths:{:?}", result.len(), block_hashes, block_data_lengths);
            }
        }

        Ok(result)
    }
/*
    async fn read_data_traditional(self: TxnArc, ino: StorageIno, start: u64, size: u64) -> TiFsResult<Vec<u8>> {
        let target = start + size;
        let start_block = start.div_floor(self.fs_config.block_size);
        let end_block = target.div_ceil(self.fs_config.block_size);

        let block_range = self.key_builder().block_range(ino, start_block..end_block);
        let pairs = self.f_txn
            .scan(
                block_range,
                (end_block - start_block) as u32,
            )
            .await?;

        let mut block_map: HashMap<BlockAddress, Vec<u8>> = HashMap::new();
        for KvPair(k, v) in pairs.into_iter() {
            let key_data = Vec::from(k);
            let mut key_i = key_data.iter();
            let addr = self.key_parser(&mut key_i)?.parse_key_block_address()?;
            block_map.insert(addr, v);
        }

        let mut result = Vec::with_capacity(
            ((end_block - start_block) * self.fs_config.block_size) as usize);
        for i in start_block..end_block {
            let data = block_map.remove(&BlockAddress{ino, index: i})
                .unwrap_or(empty_block(self.fs_config.block_size));
            let block_offset = if i == start_block {
                start % self.fs_config.block_size
            } else { 0 } as usize;
            let block_end = if i == (end_block-1) {
                target % self.fs_config.block_size
            } else { self.fs_config.block_size } as usize;
            result.extend_from_slice(&data[block_offset..block_end]);
        }

        Ok(result)
    }
*/
    pub async fn read_data(
        self: Arc<Self>,
        ino: StorageIno,
        start: u64,
        chunk_size: Option<u64>,
    ) -> TiFsResult<Vec<u8>> {
        self.hb_read_data(ino, start, chunk_size).await
    }
/*
    pub async fn traditional_clear_data(self: TxnArc, ino: StorageIno) -> TiFsResult<u64> {

        let ino_size_key = self.key_builder().inode_size(ino);
        let key_lock = CriticalSectionKeyLock::new(
            self.clone(), ino_size_key).await?;

        let mut ino_size = self.caches.inode_size.read_uncached(
            &ino, &self.f_txn).await?.deref().clone();
        let block_cnt = ino_size.size().div_ceil(self.block_size);

        let mut deletes = Vec::with_capacity(block_cnt as usize);
        for block in 0..block_cnt {
            let addr = BlockAddress { ino, index: block };
            let key = if self.fs_config.hashed_blocks {
                Key::from(self.key_builder().block_hash(addr))
            } else {
                Key::from(self.key_builder().block(addr))
            };
            deletes.push(Mutation::Delete(key));
        }
        self.f_txn.batch_mutate(deletes).await?;

        let clear_size = ino_size.size();
        ino_size.set_size(0, self.fs_config.block_size);
        ino_size.last_change = SystemTime::now();
        self.caches.inode_size.write_cached(ino, Arc::new(ino_size), &self.f_txn).await?;
        key_lock.unlock().await?;
        Ok(clear_size)
    }
*/
/*
    pub async fn write_blocks_traditional(self: TxnArc, ino: StorageIno, start: u64, data: &Bytes) -> TiFsResult<()> {

        let mut block_index = start / self.block_size;
        let start_key = Key::from(self.key_builder().block(BlockAddress { ino, index: block_index }));
        let start_index = (start % self.block_size) as usize;

        let first_block_size = self.block_size as usize - start_index;

        let (first_block, mut rest) = data.split_at(first_block_size.min(data.len()));

        let mut start_value = self.clone()
            .f_txn.get(start_key.clone())
            .await?
            .unwrap_or_else(|| empty_block(self.block_size));

        start_value[start_index..start_index + first_block.len()].copy_from_slice(first_block);

        self.f_txn.put(start_key, start_value).await?;

        while !rest.is_empty() {
            block_index += 1;
            let key = Key::from(self.key_builder().block(BlockAddress { ino, index: block_index }));
            let (curent_block, current_rest) =
                rest.split_at((self.block_size as usize).min(rest.len()));
            let mut value = curent_block.to_vec();
            if value.len() < self.block_size as usize {
                let mut last_value = self.clone()
                    .f_txn.get(key.clone())
                    .await?
                    .unwrap_or_else(|| empty_block(self.block_size));
                last_value[..value.len()].copy_from_slice(&value);
                value = last_value;
            }
            self.f_txn.put(key, value).await?;
            rest = current_rest;
        }

        Ok(())
    }
*/
    pub async fn hb_get_block_data_by_hashes_cached(self: TxnArc, hash_list: &HashSet<TiFsHash>
    ) -> TiFsResult<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        let mut watch = AutoStopWatch::start("get_hash_blocks_data");

        let mut result = HashMap::new();
        let mut uncached_block_hashes: HashSet<&Vec<u8>> = HashSet::<&TiFsHash>::new();
        for hash in hash_list {
            if let Some(cached) = self.caches.block.get(hash).await {
                result.insert(hash.clone(), cached);
            } else {
                uncached_block_hashes.insert(hash);
            }
        }

        watch.sync("cached");

        if uncached_block_hashes.len() == 0 {
            return Ok(result);
        }

        let uncached_blocks = self.hash_fs
            .hb_get_block_data_by_hashes(&uncached_block_hashes).await?;

        for (hash, value) in &uncached_blocks {
            self.caches.block.insert(hash.clone(), value.clone()).await;
        }

        watch.sync("fetch");

        if self.fs_config.validate_read_hashes {
            for (hash, value) in uncached_blocks.iter() {
                let actual_hash = self.fs_config.calculate_hash(&value);
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

    async fn inode_write_upload_block_reference_counting_and_write_addresses(
        self: TxnArc,
        ino: StorageIno,
        block_hash: TiFsHash,
        ref_count_delta: u64,
        data: Vec<u8>,
        block_ids: Vec<BlockIndex>,
    ) -> TiFsResult<()> {
        let previous_reference_count = self.hash_fs.hb_increment_reference_count(
            &block_hash, ref_count_delta).await?;

        let block_len = data.len() as u64;

        // Upload block if new
        if previous_reference_count == BigUint::from_u8(0).unwrap() {
            self.hash_fs.hb_upload_new_block(block_hash.clone(), data).await?;
        }

        self.hash_fs.inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
            ino, block_hash, block_len, block_ids).await?;

        Ok(())
    }

    pub async fn hb_write_data(
        self: Arc<Self>,
        fh: Arc<FileHandler>,
        start: u64,
        data: Bytes
    ) -> TiFsResult<bool> {

        let mut watch = AutoStopWatch::start("hb_wrt");
        let bs = BlockSplitterWrite::new(self.block_size, start, &data);
        let block_range = bs.get_range();
        let ino = fh.ino();

        // 1. Remote interaction: Read existing block hashes for range:
        let hash_list_prev = self.hash_fs.inode_read_block_hashes_block_range(
            ino, block_range.clone()).await?;
        let input_block_hashes = hash_list_prev.len();
        watch.sync("hp");

        let mut pre_data_hash_request = HashSet::<TiFsHash>::new();
        let first_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            &self.fs_config,
            bs.first_data,
            bs.first_data_start_position,
            &hash_list_prev,
            &mut pre_data_hash_request
        );
        let last_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            &self.fs_config,
            bs.last_data,
            0,
            &hash_list_prev,
            &mut pre_data_hash_request
        );

        // 2. Remote interaction: Read data of first and last block (only if needed)
        let pre_data = self.clone().hb_get_block_data_by_hashes_cached(&pre_data_hash_request).await?;
        watch.sync("pd");
        let mut new_blocks = HashMap::new();
        let mut new_block_hashes = HashMap::new();

        first_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);
        last_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);

        for (index, chunk) in bs.mid_data.data.chunks(self.block_size as usize).enumerate() {
            let hash = self.fs_config.calculate_hash(chunk);
            new_blocks.insert(hash.clone(), Arc::new(chunk.to_vec()));
            new_block_hashes.insert(BlockIndex(bs.mid_data.block_index.0 + index as u64), hash);
        }
        watch.sync("hash");

        for (hash, new_block) in &new_blocks {
            self.caches.block.insert(hash.clone(), new_block.clone()).await;
        }
        watch.sync("add_cache");

        let mut parallel_executor = AsyncParallelPipeStage::new(self.fs_config.parallel_jobs);

        // filter out unchanged blocks:
        let mut skipped_new_block_hashes = 0;
        for (block_index, prev_block_hash) in hash_list_prev.iter() {
            if let Some(new_block_hash) = new_block_hashes.get(block_index) {
                if prev_block_hash == new_block_hash {
                    new_block_hashes.remove(block_index);
                    skipped_new_block_hashes += 1;
                }
            }
        }

        let new_block_hashes_len = new_block_hashes.len();
        let mm = new_block_hashes.into_iter()
            .map(|(k,v)| (v,k)).collect::<MultiMap<_,_>>();

        for (hash, block_ids) in mm {

            let data = new_blocks.get(&hash).unwrap();
            let fut = self.clone()
                .inode_write_upload_block_reference_counting_and_write_addresses(
                ino,
                hash,
                block_ids.len() as u64,
                data.deref().clone(),
                block_ids,
            );

            parallel_executor.push(fut).await;
        }

        parallel_executor.wait_finish_all().await;
        let total_jobs = parallel_executor.get_total();

        watch.sync("pm");

        tracing::debug!("hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,jobs:{total_jobs}({skipped_new_block_hashes}/{input_block_hashes} skipped)", ino, start.to_formatted_string(&Locale::en), data.len().to_formatted_string(&Locale::en), bs.block_size, block_range.end - block_range.start, block_range.start, block_range.end);

        let was_modified = new_block_hashes_len > 0;
        Ok(was_modified)
    }

    pub async fn write(self: TxnArc, fh: Arc<FileHandler>, start: u64, data: Bytes) -> TiFsResult<usize> {
        let mut watch = AutoStopWatch::start("write_data");
        let ino = fh.ino();
        debug!("write data at ({})[{}]", ino, start);

        let size = data.len();
        let _ = self.clone().hb_write_data(fh.clone(), start, data).await?;

        watch.sync("write impl");

        Ok(size)
    }

    pub fn set_fresh_inode_to_link(
        block_size: u64,
        ino_size: &mut InoSize,
        data: Bytes) {
        ino_size.set_inline_data(data.to_vec());
        ino_size.set_size(data.len() as u64, block_size);
    }

    pub async fn read_link(self: TxnArc, ino: StorageIno) -> TiFsResult<Vec<u8>> {
        Ok(self.hash_fs.inode_read_inline_data(ino).await?)
    }

    pub async fn add_hard_link(
        self: TxnArc,
        ino: StorageIno,
        further_parent: ParentStorageIno,
        new_name: ByteString,
    ) -> TiFsResult<()> {
        Ok(self.hash_fs.directory_add_child_checked_existing_inode(
            further_parent, new_name, ino).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn unlink(
            self: TxnArc,
            parent: ParentStorageIno,
            name: ByteString
    ) -> TiFsResult<()> {
        tracing::info!("remove_directory_child. parent: {:?}, name: {:?}", parent, name);
        self.clone().directory_remove_child(parent, name).await?;
        Ok(())
    }

    pub async fn rmdir(self: TxnArc, parent: ParentStorageIno, name: ByteString) -> TiFsResult<()> {
        Ok(self.hash_fs.directory_remove_child_directory(parent, name).await?)
    }

    pub async fn get_all_ino_data(self: TxnArc, ino: StorageIno,
    ) -> TiFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
        Ok(self.hash_fs.inode_get_all_attributes(ino).await?)
    }

    pub async fn set_attributes(
        self: TxnArc,
        ino: StorageIno,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> TiFsResult<()> {
        let mode_conv = mode.map(|m| StorageFilePermission(
            as_file_perm(m)));
        Ok(self.hash_fs.inode_set_all_attributes(
            ino, mode_conv, uid, gid, size, atime, mtime, ctime,
            crtime, chgtime, bkuptime, flags).await?)
    }

    pub async fn f_allocate(
        self: TxnArc,
        _fh: Arc<FileHandler>,
        ino: StorageIno,
        offset: i64,
        length: i64
    ) -> TiFsResult<()> {
        Ok(self.hash_fs.inode_allocate_size(ino, offset, length).await?)
    }

    pub async fn mkdir(
        self:  TxnArc,
        parent: ParentStorageIno,
        name: ByteString,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
    ) -> TiFsResult<StorageDirItem> {
        let result = self.clone().directory_add_child_checked_new_inode(
                parent, name.clone(), StorageDirItemKind::Directory,
                perm, gid, uid, 0, None).await?;

        if result.existed_before() {
            return Err(FsError::FileExist { file: name.to_string() });
        }

        Ok(result.value())
    }

    pub async fn read_dir(self: Arc<Self>, ino: StorageIno) -> TiFsResult<StorageDirectory> {
        Ok(self.hash_fs.directory_read_children(ino).await?)
    }

    pub async fn statfs(self: TxnArc) -> TiFsResult<StatFs> {

        /*
        let bsize = self.block_size as u32;
        let mut meta = self
            .clone().read_meta()
            .await?
            .expect("meta should not be none after fs initialized");
        let next_inode = StorageIno(meta.inode_next);
        let range = self.key_builder().inode_size(ROOT_INODE..next_inode);
        let (used_blocks, files) = self
            .scan(
                range,
                (next_inode.0 - ROOT_INODE.0) as u32,
            )
            .await?
            .into_iter().map(|pair| InoSize::deserialize(pair.value()))
            .try_fold((0, 0), |(blocks, files), inode| {
                Ok::<_, FsError>((blocks + inode?.blocks(), files + 1))
            })?;
        let ffree = std::u64::MAX - next_inode.0;
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
        */

        let stat = StatFs::new(
            0,
            u64::MAX,
            u64::MAX,
            0,
            u64::MAX,
            self.fs_config.block_size as u32,
            MAX_NAME_LEN,
            0,
        );

        trace!("statfs: {:?}", stat);
        Ok(stat)
    }
/*
    pub async fn setlkw(
        self: TxnArc,
        ino: StorageIno,
        lock_owner: u64,
        typ: i32,
    ) -> TiFsResult<bool> {
        let mut inode = self.caches.inode_lock_state.read_cached(
            &ino, &self.f_txn).await?.deref().clone();
        match typ {
            libc::F_WRLCK => {
                if inode.owner_set.len() > 1 {
                    Ok(false)
                } else if inode.owner_set.is_empty() {
                    inode.lk_type = libc::F_WRLCK;
                    inode.owner_set.insert(lock_owner);
                    self.caches.inode_lock_state.write_cached(
                        ino, Arc::new(inode), &self.f_txn).await?;
                    Ok(true)
                } else if inode.owner_set.get(&lock_owner)
                    == Some(&lock_owner)
                {
                    inode.lk_type = libc::F_WRLCK;
                    self.caches.inode_lock_state.write_cached(
                        ino, Arc::new(inode), &self.f_txn).await?;
                    Ok(true)
                } else {
                    Err(FsError::InvalidLock)
                }
            }
            libc::F_RDLCK => {
                if inode.lk_type == libc::F_WRLCK {
                    Ok(false)
                } else {
                    inode.lk_type = libc::F_RDLCK;
                    inode.owner_set.insert(lock_owner);
                    self.caches.inode_lock_state.write_cached(
                        ino, Arc::new(inode), &self.f_txn).await?;
                    Ok(true)
                }
            }
            _ => Err(FsError::InvalidLock),
        }
    }
*/
    pub async fn get_hashes_of_file(
        self: TxnArc,
        ino: StorageIno,
        block_range: Range<BlockIndex>
    ) -> TiFsResult<Vec<u8>> {
        Ok(self.hash_fs.file_read_block_hashes(ino, block_range).await?)
    }
}

