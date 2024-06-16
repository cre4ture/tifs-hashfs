use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
use std::ops::{Deref, Range};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use bytestring::ByteString;
use fuser::TimeOrNow;
use multimap::MultiMap;
use num_bigint::BigUint;
use num_format::{Buffer, Locale, ToFormattedString};
use num_traits::FromPrimitive;
use tikv_client::{Backoff, Key, KvPair, RetryOptions, Transaction, TransactionOptions};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace};
use tikv_client::transaction::Mutation;
use uuid::Uuid;

use crate::fs::index::deserialize_json;
use crate::fs::inode::{DirectoryItem, StorageDirItem, StorageDirItemKind, StorageIno};
use crate::fs::key::BlockAddress;
use crate::fs::meta::MetaStatic;
use crate::fs::utils::stop_watch::AutoStopWatch;
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

use super::block::empty_block;
use super::dir::StorageDirectory;
use super::error::{FsError, TiFsResult};
use super::file_handler::FileHandler;
use super::flexible_transaction::{FlexibleTransaction, SpinningIterResult, SpinningTxn};
use super::fs_config::TiFsConfig;
use super::hash_block::block_splitter::{BlockSplitterRead, BlockSplitterWrite};
use super::hash_block::helpers::UpdateIrregularBlock;
use super::index::serialize_json;
use super::inode::{AccessTime, InoDescription, InoLockState, InoSize, ModificationTime, StorageFileAttr, StorageFilePermission, TiFsHash};
use super::key::{read_big_endian, HashedBlockMeta, KeyParser, PendingDeleteMeta, ScopedKeyBuilder, ROOT_INODE};
use super::meta::Meta;
use super::mode::as_file_perm;
use super::pending_deletes::PendingDeletes;
use super::szymanskis_critical_section::CriticalSectionKeyLock;
use super::parsers;
use super::reply::StatFs;
use super::tikv_fs::TiFsCaches;
use super::transaction_client_mux::TransactionClientMux;

use tikv_client::Result as TiKvResult;

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


fn get_time_from_time_or_now(time: TimeOrNow) -> SystemTime {
    match time {
        TimeOrNow::SpecificTime(t) => t,
        TimeOrNow::Now => SystemTime::now(),
    }
}

fn set_if_changed<T: std::cmp::PartialEq>(
    change_cnt: &mut usize,
    field: &mut T,
    new: Option<T>
) {
    if let Some(new_value) = new {
        let changed = *field != new_value;
        if changed {
            *change_cnt += 1;
            *field = new_value;
        }
    }
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
    pub f_txn: FlexibleTransaction,
    fs_config: TiFsConfig,
    block_size: u64,            // duplicate of fs_config.block_size. Keep it to avoid refactoring efforts.
    max_name_len: u32,
    caches: TiFsCaches,
}

pub type TxnArc = Arc<Txn>;

impl Txn {
    fn inline_data_threshold(&self) -> u64 {
        self.fs_config.inline_data_limit
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn fs_config(&self) -> TiFsConfig {
        self.fs_config.clone()
    }

    fn check_space_left(self: TxnArc, meta: &Meta) -> TiFsResult<()> {
        match meta.last_stat {
            Some(ref stat) if stat.bavail == 0 => {
                Err(FsError::NoSpaceLeft(stat.bsize as u64 * stat.blocks))
            }
            _ => Ok(()),
        }
    }

    pub async fn begin_optimistic(
        instance_id: Uuid,
        client: Arc<TransactionClientMux>,
        raw: Arc<tikv_client::RawClient>,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
        max_name_len: u32,
    ) -> TiFsResult<TxnArc> {
        let txn = if fs_config.pure_raw { None } else {
            let options = TransactionOptions::new_optimistic().use_async_commit();
            let options = options.retry_options(RetryOptions {
                region_backoff: DEFAULT_REGION_BACKOFF,
                lock_backoff: OPTIMISTIC_BACKOFF,
            });
            Some(client.give_one_transaction(&options).await?)
        }.map(|x|RwLock::new(x));
        Ok(TxnArc::new_cyclic(|weak| { Self {
                weak: weak.clone(),
                _instance_id: instance_id,
                f_txn: FlexibleTransaction::new_txn(client, txn, raw, fs_config.clone()),
                fs_config: fs_config.clone(),
                block_size: fs_config.block_size,
                max_name_len,
                caches,
            }
        }))
    }

    pub fn pure_raw(
        instance_id: Uuid,
        client: Arc<TransactionClientMux>,
        raw: Arc<tikv_client::RawClient>,
        fs_config: &TiFsConfig,
        caches: TiFsCaches,
        max_name_len: u32,
    ) -> TxnArc {
        Arc::new_cyclic(|weak| Txn {
            weak: weak.clone(),
            _instance_id: instance_id,
            f_txn: FlexibleTransaction::new_pure_raw(
                client, raw, fs_config.clone()),
            fs_config: fs_config.clone(),
            block_size: fs_config.block_size,
            max_name_len,
            caches,
        })
    }

    pub async fn open(self: Arc<Self>, ino: StorageIno, use_id: Uuid) -> TiFsResult<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.f_txn.put(key, &[]).await?;
        tracing::debug!("open-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn close(self: Arc<Self>, ino: StorageIno, use_id: Uuid) -> TiFsResult<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // de-publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.f_txn.delete(key).await?;
        tracing::debug!("close-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn read_hash_of_file(self: TxnArc, ino: StorageIno, offset: u64, size: u64) -> TiFsResult<Vec<u8>> {

        let hash_size = self.fs_config.hash_len as u64;
        let offset = offset.clamp(0, hash_size);
        let remaining = hash_size - offset;
        let size = size.clamp(0, remaining);

        let mut result = Vec::with_capacity(hash_size as usize);
        let mut ino_size_arc = self.caches.inode_size.read_cached(&ino, &self.f_txn).await?;
        while ino_size_arc.data_hash.is_none() {
            let change_iteration = ino_size_arc.change_iteration;
            let size_in_blocks = ino_size_arc.size().div_ceil(self.fs_config.block_size);
            let hash_data = self.clone().read_hashes_of_file(ino, 0, (size_in_blocks * hash_size) as u32).await?;
            let full_data_hash = self.fs_config.calculate_hash(&hash_data);
            trace!("freshly_calculated_hash: {full_data_hash:x?}");

            let lock = self.caches.get_or_create_ino_lock(ino).await;
            ino_size_arc = self.caches.inode_size.read_cached(&ino, &self.f_txn).await?;
            // was data changed in the meantime?
            if ino_size_arc.change_iteration == change_iteration {
                let mut ino_data_mut = ino_size_arc.deref().clone();
                ino_data_mut.data_hash = Some(full_data_hash);
                ino_size_arc = Arc::new(ino_data_mut);
                self.caches.inode_size.write_cached(ino, ino_size_arc.clone(), &self.f_txn).await?;
            }
            drop(lock);
        }

        if let Some(precalculated_hash) = &ino_size_arc.data_hash {
            trace!("precalculated_hash: {precalculated_hash:x?}");
            result.extend_from_slice(&precalculated_hash[offset as usize..(offset+size) as usize]);
            return Ok(result);
        }

        unreachable!();
    }

    #[instrument(skip(self))]
    pub async fn read_hashes_of_file(self: TxnArc, ino: StorageIno, offset: u64, size: u32) -> TiFsResult<Vec<u8>> {
        let hash_size = self.fs_config.hash_len as u64;
        let first_block = offset / hash_size;
        let first_block_offset = offset % hash_size;
        let block_cnt = (first_block_offset + size as u64) / hash_size;
        let remainder = (first_block_offset + size as u64) % hash_size;
        let fetch_block_cnt = if remainder != 0 {block_cnt + 1} else {block_cnt};
        let hashes = self.clone().hb_get_block_hash_list_by_block_range_chunked(ino, first_block..(first_block+fetch_block_cnt)).await?;
        let mut result = Vec::with_capacity((fetch_block_cnt * hash_size) as usize);
        let zero_hash = self.fs_config.calculate_hash(&[]);
        for block_id in first_block..(first_block+block_cnt) {
            let block_hash = hashes.get(&BlockAddress { ino, index: block_id }).unwrap_or(&zero_hash);
            let offset = if block_id == first_block {first_block_offset} else {0};
            result.extend_from_slice(&block_hash[offset as usize..hash_size as usize]);
        }
        if remainder != 0 {
            let block_hash = hashes.get(&BlockAddress { ino, index: block_cnt }).unwrap_or(&zero_hash);
            result.extend_from_slice(&block_hash[0..remainder as usize]);
        }
        trace!("res-len-end: {}", result.len());
        Ok(result)
    }

    pub async fn read(self: Arc<Self>, ino: StorageIno, start: u64, size: u32) -> TiFsResult<Vec<u8>> {
        self.clone().read_data(ino, start, Some(size as u64), self.fs_config.enable_atime).await
    }

    pub async fn reserve_new_ino(self: TxnArc) -> TiFsResult<StorageIno> {
        let read_meta = self
            .clone().read_meta()
            .await?;

        let mut dyn_meta = if let Some(meta) = read_meta {
            meta
        } else {
            let meta_static = MetaStatic {
                block_size: self.fs_config.block_size as u64,
                hashed_blocks: self.fs_config.hashed_blocks,
                hash_algorithm: self.fs_config.hash_algorithm.to_string(),
            };
            let static_key = Key::from(self.key_builder().meta_static());
            self.f_txn.put(static_key, meta_static.serialize()?).await?;

            Meta {
                inode_next: ROOT_INODE.0,
                last_stat: None,
            }
        };

        self.clone().check_space_left(&dyn_meta)?;

        let ino = dyn_meta.inode_next;
        dyn_meta.inode_next += 1;

        self.save_meta(&dyn_meta).await?;
        debug!("reserved new ino: {}", ino);
        Ok(StorageIno(ino))
    }

    pub async fn check_if_dir_entry_exists(self: Arc<Self>, parent: StorageIno, name: &ByteString) -> TiFsResult<bool> {
        Ok(self.get_directory_item(parent, name.clone()).await?.is_some())
    }

    pub async fn connect_inode_to_directory(
        self: Arc<Self>,
        parent: StorageIno,
        name: &ByteString,
        inode: &InoDescription
    ) -> TiFsResult<()> {
        if parent >= ROOT_INODE {
            if self.clone().check_if_dir_entry_exists(parent, name).await? {
                return Err(FsError::FileExist {
                    file: name.to_string(),
                });
            }
            self.clone().add_directory_child(
                parent,
                name.clone(),
                &StorageDirItem { ino: inode.ino, typ: inode.typ },
            ).await?;
        }
        Ok(())
    }

    pub async fn make_inode(
        self: TxnArc,
        parent: StorageIno,
        name: ByteString,
        typ: StorageDirItemKind,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
        rdev: u32,
        inline_data: Option<Vec<u8>>,
    ) -> TiFsResult<(Arc<InoDescription>, Arc<InoSize>, Arc<StorageFileAttr>)> {
        let ino = self.clone().reserve_new_ino().await?;

        let ino_desc = Arc::new(InoDescription{
            ino,
            creation_time: SystemTime::now(),
            typ,
        });
        let mut i_size = InoSize::new();
        inline_data.map(|data| i_size.set_inline_data(data));
        let ino_size = Arc::new(i_size);
        let ino_attr = Arc::new(StorageFileAttr{
            perm,
            uid,
            gid,
            rdev,
            flags: 0,
            last_change: SystemTime::now(),
        });

        self.caches.inode_desc.write_cached(ino, ino_desc.clone(), &self.f_txn).await?;
        self.caches.inode_size.write_cached(ino, ino_size.clone(), &self.f_txn).await?;
        self.caches.inode_attr.write_cached(ino, ino_attr.clone(), &self.f_txn).await?;
        self.clone().connect_inode_to_directory(parent, &name, &ino_desc).await?;

        Ok((ino_desc, ino_size, ino_attr))
    }

    pub async fn get_directory_item(self: Arc<Self>, parent: StorageIno, name: ByteString) -> TiFsResult<Option<StorageDirItem>> {
        let key = Key::from(self.key_builder().directory_child(parent, &name));
        trace!("get dir item by key: {:?}", key);
        self.f_txn.get(key)
            .await
            .map_err(FsError::from)
            .and_then(|value| {
                value
                    .map(|data| Ok(deserialize_json::<StorageDirItem>(&data)?))
                    .transpose()
            })
    }

    pub async fn add_directory_child(self: TxnArc, parent: StorageIno, name: ByteString, item: &StorageDirItem) -> TiFsResult<()> {
        let lock_key = self.key_builder().inode_link_count(item.ino);
        let _critical_section = CriticalSectionKeyLock::new(self.clone(), lock_key).await?;
        let key = Key::from(self.key_builder().directory_child(parent, &name));
        let key_rev = Key::from(self.key_builder().parent_link(item.ino, parent, &name));
        trace!("set dir item by key: {:?}, name: {:?}, item: {:?}", key, name, item);
        let value = serialize_json(&item)?;
        let mut put_pairs = Vec::with_capacity(2);
        put_pairs.push(KvPair(key, value));
        put_pairs.push(KvPair(key_rev, vec![]));
        Ok(self.f_txn.batch_put(put_pairs).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_directory_child(self: TxnArc, parent: StorageIno, name: ByteString, ino: StorageIno) -> TiFsResult<()> {
        let lock_key = self.key_builder().inode_link_count(ino);
        let _critical_section = CriticalSectionKeyLock::new(self.clone(), lock_key).await?;
        let key = Key::from(self.key_builder().directory_child(parent, &name));
        let key_rev = Key::from(self.key_builder().parent_link(ino, parent, &name));
        let mut mutations = Vec::with_capacity(2);
        mutations.push(Mutation::Delete(key));
        mutations.push(Mutation::Delete(key_rev));
        self.f_txn.batch_mutate(mutations).await?;

        let key_usages = self.key_builder().parent_link_scan(ino);
        let usage_count = self.f_txn.scan_keys(key_usages, MAX_TIKV_SCAN_LIMIT).await?.len();
        if usage_count == 0 {
            self.clone().remove_inode(ino).await?;
            self.remove_inode_data(ino).await?;
        }
        Ok(())
    }

    pub async fn read_inode_uncached(self: TxnArc, ino: StorageIno) -> TiFsResult<Arc<InoDescription>> {
        self.caches.inode_desc.read_uncached(&ino, &self.f_txn).await
    }

    pub async fn read_inode(self: TxnArc, ino: StorageIno) -> TiFsResult<Arc<InoDescription>> {
        self.caches.inode_desc.read_cached(&ino, &self.f_txn).await
    }

    pub async fn read_ino_size(self: TxnArc, ino: StorageIno) -> TiFsResult<Arc<InoSize>> {
        self.caches.inode_size.read_cached(&ino, &self.f_txn).await
    }

    pub async fn read_ino_lock_state(self: TxnArc, ino: StorageIno) -> TiFsResult<Arc<InoLockState>> {
        self.caches.inode_lock_state.read_cached(&ino, &self.f_txn).await
    }

    pub async fn write_ino_lock_state(self: TxnArc, ino: StorageIno, state: InoLockState
    ) -> TiFsResult<()> {
        self.caches.inode_lock_state.write_cached(ino, Arc::new(state), &self.f_txn).await
    }

    pub async fn read_inode_arc(self: Arc<Self>, ino: StorageIno) -> TiFsResult<Arc<InoDescription>> {
        self.read_inode(ino).await
    }

    pub async fn save_inode(self: TxnArc, inode: &Arc<InoDescription>) -> TiFsResult<()> {
        self.caches.inode_desc.write_cached(inode.storage_ino(), inode.clone(), &self.f_txn).await?;
        debug!("saved inode: {:?}", inode);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_inode(self: TxnArc, ino: StorageIno) -> TiFsResult<()> {
        let key = Key::from(self.key_builder().inode_description(ino));
        self.f_txn.delete(key).await?;
        self.caches.inode_desc.delete_with_cache(&ino, &self.f_txn).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove_inode_data(self: TxnArc, ino: StorageIno) -> TiFsResult<()> {
        if self.fs_config.hashed_blocks {
            self.hb_clear_data(ino).await?;
        } else {
            self.traditional_clear_data(ino).await?;
        }
        Ok(())
    }

    pub fn key_builder(&self) -> ScopedKeyBuilder {
        ScopedKeyBuilder::new(&self.fs_config.key_prefix)
    }

    pub fn key_parser<'fl>(&'fl self, i: &'fl mut std::slice::Iter<'fl, u8>) -> TiFsResult<KeyParser<'fl>> {
        KeyParser::start(i, &self.fs_config.key_prefix, self.fs_config.hash_len)
    }

    pub async fn read_static_meta(self: TxnArc) -> TiFsResult<Option<MetaStatic>> {
        let key = Key::from(self.key_builder().meta_static());
        let opt_data = self.f_txn.get(key).await?;
        opt_data.map(|data| MetaStatic::deserialize(&data)).transpose()
    }

    pub async fn read_meta(self: TxnArc) -> TiFsResult<Option<Meta>> {
        let key = Key::from(self.key_builder().meta());
        let opt_data = self.f_txn.get(key).await?;
        opt_data.map(|data| Meta::deserialize(&data)).transpose()
    }

    pub async fn save_meta(self: TxnArc, meta: &Meta) -> TiFsResult<()> {
        let key = Key::from(self.key_builder().meta());
        self.f_txn.put(key, meta.serialize()?).await?;
        Ok(())
    }

    async fn transfer_inline_data_to_block(self: TxnArc, ino: StorageIno, ino_size: &mut InoSize) -> TiFsResult<()> {
        debug_assert!(ino_size.size() <= self.inline_data_threshold());
        let key = Key::from(self.key_builder().block(BlockAddress { ino, index: 0 }));
        let data = ino_size.take_inline_data().unwrap();
        self.f_txn.put(key, data).await?;
        Ok(())
    }

    async fn write_inline_data(
        self: TxnArc,
        ino: StorageIno,
        ino_size: &mut InoSize,
        start: u64,
        data: &[u8],
    ) -> TiFsResult<usize> {
        let size = data.len();
        let start = start as usize;

        let mut inlined = ino_size.take_inline_data().unwrap_or_else(Vec::new);
        if start + size > inlined.len() {
            inlined.resize(start + size, 0);
        }
        inlined[start..start + size].copy_from_slice(data);

        ino_size.last_change = SystemTime::now();
        ino_size.set_size(inlined.len() as u64, self.block_size);
        ino_size.set_inline_data(inlined);
        self.caches.inode_size.write_cached(
            ino, Arc::new(ino_size.clone()), &self.f_txn).await?;

        Ok(size)
    }

    async fn read_inline_data(
        &self,
        inode: &InoSize,
        start: u64,
        size: u64,
    ) -> TiFsResult<Vec<u8>> {
        let start = start as usize;
        let size = size as usize;

        let inlined = inode.inline_data().unwrap();
        debug_assert!(inode.size() as usize == inlined.len());
        let mut data = vec![0; size];
        if inlined.len() > start {
            let to_copy = size.min(inlined.len() - start);
            data[..to_copy].copy_from_slice(&inlined[start..start + to_copy]);
        }
        Ok(data)
    }

    async fn hb_read_data(self: TxnArc, ino: StorageIno, start: u64, size: u64) -> TiFsResult<Vec<u8>> {

        let bs = BlockSplitterRead::new(self.block_size, start, size);
        let block_range = bs.block_range();

        let block_hashes = self.clone().hb_get_block_hash_list_by_block_range(ino, block_range.clone()).await?;
        //tracing::debug!("block_hashes(count: {}): {:?}", block_hashes.len(), block_hashes);
        let block_hashes_set = HashSet::from_iter(block_hashes.values().cloned());
        let blocks_data = self.clone().hb_get_block_data_by_hashes(&block_hashes_set).await?;

        let result = parsers::hb_read_from_blocks(ino, &block_range, &bs, &block_hashes, &blocks_data)?;

        let mut buf_start = Buffer::default();
        buf_start.write_formatted(&start, &Locale::en);
        tracing::debug!("hb_read_data(ino: {ino}, start:{buf_start}, size: {size}) - block_size: {}, blocks_count: {}, range: [{}..{}[ -> {} read", bs.block_size, bs.block_count, block_range.start, block_range.end, result.len());

        if result.len() < size as usize {
            let block_data_lengths = blocks_data.iter().map(|(key, data)|(key, data.len())).collect::<Vec<_>>();
            tracing::debug!("incomplete read - (ino: {ino}, start:{buf_start}, size: {size}): len:{}, block_hashes:{:?}, block_lengths:{:?}", result.len(), block_hashes, block_data_lengths);
        }

        Ok(result)
    }

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

    pub async fn read_data(
        self: Arc<Self>,
        ino: StorageIno,
        start: u64,
        chunk_size: Option<u64>,
        update_atime: bool,
    ) -> TiFsResult<Vec<u8>> {

        if update_atime {
            let atime = AccessTime(SystemTime::now());
            self.caches.inode_atime.write_cached(
                ino, Arc::new(atime), &self.f_txn).await?;
        }

        let ino_size = self.caches.inode_size.read_cached(&ino, &self.f_txn).await?;
        if start >= ino_size.size() {
            return Ok(Vec::new());
        }

        let max_size = ino_size.size() - start;
        let size = chunk_size.unwrap_or(max_size).min(max_size);

        if ino_size.inline_data().is_some() {
            return self.clone().read_inline_data(&ino_size, start, size).await;
        }

        if self.fs_config.hashed_blocks {
            self.hb_read_data(ino, start, size).await
        } else {
            self.read_data_traditional(ino, start, size).await
        }
    }

    pub async fn hb_clear_data_single_block(self: TxnArc, addr: BlockAddress) -> TiFsResult<()> {
        let key = self.key_builder().block_hash(addr);
        let mut spin = SpinningTxn {
            backoff: Backoff::decorrelated_jitter_backoff(10, 500, 10),
        };
        let prev_hash = loop {
            let mut mini = self.f_txn.mini_txn().await?;
            let result = Self::hb_replace_block_hash_for_address(&mut mini, &key, None).await;
            match spin.end_iter(result, mini).await {
                SpinningIterResult::Done(prev_hash) => break prev_hash,
                SpinningIterResult::TryAgain => {},
                SpinningIterResult::Failed(err) => return Err(err.into()),
            }
        };

        let mut spin = SpinningTxn {
            backoff: Backoff::decorrelated_jitter_backoff(10, 500, 10),
        };
        if let Some(prev_h) = prev_hash {
            loop {
                let mut mini = self.f_txn.mini_txn().await?;
                let result = self.hb_decrement_blocks_reference_count_and_delete_if_zero_reached(&mut mini, &prev_h, 1).await;
                match spin.end_iter(result, mini).await {
                    SpinningIterResult::Done(prev_hash) => break prev_hash,
                    SpinningIterResult::TryAgain => {},
                    SpinningIterResult::Failed(err) => return Err(err.into()),
                }
            }
        }
        Ok(())
    }

    pub async fn hb_clear_data(self: TxnArc, ino: StorageIno) -> TiFsResult<u64> {

        let ino_size_key = self.key_builder().inode_size(ino);
        let key_lock = CriticalSectionKeyLock::new(
            self.clone(), ino_size_key).await?;

        let mut ino_size = self.caches.inode_size.read_uncached(
            &ino, &self.f_txn).await?.deref().clone();
        let block_cnt = ino_size.size().div_ceil(self.block_size);

        let mut parallel_executor = AsyncParallelPipeStage::new(
            self.fs_config.parallel_jobs_delete);
        for block in 0..block_cnt {
            let addr = BlockAddress { ino, index: block };
            parallel_executor.push(self.clone().hb_clear_data_single_block(addr)).await;
        }
        parallel_executor.wait_finish_all().await;
        for r in parallel_executor.get_results_so_far() {
            r?;
        }

        let clear_size = ino_size.size();
        ino_size.set_size(0, self.fs_config.block_size);
        ino_size.last_change = SystemTime::now();
        self.caches.inode_size.write_cached(ino, Arc::new(ino_size), &self.f_txn).await?;
        key_lock.unlock().await?;
        Ok(clear_size)
    }

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

    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range(self: TxnArc, ino: StorageIno, block_range: Range<u64>
    ) -> TiFsResult<HashMap<BlockAddress, TiFsHash>> {
        let range = self.key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.f_txn.scan(
                range,
                block_range.count() as u32,
            )
            .await?;
        let mut result = HashMap::<BlockAddress, TiFsHash>::new();
        for KvPair(k, hash) in iter.into_iter() {
            let key_vec = Vec::from(k);
            let mut i = key_vec.iter();
            let key = self.key_parser(&mut i)?.parse_key_block_address()?;
            if hash.len() != self.fs_config.hash_len {
                tracing::error!("hash lengths mismatch!");
                continue;
            };
            result.insert(key, hash);
        }
        trace!("result: len:{}", result.len());
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range_chunked(self: TxnArc, ino: StorageIno, block_range: Range<u64>
    ) -> TiFsResult<HashMap<BlockAddress, TiFsHash>> {
        let mut i = block_range.start;
        let mut result = HashMap::with_capacity(block_range.clone().count());
        while i < block_range.end {
            let chunk_start = i;
            let chunk_end = (chunk_start+MAX_TIKV_SCAN_LIMIT as u64).min(block_range.end);
            i = chunk_end;

            let chunk = self.clone().hb_get_block_hash_list_by_block_range(ino, chunk_start..chunk_end).await?;
            result.extend(chunk.into_iter());
        }
        trace!("result: len:{}", result.len());
        Ok(result)
    }

    pub async fn hb_get_block_data_by_hashes(self: TxnArc, hash_list: &HashSet<TiFsHash>
    ) -> TiFsResult<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        let mut watch = AutoStopWatch::start("get_hash_blocks_data");

        let mut result = HashMap::new();
        let mut keys = Vec::<Key>::new();
        for hash in hash_list {
            if let Some(cached) = self.caches.block.get(hash).await {
                result.insert(hash.clone(), cached);
            } else {
                keys.push(self.key_builder().hashed_block(hash).into());
            }
        }

        watch.sync("cached");

        let mut uncached_blocks = HashMap::new();
        let rcv_data_list = self.f_txn.batch_get(keys).await?;
        for KvPair(k, v) in rcv_data_list {
            let key_data = Vec::from(k);
            let mut i = key_data.iter();
            let hash = self.key_parser(&mut i)?.parse_key_hashed_block()?;
            let value = Arc::new(v);
            uncached_blocks.insert(hash.clone(), value.clone());
            self.caches.block.insert(hash, value).await;
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

    pub fn parse_keys_to_struct(&self, result: Vec<Key>, my_uuid: &Uuid) -> TiFsResult<HashedBlockMetaData> {
        let mut all = HashedBlockMetaData::new();
        for key in result.into_iter().map(|k|Vec::from(k)) {
            let mut i = key.iter();
            let kp = self.key_parser(&mut i)?.parse_hash_block_key_x()?;
            match kp.meta {
                HashedBlockMeta::ANamedBlock => {
                    let (_kp, uuid) = kp.pre.parse_uuid()?;
                    all.existing.insert(uuid);
                }
                HashedBlockMeta::BNamedUsages => {
                    let (_kp, uuid) = kp.pre.parse_uuid()?;
                    if uuid != *my_uuid { // filter own registration
                        all.named_usages.insert(uuid);
                    }
                }
                HashedBlockMeta::CCountedNamedUsages => {
                    let (_kp, uuid) = kp.pre.parse_uuid()?;
                    all.counters.insert(uuid);
                }
            }
        }
        Ok(all)
    }

    async fn hb_increment_blocks_reference_count(
        &self,
        mini: &mut Transaction,
        block_key: Vec<u8>,
        cnt: u64,
    ) -> TiKvResult<BigUint> {
        let prev_counter_value = mini.get(block_key.clone()).await?
        .map(|vec|BigUint::from_bytes_be(&vec))
        .unwrap_or(BigUint::from_u8(0u8).unwrap());
        let new_counter_value = prev_counter_value.clone() + cnt;
        mini.put(block_key.clone(), new_counter_value.to_bytes_be()).await?;
        Ok(prev_counter_value)
    }

    async fn hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
        &self,
        mini: &mut Transaction,
        hash: &Vec<u8>,
        cnt: u64,
    ) -> TiKvResult<()> {
        let block_ref_cnt_key = self.key_builder().named_hashed_block_x(
            hash, Some(HashedBlockMeta::CCountedNamedUsages), None);

        let prev_counter_value = mini.get(block_ref_cnt_key.clone()).await?
            .map(|vec|BigUint::from_bytes_be(&vec))
            .unwrap_or(BigUint::from_u8(0u8).unwrap());
        let mut actual_dec = BigUint::from_u64(cnt).unwrap();
        if prev_counter_value < actual_dec {
            tracing::error!("full decrement by {cnt} of block reference counter not possible with value {prev_counter_value}.");
            actual_dec = prev_counter_value.clone();
        }
        let new_counter_value = prev_counter_value.clone() - actual_dec;
        if new_counter_value == BigUint::from_u8(0).unwrap() {
            let block_key = self.key_builder().hashed_block(hash);
            let mut deletes = Vec::with_capacity(2);
            deletes.push(Mutation::Delete(Key::from(block_ref_cnt_key)));
            deletes.push(Mutation::Delete(Key::from(block_key)));
            mini.batch_mutate(deletes).await?;
            tracing::warn!("deleting block with hash: {hash:?}");
        } else {
            mini.put(block_ref_cnt_key.clone(), new_counter_value.to_bytes_be()).await?;
        }
        Ok(())
    }

    pub async fn hb_upload_block_reference_counting(&self, hash: &TiFsHash, cnt: u64, data: Vec<u8>
    ) -> TiFsResult<()> {
        let mut spin = SpinningTxn{
            backoff: Backoff::decorrelated_jitter_backoff(10, 500, 10)
        };
        let block_ref_cnt_key = self.key_builder().named_hashed_block_x(
            hash, Some(HashedBlockMeta::CCountedNamedUsages), None);

        // get and increment block reference counter (with automatic retry)
        let prev_cnt = loop {
            let mut mini = self.f_txn.mini_txn().await?;
            let result = self.hb_increment_blocks_reference_count(
                &mut mini, block_ref_cnt_key.clone(), cnt).await;
            match spin.end_iter(result, mini).await {
                SpinningIterResult::Done(prev_cnt) => break prev_cnt,
                SpinningIterResult::TryAgain => {},
                SpinningIterResult::Failed(err) => return Err(err.into()),
            }
        };

        // Upload block if new
        if prev_cnt == BigUint::from_u8(0).unwrap() {
            let key = self.f_txn.key_builder().hashed_block(hash);
            self.f_txn.put(key, data).await?;
        }
        Ok(())
    }

    pub async fn hb_replace_block_hash_for_address(
        mini: &mut Transaction,
        key: &Vec<u8>,
        new_hash: Option<&TiFsHash>,
    ) -> TiKvResult<Option<TiFsHash>> {
        let prev_block_hash = mini.get(key.clone()).await?;
        if let Some(new_hash) = new_hash {
            mini.put(key.clone(), new_hash.clone()).await?;
        } else {
            mini.delete(key.clone()).await?;
        }
        Ok(prev_block_hash)
    }

    pub async fn hb_upload_block_reference_counting_and_write_addresses(
        self: TxnArc, hash: TiFsHash, cnt: u64, data: Vec<u8>, addresses: Vec<BlockAddress>,
    ) -> TiFsResult<()> {
        self.hb_upload_block_reference_counting(&hash, cnt, data).await?;

        let mut decrement_cnts = HashMap::<TiFsHash, u64>::new();
        for addr in addresses {
            let key = self.key_builder().block_hash(addr);
            let mut spin = SpinningTxn {
                backoff: Backoff::decorrelated_jitter_backoff(10, 500, 10),
            };
            let prev_hash = loop {
                let mut mini: Transaction = self.f_txn.mini_txn().await?;
                let result = Self::hb_replace_block_hash_for_address(&mut mini, &key, Some(&hash)).await;
                match spin.end_iter(result, mini).await {
                    SpinningIterResult::Done(prev_hash) => break prev_hash,
                    SpinningIterResult::TryAgain => {},
                    SpinningIterResult::Failed(err) => {
                        tracing::error!("failed to exchange hash for block-address");
                        return Err(err.into());
                    }
                }
            };

            if let Some(pre_hash) = prev_hash {
                decrement_cnts.insert(pre_hash.clone(),
                    1 + decrement_cnts.get(&pre_hash).copied().unwrap_or(0));
            }
        }

        for (prev_hash, dec_cnt) in decrement_cnts {
            let mut spin = SpinningTxn {
                backoff: Backoff::decorrelated_jitter_backoff(10, 500, 10),
            };
            loop {
                let mut mini: Transaction = self.f_txn.mini_txn().await?;
                let result = self.hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
                    &mut mini, &prev_hash, dec_cnt).await;
                match spin.end_iter(result, mini).await {
                    SpinningIterResult::Done(()) => break,
                    SpinningIterResult::TryAgain => {},
                    SpinningIterResult::Failed(err) => return Err(err.into()),
                }
            }
        }
        Ok(())
    }

    pub async fn hb_upload_new_blocks(self: Arc<Self>, new_blocks: HashMap<TiFsHash, Arc<Vec<u8>>>) -> TiFsResult<()> {
        let mut mutations_txn = Vec::<Mutation>::new();

        for (k, new_block) in new_blocks {
            mutations_txn.push(Mutation::Put(self.key_builder().hashed_block(&k).into(), new_block.deref().clone()));
            //mutations_txn.push(Mutation::Put(self.key_builder().hashed_block_exists(&k).into(), vec![]));
        }

        if mutations_txn.len() > 0 {
            self.f_txn.batch_mutate(mutations_txn).await?;
        }

        Ok(())
    }

    pub async fn hb_upload_new_block_addresses(me: Arc<Self>, blocks_to_assign: VecDeque<(TiFsHash, Vec<BlockAddress>)>) -> TiFsResult<()> {
        let mut kv_pairs = Vec::<KvPair>::new();
        for (hash, addresses) in blocks_to_assign {
            for addr in addresses {
                kv_pairs.push(KvPair(me.key_builder().block_hash(addr).into(), hash.clone()));
            }
        }
        Ok(me.f_txn.batch_put(kv_pairs).await?)
    }

    pub async fn hb_write_data(self: Arc<Self>, fh: Arc<FileHandler>, start: u64, data: Bytes) -> TiFsResult<bool> {

        let mut watch = AutoStopWatch::start("hb_wrt");
        let bs = BlockSplitterWrite::new(self.block_size, start, &data);
        let block_range = bs.get_range();
        let ino = fh.ino();

        let hash_list_prev = self.clone().hb_get_block_hash_list_by_block_range(ino, block_range.clone()).await?;
        let input_block_hashes = hash_list_prev.len();
        watch.sync("hp");

        let mut pre_data_hash_request = HashSet::<TiFsHash>::new();
        let first_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            self.fs_config.clone(),
            ino,
            bs.first_data,
            bs.first_data_start_position,
            &hash_list_prev,
            &mut pre_data_hash_request
        );
        let last_data_handler = UpdateIrregularBlock::get_and_add_original_block_hash(
            self.fs_config.clone(),
            ino,
            bs.last_data,
            0,
            &hash_list_prev,
            &mut pre_data_hash_request
        );

        let pre_data = self.clone().hb_get_block_data_by_hashes(&pre_data_hash_request).await?;
        watch.sync("pd");
        let mut new_blocks = HashMap::new();
        let mut new_block_hashes = HashMap::new();

        first_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);
        last_data_handler.get_and_modify_block_and_publish_hash(&pre_data, &mut new_blocks, &mut new_block_hashes);

        for (index, chunk) in bs.mid_data.data.chunks(self.block_size as usize).enumerate() {
            let hash = self.fs_config.calculate_hash(chunk);
            new_blocks.insert(hash.clone(), Arc::new(chunk.to_vec()));
            new_block_hashes.insert(BlockAddress{ino, index: bs.mid_data.block_index + index as u64}, hash);
        }
        watch.sync("hash");

        for (hash, new_block) in &new_blocks {
            self.caches.block.insert(hash.clone(), new_block.clone()).await;
        }
        watch.sync("add_cache");

        let mut parallel_executor = AsyncParallelPipeStage::new(self.fs_config.parallel_jobs);

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
        let mm = new_block_hashes.into_iter()
            .map(|(k,v)| (v,k)).collect::<MultiMap<_,_>>();

        for (hash, addresses) in mm {

            let data = new_blocks.get(&hash).unwrap();
            let fut = self.clone().hb_upload_block_reference_counting_and_write_addresses(
                hash, addresses.len() as u64, data.deref().clone(), addresses);

            parallel_executor.push(fut).await;
        }

        parallel_executor.wait_finish_all().await;
        let total_jobs = parallel_executor.get_total();

        watch.sync("pm");

        tracing::debug!("hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,jobs:{total_jobs}({skipped_new_block_hashes}/{input_block_hashes} skipped)", ino, start.to_formatted_string(&Locale::en), data.len().to_formatted_string(&Locale::en), bs.block_size, block_range.end - block_range.start, block_range.start, block_range.end);

        let was_modified = new_block_hashes_len > 0;
        Ok(was_modified)
    }

    pub async fn read_pending_deletes(self: TxnArc) -> TiFsResult<PendingDeletes> {
        let range = self.key_builder().pending_delete_scan_list();
        let pairs = self.f_txn.scan(range, MAX_TIKV_SCAN_LIMIT).await?;
        let mut iteration_number = None;
        let mut last_update = None;
        let mut pending = Vec::new();
        for KvPair(key, value) in pairs {
            let key_buffer = Vec::from(key);
            let mut i = key_buffer.iter();
            let base_key = self.key_parser(&mut i)?.parse_pending_deletes_x()?;
            match base_key.meta {
                PendingDeleteMeta::AIterationNumber => {
                    let mut vi = value.iter();
                    iteration_number = Some(read_big_endian::<8, u64>(&mut vi)?);
                }
                PendingDeleteMeta::BLastUpdate => {
                    let mut vi = value.iter();
                    last_update = Some(read_big_endian::<8, u64>(&mut vi)?); // unix timestamp (s)
                }
                PendingDeleteMeta::CPendingDeletes => {
                    pending.push(value); // hash
                }
                other => {
                    return Err(FsError::UnknownError(format!("unexpected meta key: {other:?}")));
                }
            }
        }
        Ok(PendingDeletes {
            iteration_number: iteration_number.ok_or(
                FsError::UnknownError(format!("missing field: iteration_number")))?,
            last_update: last_update.ok_or(
                FsError::UnknownError(format!("missing field: last_update")))?,
            pending
        })
    }

    pub async fn publish_active_writer_state(self: TxnArc, iteration_number: u64, writer_id: Uuid) -> TiFsResult<()> {
        let key = self.key_builder().pending_delete_active_writer(iteration_number, writer_id);
        let unix_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.f_txn.put(key, unix_timestamp.to_be_bytes().to_vec()).await
    }

    pub async fn read_pending_deletes_and_publish_writer_state(self: TxnArc, writer_id: Uuid) -> TiFsResult<PendingDeletes> {
        let output = self.clone().read_pending_deletes().await?;
        self.publish_active_writer_state(output.iteration_number, writer_id).await?;
        Ok(output)
    }

    pub async fn write(self: TxnArc, fh: Arc<FileHandler>, start: u64, data: Bytes) -> TiFsResult<usize> {
        let mut watch = AutoStopWatch::start("write_data");
        let ino = fh.ino();
        debug!("write data at ({})[{}]", ino, start);
        //let meta = self.read_meta().await?.unwrap(); // TODO: is this needed?
        //self.check_space_left(&meta)?;

        let size = data.len();
        let target = start + size as u64;
        let size_changed;
        let lock = self.caches.get_or_create_ino_lock(fh.ino_use.ino).await;
        {
            let write_size_lock = lock.write().await;
            let mut ino_size = self.caches.inode_size.read_cached(
                &ino, &self.f_txn).await?.deref().clone();
            size_changed = target > ino_size.size();
            watch.sync("read_inode");

            let transfer_needed = ino_size.inline_data().is_some() && target > self.inline_data_threshold();
            if transfer_needed {
                self.clone().transfer_inline_data_to_block(ino, &mut ino_size).await?;
                watch.sync("transfer_inline");
            }

            if (ino_size.inline_data().is_some() || ino_size.size() == 0)
                && target <= self.inline_data_threshold()
            {
                let result = self.write_inline_data(ino, &mut ino_size, start, &data).await;
                watch.sync("write_inline");
                return result;
            }

            if transfer_needed {
                self.caches.inode_size.write_cached(
                    ino, Arc::new(ino_size), &self.f_txn).await?;
            }

            drop(write_size_lock);
        }

        let content_was_modified = if self.fs_config.hashed_blocks {
            self.clone().hb_write_data(fh.clone(), start, data).await?
        } else {
            self.clone().write_blocks_traditional(ino, start, &data).await?;
            true
        };

        watch.sync("write impl");

        if size_changed || content_was_modified {
            let wr_lock = lock.write().await;
            let mut ino_size_now = self.caches.inode_size.read_cached(
                &ino, &self.f_txn).await?.deref().clone();
            // parallel writes might have done the size increment already
            let size_still_changed = target > ino_size_now.size();
            if size_still_changed {
                ino_size_now.set_size(target, self.fs_config.block_size);
            }
            // if no hash was available before, no need to store a change
            let hash_removed = ino_size_now.data_hash.take().is_some();
            if size_still_changed || hash_removed {
                ino_size_now.change_iteration += 1;
                ino_size_now.last_change = SystemTime::now();
                self.caches.inode_size.write_cached(
                    ino, Arc::new(ino_size_now), &self.f_txn).await?;
                drop(wr_lock);
                watch.sync("save inode");
            }
        }
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
        let ino_size = self.caches.inode_size.read_cached(&ino, &self.f_txn).await?;
        let size = ino_size.size();

        if self.fs_config.enable_atime {
            let atime = AccessTime(SystemTime::now());
            self.caches.inode_atime.write_cached(
                ino, Arc::new(atime), &self.f_txn).await?;
        }

        self.read_inline_data(&ino_size, 0, size).await
    }

    pub async fn add_hard_link(self: TxnArc, ino: StorageIno, further_parent: StorageIno, new_name: ByteString
    ) -> TiFsResult<Arc<InoDescription>> {
        // check and remove any inode at destination:
        if let Some(old_dir_item) = self.clone().get_directory_item(further_parent, new_name.clone()).await? {
            match old_dir_item.typ {
                StorageDirItemKind::Directory => self.clone().rmdir(further_parent, new_name.clone()).await?,
                _ => self.clone().unlink(further_parent, new_name.clone()).await?,
            }
        }

        let inode = self.clone().read_inode(ino).await?.deref().clone();
        // attach existing ino to a further directory with a new name:
        self.clone().add_directory_child(
            further_parent,
            new_name.clone(),
            &StorageDirItem { ino, typ: inode.typ }).await?;

        Ok(Arc::new(inode))
    }

    #[tracing::instrument(skip(self))]
    pub async fn unlink(
            self: TxnArc,
            parent: StorageIno,
            name: ByteString
    ) -> TiFsResult<()> {
        let Some(dir_entry) = self.clone().get_directory_item(parent, name.clone()).await? else {
            return Err(FsError::FileNotFound { file: name.to_string() });
        };
        tracing::info!("remove_directory_child. parent: {:?}, name: {:?}, entry: {:?}", parent, name, dir_entry);
        self.clone().remove_directory_child(parent, name.clone(), dir_entry.ino).await?;
        Ok(())
    }

    pub async fn rmdir(self: TxnArc, parent: StorageIno, name: ByteString) -> TiFsResult<()> {
        match self.clone().get_directory_item(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(dir_item) => match dir_item.typ {
                StorageDirItemKind::Directory => {
                    if !self.clone()
                        .read_dir(dir_item.ino)
                        .await?.is_empty()
                    {
                        let name_str = name.to_string();
                        debug!("dir({}) not empty", &name_str);
                        return Err(FsError::DirNotEmpty { dir: name_str });
                    }

                    self.clone().unlink(parent, name).await
                }
                _ => Err(FsError::WrongFileType),
            }
        }
    }

    pub async fn lookup_ino(self: TxnArc, parent: StorageIno, name: ByteString) -> TiFsResult<StorageDirItem> {
        self.get_directory_item(parent, name.clone())
            .await?
            .ok_or_else(|| FsError::FileNotFound {
                file: name.to_string(),
            })
    }

    pub async fn get_all_ino_data(self: TxnArc, ino: StorageIno,
    ) -> TiFsResult<(Arc<InoDescription>, Arc<StorageFileAttr>, Arc<InoSize>, AccessTime)> {
        let desc = self.caches.inode_desc.read_cached(&ino, &self.f_txn).await?;
        let attr = self.caches.inode_attr.read_cached(&ino, &self.f_txn).await?;
        let size = self.caches.inode_size.read_cached(&ino, &self.f_txn).await?;
        let atime = self.caches.inode_atime.read_cached(
            &ino, &self.f_txn).await.ok().as_deref().cloned().unwrap_or_else(||{
                AccessTime(size.last_change.max(attr.last_change))
            });
        Ok((desc, attr, size, atime))
    }

    pub async fn set_attributes(
        self: TxnArc,
        ino: StorageIno,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        _size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> TiFsResult<()> {

        if let Some(atime_modification) = atime {
            let new_atime = AccessTime(get_time_from_time_or_now(atime_modification));
            self.caches.inode_atime.write_cached(ino, Arc::new(new_atime), &self.f_txn).await?;
        }

        if let Some(mtime_modification) = mtime {
            let new_mtime = ModificationTime(get_time_from_time_or_now(mtime_modification));
            self.caches.inode_mtime.write_cached(ino, Arc::new(new_mtime), &self.f_txn).await?;
        }

        // TODO: how to deal with fh, chgtime, bkuptime?
        let mut ino_attr = self.clone().caches.inode_attr.read_cached(
            &ino, &self.f_txn).await?.deref().clone();

        let mut cnt = 0 as usize;
        set_if_changed(&mut cnt, &mut ino_attr.perm,
            mode.map(|m|StorageFilePermission(as_file_perm(m))));

        set_if_changed(&mut cnt, &mut ino_attr.uid, uid);
        set_if_changed(&mut cnt, &mut ino_attr.gid, gid);

        set_if_changed(&mut cnt, &mut ino_attr.last_change,
            Some(ctime.unwrap_or(SystemTime::now())));
        set_if_changed(&mut cnt, &mut ino_attr.flags, flags);

        if cnt > 0 {
            self.caches.inode_attr.write_cached(
                ino, Arc::new(ino_attr), &self.f_txn).await?;
        }

        Ok(())
    }

    pub async fn f_allocate(
        self: TxnArc,
        fh: Arc<FileHandler>,
        ino: StorageIno,
        offset: i64,
        length: i64
    ) -> TiFsResult<()> {
        let lock = self.caches.get_or_create_ino_lock(fh.ino_use.ino).await;
        let wr_lock = lock.write().await;
        let mut ino_size = self.caches.inode_size.read_cached(
            &ino, &self.f_txn).await?.deref().clone();
        let target_size = (offset + length) as u64;
        if target_size <= ino_size.size() {
            return Ok(());
        }

        if ino_size.inline_data().is_some() {
            if target_size <= self.inline_data_threshold() {
                let original_size = ino_size.size();
                let data = vec![0; (target_size - original_size) as usize];
                self.clone().write_inline_data(ino, &mut ino_size, original_size, &data).await?;
                return Ok(());
            } else {
                self.clone().transfer_inline_data_to_block(ino, &mut ino_size).await?;
            }
        }

        ino_size.set_size(target_size, self.block_size);
        ino_size.last_change = SystemTime::now();
        self.caches.inode_size.write_cached(ino, Arc::new(ino_size), &self.f_txn).await?;
        drop(wr_lock);
        Ok(())
    }

    pub async fn mkdir(
        self:  TxnArc,
        parent: StorageIno,
        name: ByteString,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
    ) -> TiFsResult<(Arc<InoDescription>, Arc<InoSize>, Arc<StorageFileAttr>)> {
        let (ino_desc, ino_size, ino_attr)
            = self.clone().make_inode(parent, name, StorageDirItemKind::Directory,
                perm, gid, uid, 0, None).await?;
        Ok((ino_desc, ino_size, ino_attr))
    }

    pub async fn read_dir(self: Arc<Self>, ino: StorageIno) -> TiFsResult<StorageDirectory> {
        let range = self.key_builder().directory_child_range(ino);
        //trace!("start scan: {:?}", range);
        let data = self.f_txn.scan(range, 10).await?;
        //trace!("scan result size: {:?}", data.len());
        let mut result = StorageDirectory::with_capacity(data.len());
//        data.iter().enumerate().map(|(i, KvPair(k,v))|{
//            trace!("scan result key #{i}: {:?}, data-len: {}", k, v.len());
//        }).fold((), |_,_|{});
        for (_i, KvPair(k,v)) in data.into_iter().enumerate() {
            let key_buf = Vec::from(k);
            //trace!("process scan result key #{_i}: {:?}", key_buf);
            let mut i = key_buf.iter();
            let (_p_ino, name) = self.key_parser(&mut i)?.parse_directory_child()?;
            let item = deserialize_json::<StorageDirItem>(&v)?;
            result.push(DirectoryItem {
                ino: item.ino,
                name: String::from_utf8_lossy(&name).to_string(),
                typ: item.typ,
            });
        }
        //trace!("read dir data: len: {}", result.len());
        Ok(result)
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
            self.max_name_len,
            0,
        );

        trace!("statfs: {:?}", stat);
        Ok(stat)
    }

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
}
