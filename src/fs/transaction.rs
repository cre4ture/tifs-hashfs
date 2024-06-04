use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
use std::mem;
use std::ops::{Deref, Range, RangeInclusive};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use fuser::TimeOrNow;
use futures::TryFutureExt;
use multimap::MultiMap;
use num_format::{Buffer, Locale, ToFormattedString};
use serde::{Deserialize, Serialize};
use tikv_client::{Backoff, BoundRange, Key, KvPair, RetryOptions, Timestamp, Transaction, TransactionOptions, Value};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace};
use tikv_client::transaction::Mutation;
use uuid::Uuid;

use crate::fs::inode::{StorageDirItem, StorageDirItemKind, StorageIno};
use crate::fs::key::BlockAddress;
use crate::fs::meta::StaticFsParameters;
use crate::fs::utils::stop_watch::AutoStopWatch;
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

use super::block::empty_block;
use super::dir::StorageDirectory;
use super::error::{FsError, Result, TiFsResult};
use super::file_handler::FileHandler;
use super::fs_config::TiFsConfig;
use super::hash_block::block_splitter::{BlockSplitterRead, BlockSplitterWrite};
use super::hash_block::helpers::UpdateIrregularBlock;
use super::index::Index;
use super::inode::{AccessTime, InoDescription, InoLockState, InoSize, ModificationTime, StorageFileAttr, StorageFilePermission, TiFsHash};
use super::key::{KeyGenerator, KeyParser, ScopedKeyBuilder, ROOT_INODE};
use super::meta::Meta;
use super::mode::as_file_perm;
use super::{parsers, serialize};
use super::reply::StatFs;
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

pub trait TxnFetch<K, V> {
    #[allow(async_fn_in_trait)]
    async fn fetch(&self, key: &K) -> TiFsResult<Arc<V>>;
}

pub trait TxnPut<K, V> {
    #[allow(async_fn_in_trait)]
    async fn put(&self, key: &K, value: Arc<V>) -> TiFsResult<()>;
}

pub trait TxnDelete<K, V> {
    #[allow(async_fn_in_trait)]
    async fn delete(&self, key: &K) -> TiFsResult<()>;
}

#[derive(Clone)]
pub struct TxnDataCache<K, V> {
    cache: moka::future::Cache<K, (Instant, Arc<V>)>,
    time_limit: Duration,
}

impl<K, V> TxnDataCache<K, V>
where
    K: Clone + 'static,
    K: std::hash::Hash,
    K: std::cmp::Eq,
    K: std::marker::Send,
    K: std::marker::Sync,
    V: std::marker::Sync,
    V: std::marker::Send,
    V: 'static,
{
    pub fn new(capacity: u64, time_limit: Duration) -> Self {
        Self{
            cache: moka::future::Cache::new(capacity),
            time_limit,
        }
    }

    pub async fn read_uncached(&self, key: &K, txn: &impl TxnFetch<K, V>) -> Result<Arc<V>> {
        let value = txn.fetch(key).await?;
        self.cache.insert(key.clone(), (Instant::now(), value.clone())).await;
        Ok(value)
    }

    pub async fn read_cached(&self, key: &K, txn: &impl TxnFetch<K, V>) -> Result<Arc<V>> {
        if let Some((time, value)) = self.cache.get(key).await {
            if time.elapsed() < self.time_limit {
                return Ok(value);
            } else {
                self.cache.remove(key).await;
            }
        }
        self.read_uncached(key, txn).await
    }

    pub async fn write_cached(&self, key: K, value: Arc<V>, txn: &impl TxnPut<K, V>) -> Result<()> {
        self.cache.insert(key.clone(), (Instant::now(), value.clone())).await;
        Ok(txn.put(&key, value).await?)
    }

    pub async fn delete_with_cache(&self, key: &K, txn: &impl TxnDelete<K, V>) -> Result<()> {
        self.cache.remove(key).await;
        txn.delete(key).await
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

    pub async fn open(self: Arc<Self>, ino: StorageIno, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.clone().put(key, &[]).await?;
        eprintln!("open-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    pub async fn close(self: Arc<Self>, ino: StorageIno, use_id: Uuid) -> Result<()> {
        // check for existence
        let _inode = self.clone().read_inode(ino).await?;
        // de-publish opened state
        let key = Key::from(self.key_builder().opened_inode(ino, use_id));
        self.clone().delete(key).await?;
        eprintln!("close-ino: {ino}, use_id: {use_id}");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn read_hash_of_file(self: TxnArc, ino: StorageIno, offset: u64, size: u64) -> Result<Vec<u8>> {

        let hash_size = mem::size_of::<TiFsHash>() as u64;
        let offset = offset.clamp(0, hash_size);
        let remaining = hash_size - offset;
        let size = size.clamp(0, remaining);

        let mut result = Vec::with_capacity(hash_size as usize);
        let mut ino_size_data = self.caches.inode_size.read_cached(&ino, self.deref()).await?;
        if ino_size_data.data_hash.is_none() {
            let size_in_blocks = ino_size_data.size().div_ceil(self.fs_config.block_size);
            let hash_data = self.clone().read_hashes_of_file(ino, 0, (size_in_blocks * hash_size) as u32).await?;
            let full_data_hash = self.fs_config.calculate_hash(&hash_data);
            ino_size_data = self.caches.inode_size.read_cached(&ino, self.deref()).await?;
            let mut ino_data_mut = ino_size_data.deref().clone();
            ino_data_mut.data_hash = Some(full_data_hash);
            ino_size_data = Arc::new(ino_data_mut);
            self.caches.inode_size.write_cached(ino, ino_size_data.clone(), self.deref()).await?;
        }

        if let Some(precalculated_hash) = &ino_size_data.data_hash {
            result.extend_from_slice(&precalculated_hash[offset as usize..(offset+size) as usize]);
            return Ok(result);
        }

        unreachable!();
    }

    #[instrument(skip(self))]
    pub async fn read_hashes_of_file(self: TxnArc, ino: StorageIno, offset: u64, size: u32) -> Result<Vec<u8>> {
        let hash_size = mem::size_of::<TiFsHash>() as u64;
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

    pub async fn read(self: Arc<Self>, ino: StorageIno, start: u64, size: u32) -> Result<Vec<u8>> {
        self.clone().read_data(ino, start, Some(size as u64), self.fs_config.enable_atime).await
    }

    pub async fn reserve_new_ino(self: TxnArc) -> Result<StorageIno> {
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
        Ok(StorageIno(ino))
    }

    pub async fn check_if_dir_entry_exists(self: Arc<Self>, parent: StorageIno, name: &ByteString) -> Result<bool> {
        Ok(self.get_index(parent, name.clone()).await?.is_some())
    }

    pub async fn connect_inode_to_directory(self: Arc<Self>, parent: StorageIno, name: &ByteString, inode: &InoDescription) -> Result<()> {
        if parent >= ROOT_INODE {
            if self.clone().check_if_dir_entry_exists(parent, name).await? {
                return Err(FsError::FileExist {
                    file: name.to_string(),
                });
            }
            self.clone().set_index(parent, name.clone(), inode.storage_ino()).await?;

            let mut dir = self.clone().read_dir(parent).await?;
            debug!("read dir({:?})", &dir);

            dir.push(StorageDirItem {
                ino: inode.storage_ino(),
                name: name.to_string(),
                typ: inode.typ,
            });

            self.save_dir(parent, &dir).await?;
            // TODO: update attributes of directory
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
    ) -> Result<(Arc<InoDescription>, Arc<InoSize>, Arc<StorageFileAttr>)> {
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

        self.caches.inode_desc.write_cached(ino, ino_desc.clone(), self.deref()).await?;
        self.caches.inode_size.write_cached(ino, ino_size.clone(), self.deref()).await?;
        self.caches.inode_attr.write_cached(ino, ino_attr.clone(), self.deref()).await?;
        self.clone().connect_inode_to_directory(parent, &name, &ino_desc).await?;

        Ok((ino_desc, ino_size, ino_attr))
    }

    pub async fn get_index(self: Arc<Self>, parent: StorageIno, name: ByteString) -> Result<Option<StorageIno>> {
        let key = Key::from(self.key_builder().index(parent, &name));
        self.clone().get(key)
            .await
            .map_err(FsError::from)
            .and_then(|value| {
                value
                    .map(|data| Ok(Index::deserialize(&data)?.storage_ino()))
                    .transpose()
            })
    }

    pub async fn set_index(self: TxnArc, parent: StorageIno, name: ByteString, ino: StorageIno) -> Result<()> {
        let key = Key::from(self.key_builder().index(parent, &name));
        let value = Index::new(ino).serialize()?;
        Ok(self.clone().put(key, value).await?)
    }

    pub async fn remove_index(self: TxnArc, parent: StorageIno, name: ByteString) -> Result<()> {
        let key = Key::from(self.key_builder().index(parent, &name));
        Ok(self.clone().delete(key).await?)
    }

    pub async fn read_inode_uncached(self: TxnArc, ino: StorageIno) -> Result<Arc<InoDescription>> {
        self.caches.inode_desc.read_uncached(&ino, self.deref()).await
    }

    pub async fn read_inode(self: TxnArc, ino: StorageIno) -> Result<Arc<InoDescription>> {
        self.caches.inode_desc.read_cached(&ino, self.deref()).await
    }

    pub async fn read_ino_size(self: TxnArc, ino: StorageIno) -> Result<Arc<InoSize>> {
        self.caches.inode_size.read_cached(&ino, self.deref()).await
    }

    pub async fn read_ino_lock_state(self: TxnArc, ino: StorageIno) -> Result<Arc<InoLockState>> {
        self.caches.inode_lock_state.read_cached(&ino, self.deref()).await
    }

    pub async fn write_ino_lock_state(self: TxnArc, ino: StorageIno, state: InoLockState
    ) -> Result<()> {
        self.caches.inode_lock_state.write_cached(ino, Arc::new(state), self.deref()).await
    }

    pub async fn read_inode_arc(self: Arc<Self>, ino: StorageIno) -> Result<Arc<InoDescription>> {
        self.read_inode(ino).await
    }

    pub async fn save_inode(self: TxnArc, inode: &Arc<InoDescription>) -> Result<()> {
        self.caches.inode_desc.write_cached(inode.storage_ino(), inode.clone(), self.deref()).await?;
        debug!("saved inode: {:?}", inode);
        Ok(())
    }

    pub async fn remove_inode(self: TxnArc, ino: StorageIno) -> Result<()> {
        let key = Key::from(self.key_builder().inode_description(ino));
        self.clone().delete(key).await?;
        self.caches.inode_desc.delete_with_cache(&ino, self.deref()).await?;
        Ok(())
    }

    pub fn key_builder(&self) -> ScopedKeyBuilder {
        ScopedKeyBuilder::new(&self.fs_config.key_prefix)
    }

    pub fn key_parser<'fl>(&'fl self, i: &'fl mut std::slice::Iter<'fl, u8>) -> TiFsResult<KeyParser<'fl>> {
        KeyParser::start(i, &self.fs_config.key_prefix, self.fs_config.hash_len)
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

    async fn transfer_inline_data_to_block(self: TxnArc, ino: StorageIno, ino_size: &mut InoSize) -> Result<()> {
        debug_assert!(ino_size.size() <= self.inline_data_threshold());
        let key = Key::from(self.key_builder().block(BlockAddress { ino, index: 0 }));
        let data = ino_size.take_inline_data().unwrap();
        self.clone().put(key, data).await?;
        Ok(())
    }

    async fn write_inline_data(
        self: TxnArc,
        ino: StorageIno,
        ino_size: &mut InoSize,
        start: u64,
        data: &[u8],
    ) -> Result<usize> {
        debug_assert!(ino_size.size() <= self.inline_data_threshold());
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

        let mut inlined = ino_size.take_inline_data().unwrap_or_else(Vec::new);
        if start + size > inlined.len() {
            inlined.resize(start + size, 0);
        }
        inlined[start..start + size].copy_from_slice(data);

        ino_size.last_change = SystemTime::now();
        ino_size.set_size(inlined.len() as u64, self.block_size);
        ino_size.set_inline_data(inlined);
        self.caches.inode_size.write_cached(
            ino, Arc::new(ino_size.clone()), self.deref()).await?;

        Ok(size)
    }

    async fn read_inline_data(
        &self,
        inode: &InoSize,
        start: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
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

    async fn hb_read_data(self: TxnArc, ino: StorageIno, start: u64, size: u64) -> Result<Vec<u8>> {

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

    async fn read_data_traditional(self: TxnArc, ino: StorageIno, start: u64, size: u64) -> Result<Vec<u8>> {
        let target = start + size;
        let start_block = start.div_floor(self.fs_config.block_size);
        let end_block = target.div_ceil(self.fs_config.block_size);

        let block_range = self.key_builder().block_range(ino, start_block..end_block);
        let pairs = self
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
    ) -> Result<Vec<u8>> {

        if update_atime {
            let atime = AccessTime(SystemTime::now());
            self.caches.inode_atime.write_cached(
                ino, Arc::new(atime), self.deref()).await?;
        }

        let ino_size = self.caches.inode_size.read_cached(&ino, self.deref()).await?;
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

    pub async fn clear_data(self: TxnArc, ino: StorageIno) -> Result<u64> {
        let mut ino_size = self.caches.inode_size.read_cached(
            &ino, self.deref()).await?.deref().clone();
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
        self.clone().batch_mutate(deletes).await?;

        let clear_size = ino_size.size();
        ino_size.set_size(0, self.fs_config.block_size);
        ino_size.last_change = SystemTime::now();
        self.caches.inode_size.write_cached(ino, Arc::new(ino_size), self.deref()).await?;
        Ok(clear_size)
    }

    pub async fn write_blocks_traditional(self: TxnArc, ino: StorageIno, start: u64, data: &Bytes) -> Result<()> {

        let mut block_index = start / self.block_size;
        let start_key = Key::from(self.key_builder().block(BlockAddress { ino, index: block_index }));
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
            let key = Key::from(self.key_builder().block(BlockAddress { ino, index: block_index }));
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

    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range(self: TxnArc, ino: StorageIno, block_range: Range<u64>
    ) -> TiFsResult<HashMap<BlockAddress, TiFsHash>> {
        let range = self.key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.scan(
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
    ) -> Result<HashMap<BlockAddress, TiFsHash>> {
        let mut i = block_range.start;
        let max_chunk_size = 10240;
        let mut result = HashMap::with_capacity(block_range.clone().count());
        while i < block_range.end {
            let chunk_start = i;
            let chunk_end = (chunk_start+max_chunk_size).min(block_range.end);
            i = chunk_end;

            let chunk = self.clone().hb_get_block_hash_list_by_block_range(ino, chunk_start..chunk_end).await?;
            result.extend(chunk.into_iter());
        }
        trace!("result: len:{}", result.len());
        Ok(result)
    }

    pub async fn hb_get_block_data_by_hashes(self: TxnArc, hash_list: &HashSet<TiFsHash>
    ) -> Result<HashMap<TiFsHash, Arc<Vec<u8>>>> {
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
        let rcv_data_list = self.batch_get(keys).await?;
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

    pub async fn hb_filter_existent_blocks(self: Arc<Self>, mut new_blocks: HashMap<TiFsHash, Arc<Vec<u8>>>) -> Result<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        if self.fs_config.existence_check {
            if self.fs_config.validate_writes {
                let key_list = new_blocks.keys().cloned().collect::<Vec<_>>();
                for hash in key_list {
                    let key = self.key_builder().hashed_block(&hash);
                    let key_range: RangeInclusive<Key> = key.clone().into()..=key.into();
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
                    let key = Vec::from(key);
                    let mut i = key.iter();
                    let hash = self.key_parser(&mut i)
                        .and_then(|x|x.parse_key_hashed_block())
                        .map_err(|e|FsError::UnknownError(format!("failed parsing hash from response: {}", e)))?;
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
                kv_pairs.push(KvPair(me.key_builder().block_hash(addr).into(), hash.clone()));
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
        watch.sync("h");

        for (hash, new_block) in &new_blocks {
            self.caches.block.insert(hash.clone(), new_block.clone()).await;
        }
        watch.sync("ca");

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

        let mut mm = new_block_hashes.into_iter()
            .map(|(k,v)| (v,k)).collect::<MultiMap<_,_>>();

        let chunks: VecDeque<HashMap<TiFsHash, Arc<Vec<u8>>>> = make_chunks_hash_map(
            new_blocks, self.fs_config.max_chunk_size);

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

        eprintln!("hb_write_data(ino:{},start:{},len:{})-bl_len:{},bl_cnt:{},bl_idx[{}..{}[,jobs:{total_jobs}({skipped_new_block_hashes}/{input_block_hashes} skipped)", ino, start.to_formatted_string(&Locale::en), data.len().to_formatted_string(&Locale::en), bs.block_size, block_range.end - block_range.start, block_range.start, block_range.end);

        let was_modified = new_block_hashes_len > 0;
        Ok(was_modified)
    }

    pub async fn write(self: TxnArc, fh: Arc<FileHandler>, start: u64, data: Bytes) -> Result<usize> {
        let mut watch = AutoStopWatch::start("write_data");
        let ino = fh.ino();
        debug!("write data at ({})[{}]", ino, start);
        //let meta = self.read_meta().await?.unwrap(); // TODO: is this needed?
        //self.check_space_left(&meta)?;

        let size = data.len();
        let target = start + size as u64;
        let size_changed;
        {
            let write_size_lock = fh.ino_use.ino_size_lock.write().await;
            let mut ino_size = self.caches.inode_size.read_cached(
                &ino, self.deref()).await?.deref().clone();
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
                    ino, Arc::new(ino_size), self.deref()).await?;
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
            let wr_lock = fh.ino_use.ino_size_lock.write().await;
            let mut ino_size_now = self.caches.inode_size.read_cached(
                &ino, self.deref()).await?.deref().clone();
            // parallel writes might have done the size increment already
            let size_still_changed = target > ino_size_now.size();
            if size_still_changed {
                ino_size_now.set_size(target, self.fs_config.block_size);
            }
            // if no hash was available before, no need to store a change
            let hash_removed = ino_size_now.data_hash.take().is_some();
            if size_still_changed || hash_removed {
                ino_size_now.last_change = SystemTime::now();
                self.caches.inode_size.write_cached(
                    ino, Arc::new(ino_size_now), self.deref()).await?;
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

    pub async fn read_link(self: TxnArc, ino: StorageIno) -> Result<Vec<u8>> {
        let ino_size = self.caches.inode_size.read_cached(&ino, self.as_ref()).await?;
        let size = ino_size.size();

        if self.fs_config.enable_atime {
            let atime = AccessTime(SystemTime::now());
            self.caches.inode_atime.write_cached(
                ino, Arc::new(atime), self.deref()).await?;
        }

        self.read_inline_data(&ino_size, 0, size).await
    }

    pub async fn link(self: TxnArc, ino: StorageIno, new_parent: StorageIno, new_name: ByteString
    ) -> Result<Arc<InoDescription>> {
        // check and remove any inode at destination:
        if let Some(old_ino) = self.clone().get_index(new_parent, new_name.clone()).await? {
            let ino_desc = self.clone().read_inode(old_ino).await?;
            match ino_desc.typ {
                StorageDirItemKind::Directory => self.clone().rmdir(new_parent, new_name.clone()).await?,
                _ => self.clone().unlink(new_parent, new_name.clone()).await?,
            }
        }

        // attach existing ino to a new directory with a new name:
        self.clone().set_index(new_parent, new_name.clone(), ino).await?;

        let inode = self.clone().read_inode(ino).await?.deref().clone();
        let mut dir = self.clone().read_dir(new_parent).await?;

        dir.push(StorageDirItem {
            ino,
            name: new_name.to_string(),
            typ: inode.typ,
        });

        self.clone().save_dir(new_parent, &dir).await?;
        Ok(Arc::new(inode))
    }

    pub async fn unlink(self: TxnArc, parent: StorageIno, name: ByteString) -> Result<()> {
        match self.clone().get_index(parent, name.clone()).await? {
            None => Err(FsError::FileNotFound {
                file: name.to_string(),
            }),
            Some(_ino) => {
                self.clone().remove_index(parent, name.clone()).await?;
                let parent_dir = self.clone().read_dir(parent).await?;
                let new_parent_dir: StorageDirectory = parent_dir
                    .into_iter()
                    .filter(|item| item.name != *name)
                    .collect();
                self.clone().save_dir(parent, &new_parent_dir).await?;
                Ok(())
            }
        }
    }

    pub async fn rmdir(self: TxnArc, parent: StorageIno, name: ByteString) -> Result<()> {
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

    pub async fn lookup_ino(self: TxnArc, parent: StorageIno, name: ByteString) -> Result<StorageIno> {
        self.get_index(parent, name.clone())
            .await?
            .ok_or_else(|| FsError::FileNotFound {
                file: name.to_string(),
            })
    }

    pub async fn get_all_ino_data(self: TxnArc, ino: StorageIno,
    ) -> Result<(Arc<InoDescription>, Arc<StorageFileAttr>, Arc<InoSize>, AccessTime)> {
        let desc = self.caches.inode_desc.read_cached(&ino, self.as_ref()).await?;
        let attr = self.caches.inode_attr.read_cached(&ino, self.as_ref()).await?;
        let size = self.caches.inode_size.read_cached(&ino, self.as_ref()).await?;
        let atime = self.caches.inode_atime.read_cached(&ino, self.as_ref()).await?;
        Ok((desc, attr, size, atime.deref().clone()))
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
            self.caches.inode_atime.write_cached(ino, Arc::new(new_atime), self.deref()).await?;
        }

        if let Some(mtime_modification) = mtime {
            let new_mtime = ModificationTime(get_time_from_time_or_now(mtime_modification));
            self.caches.inode_mtime.write_cached(ino, Arc::new(new_mtime), self.deref()).await?;
        }

        // TODO: how to deal with fh, chgtime, bkuptime?
        let mut ino_attr = self.clone().caches.inode_attr.read_cached(
            &ino, self.as_ref()).await?.deref().clone();

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
                ino, Arc::new(ino_attr), self.as_ref()).await?;
        }

        Ok(())
    }

    pub async fn f_allocate(
        self: TxnArc,
        fh: Arc<FileHandler>,
        ino: StorageIno,
        offset: i64,
        length: i64
    ) -> Result<()> {
        let wr_lock = fh.ino_use.ino_size_lock.write();
        let mut ino_size = self.caches.inode_size.read_cached(
            &ino, self.as_ref()).await?.deref().clone();
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
        self.caches.inode_size.write_cached(ino, Arc::new(ino_size), self.as_ref()).await?;
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
    ) -> Result<(Arc<InoDescription>, Arc<InoSize>, Arc<StorageFileAttr>)> {
        let (ino_desc, ino_size, ino_attr)
            = self.clone().make_inode(parent, name, StorageDirItemKind::Directory,
                perm, gid, uid, 0, None).await?;
        self.clone().save_dir(ino_desc.ino, &StorageDirectory::new()).await?;
        /*
        self.clone().link(inode.storage_ino(), inode.storage_ino(), DIR_SELF).await?;
        if parent >= ROOT_INODE {
            self.clone().link(parent, inode.storage_ino(), DIR_PARENT).await?;
        }
        */
        Ok((ino_desc, ino_size, ino_attr))
    }

    pub async fn read_dir(self: Arc<Self>, ino: StorageIno) -> Result<StorageDirectory> {
        let key = Key::from(self.key_builder().block(BlockAddress { ino, index: 0 }));
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

    pub async fn save_dir(self: TxnArc, ino: StorageIno, dir: &[StorageDirItem]
    ) -> Result<()> {
        let data = super::dir::encode(dir)?;
        let mut inode = self.caches.inode_size.read_cached(
            &ino, self.deref()).await?.deref().clone();
        inode.set_size(data.len() as u64, self.block_size);
        inode.last_change = SystemTime::now();
        let ptr = Arc::new(inode);
        self.caches.inode_size.write_cached(ino, ptr, self.deref()).await?;
        let key = Key::from(self.key_builder().block(BlockAddress{ino, index: 0}));
        self.clone().put(key, data).await?;
        Ok(())
    }

    pub async fn statfs(self: TxnArc) -> Result<StatFs> {

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
            &ino, self.deref()).await?.deref().clone();
        match typ {
            libc::F_WRLCK => {
                if inode.owner_set.len() > 1 {
                    Ok(false)
                } else if inode.owner_set.is_empty() {
                    inode.lk_type = libc::F_WRLCK;
                    inode.owner_set.insert(lock_owner);
                    self.caches.inode_lock_state.write_cached(
                        ino, Arc::new(inode), self.deref()).await?;
                    Ok(true)
                } else if inode.owner_set.get(&lock_owner)
                    == Some(&lock_owner)
                {
                    inode.lk_type = libc::F_WRLCK;
                    self.caches.inode_lock_state.write_cached(
                        ino, Arc::new(inode), self.deref()).await?;
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
                        ino, Arc::new(inode), self.deref()).await?;
                    Ok(true)
                }
            }
            _ => Err(FsError::InvalidLock),
        }
    }
}

impl<K, V> TxnFetch<K, V> for Txn
where V: for<'dl> Deserialize<'dl>, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn fetch(&self, key: &K) -> TiFsResult<Arc<V>> {
        let me = self.weak.upgrade().ok_or(
            FsError::UnknownError(format!("upgrade weak failed")))?;
        let t = self.key_builder();
        let key_raw = t.generate_key(key);
        let result = me.get(key_raw).await?;
        let Some(data) = result else {
            return Err(FsError::KeyNotFound);
        };
        Ok(Arc::new(serialize::deserialize::<V>(&data)
            .map_err(|err|FsError::UnknownError(format!("deserialize failed: {err}")))?))
    }
}

impl<K, V> TxnPut<K, V> for Txn
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn put(&self, key: &K, value: Arc<V>) -> TiFsResult<()> {
        let me = self.weak.upgrade().ok_or(FsError::UnknownError(format!("upgrade weak failed")))?;
        let t = self.key_builder();
        let key = t.generate_key(key);
        let data = serialize::serialize(value.deref())
            .map_err(|err|FsError::UnknownError(format!("serialization failed: {err}")))?;
        me.put(key, data).await?;
        Ok(())
    }
}

impl<K, V> TxnDelete<K, V> for Txn
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn delete(&self, key: &K) -> TiFsResult<()> {
        let me = self.weak.upgrade().ok_or(FsError::UnknownError(format!("upgrade weak failed")))?;
        let t = self.key_builder();
        let key = t.generate_key(key);
        me.delete(key).await?;
        Ok(())
    }
}
