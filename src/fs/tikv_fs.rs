use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::future::Future;
use std::io::BufRead;
use std::mem;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::anyhow;
use aws_config::meta::region::RegionProviderChain;
use aws_config::Region;
use aws_sdk_s3::Client;
use bimap::BiHashMap;
use bytes::Bytes;
use bytestring::ByteString;
use fuser::{FileAttr, FileType};
use futures::FutureExt;

use moka::future::Cache;
use range_collections::range_set::RangeSetRange;
use tikv_client::Config;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;
use lazy_static::lazy_static;

use crate::fs::hash_fs_tikv_implementation::TikvBasedHashFs;
use crate::fs::inode::DirectoryItem;
use crate::fs::reply::{DirItem, InoKind};
use crate::fs::block_storage_s3::S3BasedBlockStorage;
use crate::fs::block_storage_interface::BlockStorageInterface;
use super::error::{FsError, Result, TiFsResult};
use super::file_handler::FileHandler;
use super::fs_config::{MountOption, TiFsConfig};
use super::hash_fs_interface::{BlockIndex, HashFsInterface};
use super::inode::{InoAccessTime, InoDescription, InoLockState, InoModificationTime, InoSize, InoStorageFileAttr, ParentStorageIno, StorageDirItemKind, StorageIno, TiFsData, TiFsHash};
use super::key::{OPENED_INODE_PARENT_INODE, ROOT_INODE, SNAPSHOT_PARENT_INODE};
use super::reply::{
    Data, Directory, LogicalIno
};
use super::fuse_to_hashfs::{Txn, TxnArc};
use super::transaction_client_mux::TransactionClientMux;
use super::utils::lazy_lock_map::LazyLockMap;
use super::utils::stop_watch::AutoStopWatch;
use super::utils::txn_data_cache::TxnDataCache;

pub type TiFsBlockCache = Cache<TiFsHash, TiFsData>;
pub type TiFsInodeCache = Cache<StorageIno, (Instant, Arc<InoDescription>)>;

lazy_static! {
    static ref FILE_TYPE_MAP: BiHashMap<StorageDirItemKind, FileType> = {
        let mut m = BiHashMap::new();
        m.insert(StorageDirItemKind::Directory, FileType::Directory);
        m.insert(StorageDirItemKind::File, FileType::RegularFile);
        m.insert(StorageDirItemKind::Symlink, FileType::Symlink);
        m
    };
}

fn map_storage_dir_item_kind_to_file_type(kind: StorageDirItemKind) -> FileType {
    FILE_TYPE_MAP.get_by_left(&kind).unwrap().clone()
}

pub fn map_file_type_to_storage_dir_item_kind(typ: FileType) -> Result<StorageDirItemKind> {
    FILE_TYPE_MAP.get_by_right(&typ).cloned().ok_or(FsError::WrongFileType)
}

#[derive(Debug)]
pub struct InoUse {
    pub instance: Weak<TiFs>,
    pub ino: StorageIno,
    pub use_id: Uuid,
}

impl Drop for InoUse {
    fn drop(&mut self) {
        if let Some(inst) = self.instance.upgrade() {
            let ino = self.ino;
            let use_id = self.use_id;
            tokio::task::spawn(async move {
                inst.release_inode_use(ino, use_id).await;
            });
        }
    }
}

impl InoUse {
    pub fn ino(&self) -> StorageIno {
        self.ino
    }

    pub fn get_tifs(&self) -> Option<Arc<TiFs>> {
        self.instance.upgrade()
    }
}

pub fn parse_filename(name: ByteString) -> (ByteString, InoKind) {
    if (name.len() > 1) && (name.as_bytes()[0] == b'.') {
        if name.as_bytes().ends_with(b".@hash") {
            (name.slice_ref(&name[1..name.len()-b".@hash".len()]), InoKind::Hash)
        } else if name.as_bytes().ends_with(b".@hashes") {
            (name.slice_ref(&name[1..name.len()-b".@hashes".len()]), InoKind::Hashes)
        } else {
            (name, InoKind::Regular)
        }
    } else {
        (name, InoKind::Regular)
    }
}

#[derive(Clone)] // Caches do only clone reference, not content
pub struct TiFsCaches {
    pub block: TiFsBlockCache,
    pub inode_desc: TxnDataCache<StorageIno, InoDescription>,
    pub inode_size: TxnDataCache<StorageIno, InoSize>,
    pub inode_lock_state: TxnDataCache<StorageIno, InoLockState>,
    pub inode_atime: TxnDataCache<StorageIno, InoAccessTime>,
    pub inode_mtime: TxnDataCache<StorageIno, InoModificationTime>,
    pub inode_attr: TxnDataCache<StorageIno, InoStorageFileAttr>,
    pub ino_locks: Arc<LazyLockMap<StorageIno, ()>>,
}

pub struct TiFsMutable {
    pub freed_fhs: HashSet<u64>,
    pub next_fh: u64,
    pub opened_ino: HashMap<u64, Weak<InoUse>>,
    pub file_handlers: HashMap<u64, Arc<FileHandler>>,
    pub caches: TiFsCaches,
}

impl TiFsMutable {

    fn new(fs_config: &TiFsConfig) -> Self {
        let ino_cache_size = fs_config.inode_cache_size as u64 / (fs_config.block_size / 16);
        Self {
            freed_fhs: HashSet::new(),
            next_fh: 0,
            opened_ino: HashMap::new(),
            file_handlers: HashMap::new(),
            caches: TiFsCaches{
                block: Cache::new(fs_config.hashed_blocks_cache_size as u64 / fs_config.block_size),
                inode_desc: TxnDataCache::new(ino_cache_size, Duration::from_secs(100)),
                inode_size: TxnDataCache::new(ino_cache_size, Duration::from_secs(5)),
                inode_atime: TxnDataCache::new(ino_cache_size, Duration::from_secs(5)),
                inode_mtime: TxnDataCache::new(ino_cache_size, Duration::from_secs(5)),
                inode_attr: TxnDataCache::new(ino_cache_size, Duration::from_secs(30)),
                inode_lock_state: TxnDataCache::new(ino_cache_size, Duration::from_secs(2)),
                ino_locks: Arc::new(LazyLockMap::new()),
            }
        }
    }

    pub fn get_free_fh(&mut self) -> u64 {
        let reused = self.freed_fhs.iter().next().map(u64::clone);
        if let Some(Some(reused)) = reused.map(|x| self.freed_fhs.take(&x)) {
            return reused;
        }

        let fh = self.next_fh;
        self.next_fh = self.next_fh + 1;
        fh
    }

    pub fn get_ino_use(&self, ino: u64) -> Option<Arc<InoUse>> {
        self.opened_ino.get(&ino).and_then(|x|x.upgrade())
    }

    fn release_file_handler(&mut self, fh: u64) -> Option<Arc<FileHandler>> {
        if let Some(handler) = self.file_handlers.remove(&fh) {
            self.freed_fhs.insert(fh);
            Some(handler)
        } else { None }
    }
}


const DEFAULT_TLS_CONFIG_PATH: &str = "~/.tifs/tls.toml";

fn default_tls_config_path() -> anyhow::Result<std::path::PathBuf> {
    Ok(DEFAULT_TLS_CONFIG_PATH.parse()?)
}


pub struct TiFs {
    pub weak: Weak<TiFs>,
    pub instance_id: Uuid,
    pub hash_fs: Arc<dyn HashFsInterface>,
    pub client_config: Config,
    pub direct_io: bool,
    pub fs_config: TiFsConfig,
    mut_data: RwLock<TiFsMutable>,
}

pub type TiFsArc = Arc<TiFs>;

pub type BoxedFuture<'a, T> = Pin<Box<dyn 'a + Send + Future<Output = Result<T>>>>;

impl TiFs {
    pub const SCAN_LIMIT: u32 = 1 << 10;

    pub async fn get_client_config(options: &Vec<MountOption>) -> anyhow::Result<tikv_client::Config> {
        let tls_cfg_path = options
            .iter()
            .find_map(|opt| {
                if let MountOption::Tls(path) = opt {
                    Some(path.parse().map_err(Into::into))
                } else {
                    None
                }
            })
            .unwrap_or_else(default_tls_config_path)?;

        let client_cfg: tikv_client::Config = if tokio::fs::metadata(&tls_cfg_path).await.is_ok() {
            let client_cfg_contents = tokio::fs::read_to_string(tls_cfg_path).await?;
            toml::from_str::<crate::fs::client::TlsConfig>(&client_cfg_contents)?.into()
        } else {
            Default::default()
        };

        let client_cfg = client_cfg.with_timeout(Duration::from_secs(1));
        debug!("use tikv client config: {:?}", client_cfg);
        Ok(client_cfg)
    }

    pub async fn construct_hash_fs_server(
        pd_endpoints: Vec<String>,
        options: Vec<MountOption>,
    ) -> anyhow::Result<Arc<TikvBasedHashFs>> {

        let cfg = Self::get_client_config(&options).await?;
        let client = Arc::new(TransactionClientMux::new(
            pd_endpoints.clone(), cfg.clone()).await
                .map_err(|err| anyhow!("{}", err))?);
        tracing::info!("connected to pd endpoints: {:?}", pd_endpoints);

        let fs_config = TiFsConfig::from_options(&options).map_err(|err| {
            tracing::error!("failed creating config. Err: {:?}", err);
            err
        })?;

        Ok(TikvBasedHashFs::new_arc(
            fs_config.clone(),
            client,
            None,
        ))
    }

    #[instrument]
    pub async fn construct_hash_fs_client<S>(
        pd_endpoints: Vec<S>,
        options: Vec<MountOption>,
        hash_fs_impl: Arc<dyn HashFsInterface>,
    ) -> anyhow::Result<TiFsArc>
    where
        S: Clone + Debug + Into<String>,
    {
        let cfg = Self::get_client_config(&options).await?;

        let fs_config = TiFsConfig::from_options(&options).map_err(|err| {
            tracing::error!("failed creating config. Err: {:?}", err);
            err
        })?;

        let fs = Arc::new_cyclic(|me| {
            TiFs {
                weak: me.clone(),
                instance_id: uuid::Uuid::new_v4(),
                hash_fs: hash_fs_impl,
                client_config: cfg,
                direct_io: fs_config.direct_io,
                mut_data: RwLock::new(TiFsMutable::new(&fs_config)),
                fs_config,
            }
        });

        Ok(fs)
    }

    #[instrument]
    pub async fn construct_direct_tikv<S>(
        pd_endpoints: Vec<S>,
        options: Vec<MountOption>,
    ) -> anyhow::Result<TiFsArc>
    where
        S: Clone + Debug + Into<String>,
    {
        let cfg = Self::get_client_config(&options).await?;

        let client = Arc::new(TransactionClientMux::new(
                pd_endpoints.clone().into_iter().map(|s|s.into()).collect::<Vec<_>>(), cfg.clone()
            )
            .await
            .map_err(|err| anyhow!("{}", err))?);
        info!("connected to pd endpoints: {:?}", pd_endpoints);

        let fs_config = TiFsConfig::from_options(&options).map_err(|err| {
            tracing::error!("failed creating config. Err: {:?}", err);
            err
        })?;

        let mut block_storage = None;
        if fs_config.s3_bucket != "" {
            let shared_config = if fs_config.s3_endpoint != "" {
                aws_config::from_env().endpoint_url(fs_config.s3_endpoint.clone()).load().await
            } else {
                let region_provider = RegionProviderChain::first_try(Region::new(fs_config.s3_region.clone()));
                aws_config::from_env().region(region_provider).load().await
            };
            let client = Client::new(&shared_config);
            let block_storage_impl = Arc::new(S3BasedBlockStorage::new(
                client,
                fs_config.s3_bucket.clone(),
                fs_config.s3_path_prefix.clone()),
            );
            block_storage = Some(block_storage_impl as Arc<dyn BlockStorageInterface>);
        }

        let fs = Arc::new_cyclic(|me| {
            TiFs {
                weak: me.clone(),
                instance_id: uuid::Uuid::new_v4(),
                hash_fs: TikvBasedHashFs::new_arc(
                    fs_config.clone(),
                    client,
                    block_storage,
                ),
                client_config: cfg,
                direct_io: fs_config.direct_io,
                mut_data: RwLock::new(TiFsMutable::new(&fs_config)),
                fs_config,
            }
        });

        Ok(fs)
    }

    async fn heartbeat_check_mut_data(&self) -> TiFsResult<()> {
        let mut_data = self.mut_data.try_read()?;
        let caches = mut_data.caches.clone();
        let active_file_handlers = mut_data.file_handlers.len();
        let opened_inos = mut_data.opened_ino.len();
        drop(mut_data);
        caches.ino_locks.print_statistics();
        trace!("active file handlers, inos: {}, {}", active_file_handlers, opened_inos);
        Ok(())
    }

    async fn heartbeat_clean_weak_ptrs(&self) -> TiFsResult<()> {
        let mut mut_data = self.mut_data.try_write()?;
        let caches = mut_data.caches.clone();
        mut_data.opened_ino.retain(|_k,w| {
            w.upgrade().is_some()
        });
        drop(mut_data);
        caches.ino_locks.cleanup();
        Ok(())
    }

    #[tracing::instrument]
    pub async fn heartbeat(me: Weak<TiFs>) {
        let mut timer = tokio::time::interval(Duration::from_millis(1000));
        loop {
            let Some(strong) = me.upgrade() else {
                return;
            };
            trace!("heartbeat");

            let clean_result = strong.heartbeat_clean_weak_ptrs().await;
            if let Err(err) = clean_result {
                trace!("failed cleanup: {err:?}");
            }

            let result = strong.heartbeat_check_mut_data().await;
            if let Err(err) = result {
                trace!("failed check: {err:?}");
            }

            drop(strong);
            timer.tick().await;
        }
    }

    pub async fn check_metadata(self: TiFsArc) -> Result<()> {
        let metadata = self
            .clone().spin_no_delay(format!("check_metadata"),
            move |_, txn| Box::pin(txn.read_static_meta()))
            .await?;
        if let Some(cfg_flags) = metadata {
            if cfg_flags.block_size != self.fs_config.block_size {
                panic!("stored config information mismatch: block_size desired: {}, actual: {}",
                    self.fs_config.block_size, cfg_flags.block_size);
            }
            if cfg_flags.hashed_blocks != self.fs_config.hashed_blocks {
                panic!("stored config information mismatch: hashed_blocks desired: {}, actual: {}",
                    self.fs_config.hashed_blocks, cfg_flags.hashed_blocks);
            }
            if cfg_flags.hash_algorithm != self.fs_config.hash_algorithm.to_string() {
                panic!("stored config information mismatch: hash_algorithm desired: {}, actual: {}",
                    self.fs_config.hash_algorithm.to_string(), cfg_flags.hash_algorithm);
            }
        }

        Ok(())
    }

    pub async fn with_mut_data<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut TiFsMutable) -> R,
    {
        let mut data = self.mut_data.write().await;
        Ok(f(data.deref_mut()))
    }

    async fn get_file_handler(&self, fh: u64) -> Option<Arc<FileHandler>> {
        let d = self.mut_data.read().await;
        d.file_handlers.get(&fh).map(|x|x.clone())
    }

    pub async fn get_file_handler_checked(&self, fh: u64) -> Result<Arc<FileHandler>> {
        self.get_file_handler(fh).await.ok_or(FsError::FhNotFound { fh })
    }

    pub async fn read_kind_regular(
        &self,
        ino: StorageIno,
        file_handler: Arc<FileHandler>,
        start: u64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
    ) -> Result<Data> {
        let write_ahead_lock = file_handler.write_cache.write().await;
        write_ahead_lock.wait_finish_all().await;
        let _ = write_ahead_lock.get_results_so_far();
        mem::drop(write_ahead_lock);

        let start = start as u64;
        let arc = self.weak.upgrade().unwrap();
        let result = arc.clone().read_transaction_arc(ino, start, size, flags, lock_owner).await?;

        if (self.fs_config.read_ahead_size > 0) && (result.data.len() == size as usize) {
            let target = start + size as u64;
            let read_ahead_size = self.fs_config.read_ahead_size;
            let mut lock_range_sets = file_handler.read_ahead_map.write().await;
            let remaining = lock_range_sets.get_remaining_to_be_read_size(
                target..target+read_ahead_size);
            let remaining = remaining.iter().filter_map(|x| {
                    if let RangeSetRange::Range(range) = x {
                        Some(*range.start..*range.end)
                    } else { None }}).collect::<Vec<_>>();
            let count = remaining.iter()
                .fold(0, |x, v| {
                x + (v.end - v.start)
            });
            eprintln!("read ahead remaining: {}", count);
            if count > (self.fs_config.read_ahead_size / 2) {
                let fp = remaining.iter().next().unwrap().clone();
                lock_range_sets.update_read_size(fp.clone());
                let start = fp.start;
                let size = fp.end - fp.start;
                eprintln!("read ahead step: pos: {}, size: {}", start, size);
                let lock = file_handler.read_ahead.write().await;
                lock.push(arc.read_transaction_arc(ino, start, size as u32,
                     flags, lock_owner)
                        .map(|d|{ d.map(|_|{}) }).boxed()).await;
            }
        }

        Ok(result)
    }

    pub async fn read_kind_hash(
        &self,
        ino: StorageIno,
        start: u64,
        size: u32,
    ) -> Result<Data> {

        let arc = self.weak.upgrade().unwrap();
        let data = arc
            .spin_no_delay(format!("read_kind_hash, ino:{ino}, start:{start}, size:{size}"),
            move |_, txn| txn.read_hash_of_file(ino, start, size as u64).boxed())
            .await?;
        Ok(Data::new(data))
    }

    #[tracing::instrument]
    pub async fn read_file_kind_hashes(
        &self,
        ino: StorageIno,
        start: u64,
        size: u32,
    ) -> Result<Data> {

        let hash_size = self.fs_config.hash_len as u64;
        let first_block = start / hash_size;
        let first_block_offset = start % hash_size;
        let block_cnt = (first_block_offset + size as u64).div_ceil(hash_size);
        let block_range = BlockIndex(first_block)..BlockIndex(first_block+block_cnt);

        let arc = self.weak.upgrade().unwrap();
        let mut data = VecDeque::<u8>::from(arc
            .spin_no_delay(format!("read_kind_hashes, ino:{ino}, start:{start}, size:{size}"),
            move |_, txn| txn.clone().get_hashes_of_file(ino, block_range.clone()).boxed())
            .await?);

        data.consume(first_block_offset as usize);
        data.truncate(size as usize);
        Ok(Data { data: data.into() })
    }

    #[instrument(skip(txn, f))]
    pub async fn process_txn<'b, F, T>(&self, txn: TxnArc, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        match f(self, txn.clone()).await {
            Ok(v) => {
                let commit_start = SystemTime::now();
                //txn.f_txn.commit().await?;
                tracing::trace!(
                    "transaction committed in {} ms",
                    commit_start.elapsed().unwrap().as_millis()
                );
                Ok(v)
            }
            Err(e) => {
                //txn.f_txn.rollback().await?;
                debug!("transaction rolled back");
                Err(e)
            }
        }
    }

    async fn get_caches(&self) -> TiFsCaches {
        self.mut_data.read().await.caches.clone()
    }

    async fn with_optimistic<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        let block_cache = self.get_caches().await;

        let txn = Txn::begin_on_hash_fs_interface(
            self.hash_fs.clone(),
            self.instance_id,
            &self.fs_config,
            block_cache,
        )
        .await?;
        self.process_txn(txn, f).await
    }

    pub async fn spin<F, T>(self: TiFsArc, msg: String, delay: Option<Duration>, mut f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        trace!("spin-start: {msg}");

        let mut other_error_count = 0;
        loop {
            match self.with_optimistic(&mut f).await {
                Ok(v) => break Ok(v),
                Err(FsError::KeyError(_err)) => {
                    eprintln!("{msg}: spin because of a key error({}, {:?})", _err, _err);
                    if let Some(time) = delay {
                        sleep(time).await;
                    }
                }
                Err(FsError::UnknownError(err)) => {
                    if other_error_count >= 3 {
                        break Err(FsError::UnknownError(err));
                    }
                    other_error_count = other_error_count + 1;
                    eprintln!("{msg}: spin because of a unknown error({}, {:?})", err, err);
                    sleep(Duration::from_millis(100)).await;
                }
                Err(err @ FsError::FileNotFound { file: _ }) => return Err(err),
                Err(err @ FsError::KeyNotFound(_)) => return Err(err),
                Err(err @ FsError::GrpcMessageIncomplete) => return Err(err),
                Err(err) => {
                    eprintln!("{msg}: no spin, error({}, {:?})", err, err);
                    return Err(err);
                }
            }
        }
    }

    pub async fn spin_no_delay<F, T>(&self, msg: String, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        let arc = self.weak.upgrade().unwrap();
        arc.spin_no_delay_arc(msg, f).await
    }

    pub async fn spin_no_delay_arc<F, T>(self: TiFsArc, msg: String, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        let _watch = AutoStopWatch::start("spin_no_delay_arc");
        self.spin(msg, None, f).await
    }

    #[tracing::instrument]
    pub fn map_storage_attr_to_fuser(&self,
        kind: InoKind,
        desc: &InoDescription,
        size: &InoSize,
        attr: &InoStorageFileAttr,
        atime: Option<SystemTime>,
    ) -> FileAttr {
        let stat = FileAttr {
            ino: LogicalIno {
                kind,
                storage_ino: desc.storage_ino(),
            }.to_raw(),
            size: match kind {
                InoKind::Regular => size.size(),
                InoKind::Hash => self.fs_config.hash_len as u64,
                InoKind::Hashes => self.fs_config.hash_len as u64 * size.blocks(),
            },
            blocks: match kind {
                InoKind::Regular => size.blocks(),
                InoKind::Hash => 0,
                InoKind::Hashes => (self.fs_config.hash_len as u64 * size.blocks())
                                    .div_ceil(self.fs_config.block_size),
            },
            atime: atime.unwrap_or(size.last_change),
            mtime: size.last_change.max(attr.last_change),
            ctime: attr.last_change,
            crtime: desc.creation_time,
            kind: map_storage_dir_item_kind_to_file_type(desc.typ),
            perm: attr.perm.0,
            nlink: 1,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            blksize: self.fs_config.block_size as u32,
            flags: attr.flags
        };
        stat
    }

    pub async fn lookup_all_info(&self, parent: ParentStorageIno, name: ByteString
    ) -> TiFsResult<FileAttr> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("lookup parent({}), name({})", parent, name),
            move |_, txn| {
                let name = name.clone();
                Box::pin(async move {
                    Ok(txn.hash_fs.directory_child_get_all_attributes(parent, name).await?)
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            InoKind::Regular, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    pub async fn lookup_all_info_logical(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        kind: InoKind,
    ) -> TiFsResult<FileAttr> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("lookup parent({}), name({})", parent, name),
            move |_, txn| {
                let name = name.clone();
                Box::pin(async move {
                    Ok(txn.hash_fs.directory_child_get_all_attributes(parent, name).await?)
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            kind, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    pub async fn get_all_file_attributes_storage_ino(
        &self,
        ino: StorageIno,
        kind: InoKind,
    ) -> TiFsResult<FileAttr> {

        let ino = match ParentStorageIno(ino) {
            OPENED_INODE_PARENT_INODE => ROOT_INODE.0,
            SNAPSHOT_PARENT_INODE => ROOT_INODE.0,
            other => other.0,
        };

        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("get attrs ino({:?})", ino),
            move |_, txn| {
                Box::pin(async move {
                    txn.clone().get_all_ino_data(ino).await
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            kind, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    pub async fn get_all_file_attributes_storage_ino_entry_reply(
        &self,
        ino: StorageIno,
        kind: InoKind,
    ) -> TiFsResult<super::reply::Entry> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("get attrs ino({:?})", ino),
            move |_, txn| {
                Box::pin(async move {
                    txn.clone().get_all_ino_data(ino).await
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            kind, &desc, &size, &attr, Some(atime.0));
        Ok(super::reply::Entry::new(stat, 0))
    }

    pub async fn get_all_file_attributes(&self, l_ino: LogicalIno
    ) -> TiFsResult<FileAttr> {
        self.get_all_file_attributes_storage_ino(l_ino.storage_ino(), l_ino.kind).await
    }

    #[tracing::instrument]
    pub async fn read_dir1(&self, dir_ino: StorageIno) -> Result<Directory> {
        let arc = self.weak.upgrade().unwrap();
        let dir = arc.spin_no_delay(format!("read_dir"),
            move |_, txn| Box::pin(txn.read_dir(dir_ino)))
            .await?;

        let mut dir_complete = Vec::with_capacity(dir.len() * 3);
        for DirectoryItem{ino, name, typ} in dir.into_iter() {
            if typ == StorageDirItemKind::File {
                let full_hash_entry = DirItem {
                    ino: LogicalIno{
                        storage_ino: ino,
                        kind: InoKind::Hash,
                    },
                    name: format!(".{}.@hash", &name),
                    typ: FileType::RegularFile,
                };
                let block_hashes_entry = DirItem {
                    ino: LogicalIno{
                        storage_ino: ino,
                        kind: InoKind::Hashes,
                    },
                    name: format!(".{}.@hashes", &name),
                    typ: FileType::RegularFile,
                };
                dir_complete.push(full_hash_entry);
                dir_complete.push(block_hashes_entry);
            }
            let regular_entry = DirItem {
                ino: LogicalIno{
                    storage_ino: ino,
                    kind: InoKind::Regular,
                },
                name,
                typ: map_storage_dir_item_kind_to_file_type(typ),
            };
            dir_complete.push(regular_entry);
        }

        if dir_ino == crate::fs::key::ROOT_INODE.0 {
            dir_complete.push(DirItem {
                ino: LogicalIno {
                    storage_ino: crate::fs::key::OPENED_INODE_PARENT_INODE.0,
                    kind: InoKind::Regular,
                },
                name: format!("{}", crate::fs::key::OPENED_INODE_PARENT_INODE_NAME),
                typ: FileType::Directory,
            });
            dir_complete.push(DirItem {
                ino: LogicalIno {
                    storage_ino: crate::fs::key::SNAPSHOT_PARENT_INODE.0,
                    kind: InoKind::Regular,
                },
                name: format!("{}", crate::fs::key::SNAPSHOT_PARENT_INODE_NAME),
                typ: FileType::Directory,
            });
        }

        trace!("read_dir1 - out: {dir_complete:?}");
        Ok(dir_complete)
    }
/*
    pub async fn setlkw(
        &self,
        ino: StorageIno,
        lock_owner: u64,
        #[cfg(target_os = "linux")] typ: i32,
        #[cfg(any(target_os = "freebsd", target_os = "macos"))] typ: i16,
    ) -> Result<()> {
        let arc = self.weak.upgrade().unwrap();
        while !arc.clone()
            .spin_no_delay(format!("setlkw"), move |_, txn| {
                Box::pin(async move {
                    txn.setlkw(ino, lock_owner, typ).await
                })
            })
            .await?
        {}
        Ok(())
    }
*/

    async fn release_inode_use(&self, ino: StorageIno, use_id: Uuid) {
        let arc = self.weak.upgrade().unwrap();
        let result = arc.spin_no_delay(format!("release, ino:{ino}"),
        move |_, txn| Box::pin(txn.close(ino, use_id)))
        .await;
        if let Err(e) = result {
            tracing::error!("failed to release inode use ino: {ino}, use_id:{use_id}, err: {:?}", e);
        }
    }

    #[tracing::instrument]
    async fn read_transaction(
        &self,
        ino: StorageIno,
        offset: u64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
    ) -> Result<Data> {
        let arc = self.weak.upgrade().unwrap();
        arc.read_transaction_arc(ino, offset, size, flags, lock_owner).await
    }

    async fn read_transaction_arc(
        self: Arc<Self>,
        ino: StorageIno,
        start: u64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Data> {
        let arc = self.weak.upgrade().unwrap();
        let data = arc
            .spin_no_delay(format!("read_txn, ino:{ino}, start:{start}, size:{size}"),
            move |_, txn| txn.read(ino, start, size).boxed())
            .await?;
        Ok(Data::new(data))
    }

    #[instrument(skip(self))]
    pub async fn release_file_handler<'fl>(&self, fh: u64) -> Result<()>{
        let released_handler = self.with_mut_data(|d| {
            d.release_file_handler(fh)
        }).await?;

        if let Some(handler) = released_handler {
            let lock = handler.write_cache.write().await;
            lock.wait_finish_all().await;
            let results = lock.get_results_so_far().await;
            trace!("got results of {} pending tasks", results.len());
            for result in &results {
                if let Err(err) = result {
                    error!("error finishing cached writes: {err}");
                }
            }
        }

        Ok(())
    }

    pub async fn flush_write_cache(&self, fh: u64) -> TiFsResult<()> {
        let file_handler = self.get_file_handler_checked(fh).await?;
        let write_cache = file_handler.write_cache.write().await;
        write_cache.wait_finish_all().await;
        let results = write_cache.get_results_so_far().await;
        for r in results {
            r?;
        }
        let start = 0;
        let size = 0;
        let ino = file_handler.ino();
        let fh_clone = file_handler.clone();
        self.spin_no_delay(format!("write, ino:{ino}, fh:{fh}, offset:{start}, data.len:{size} - flush"),
                move |_me, txn| txn.write(fh_clone.clone(), start as u64, Bytes::new(), true).boxed()).await?;
        Ok(())
    }
}

impl Debug for TiFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TiFs"))
    }
}
