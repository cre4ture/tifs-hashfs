use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::future::Future;
use std::mem;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::anyhow;
use bimap::BiHashMap;
use bytestring::ByteString;
use fuser::{FileAttr, FileType};
use futures::FutureExt;

use moka::future::Cache;
use tikv_client::Config;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace};
use uuid::Uuid;
use lazy_static::lazy_static;

use crate::fs::inode::DirectoryItem;
use crate::fs::reply::{DirItem, InoKind};
use super::error::{FsError, Result, TiFsResult};
use super::file_handler::FileHandler;
use super::fs_config::{MountOption, TiFsConfig};
use super::inode::{AccessTime, InoDescription, InoLockState, InoSize, ModificationTime, StorageDirItemKind, StorageFileAttr, StorageIno, TiFsHash};
use super::reply::{
    Data, Directory, LogicalIno
};
use super::transaction::{Txn, TxnArc};
use super::transaction_client_mux::TransactionClientMux;
use super::utils::txn_data_cache::TxnDataCache;

pub type TiFsBlockCache = Cache<TiFsHash, Arc<Vec<u8>>>;
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
    pub inode_atime: TxnDataCache<StorageIno, AccessTime>,
    pub inode_mtime: TxnDataCache<StorageIno, ModificationTime>,
    pub inode_attr: TxnDataCache<StorageIno, StorageFileAttr>,
    pub ino_locks: Arc<RwLock<HashMap<StorageIno, Weak<RwLock<()>>>>>,
}

impl TiFsCaches {
    pub async fn get_or_create_ino_lock(&self, ino: StorageIno) -> Arc<RwLock<()>> {
        let mut locks = self.ino_locks.write().await;
        let existing_lock = locks.get(&ino).and_then(|x|x.upgrade());
        if let Some(lock) = existing_lock {
            lock
        } else {
            let lock = Arc::new(RwLock::new(()));
            locks.insert(ino, Arc::downgrade(&lock));
            lock
        }
    }
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
                ino_locks: Arc::new(RwLock::new(HashMap::new())),
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

pub struct TiFs {
    pub weak: Weak<TiFs>,
    pub instance_id: Uuid,
    pub pd_endpoints: Vec<String>,
    pub client_config: Config,
    pub client: Arc<TransactionClientMux>,
    pub raw: Arc<tikv_client::RawClient>,
    pub direct_io: bool,
    pub fs_config: TiFsConfig,
    mut_data: RwLock<TiFsMutable>,
}

pub type TiFsArc = Arc<TiFs>;

pub type BoxedFuture<'a, T> = Pin<Box<dyn 'a + Send + Future<Output = Result<T>>>>;

impl TiFs {
    pub const SCAN_LIMIT: u32 = 1 << 10;
    pub const MAX_NAME_LEN: u32 = 1 << 8;

    #[instrument]
    pub async fn construct<S>(
        pd_endpoints: Vec<S>,
        cfg: Config,
        options: Vec<MountOption>,
    ) -> anyhow::Result<TiFsArc>
    where
        S: Clone + Debug + Into<String>,
    {
        let raw_cfg = cfg.clone();
        let raw = Arc::new(tikv_client::RawClient::new_with_config(pd_endpoints.clone(), raw_cfg).await?);
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

        let fs = Arc::new_cyclic(|me| {
            TiFs {
                weak: me.clone(),
                instance_id: uuid::Uuid::new_v4(),
                client,
                raw,
                pd_endpoints: pd_endpoints.clone().into_iter().map(Into::into).collect(),
                client_config: cfg,
                direct_io: fs_config.direct_io,
                mut_data: RwLock::new(TiFsMutable::new(&fs_config)),
                fs_config,
            }
        });

        fs.clone().check_metadata().await?;

        Ok(fs)
    }

    async fn check_metadata(self: TiFsArc) -> Result<()> {
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
        let mut ra_lock = file_handler.read_ahead.write().await;
        ra_lock.wait_finish_all().await;
        ra_lock.get_results_so_far();
        mem::drop(ra_lock);

        let start = start as u64;
        let arc = self.weak.upgrade().unwrap();
        let result = arc.clone().read_transaction_arc(ino, start, size, flags, lock_owner).await?;

        if (self.fs_config.read_ahead_size > 0) && (result.data.len() == size as usize) {
            let target = start + size as u64;
            let read_ahead_size = self.fs_config.read_ahead_size;
            let mut lock = file_handler.read_ahead.write().await;
            lock.push(arc.read_transaction_arc(ino, target, read_ahead_size as u32, flags, lock_owner)
                                .map(|d|{ d.map(|_|{}) }).boxed()).await;
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
    pub async fn read_kind_hashes(
        &self,
        ino: StorageIno,
        start: u64,
        size: u32,
    ) -> Result<Data> {

        let arc = self.weak.upgrade().unwrap();
        let data = arc
            .spin_no_delay(format!("read_kind_hashes, ino:{ino}, start:{start}, size:{size}"),
            move |_, txn| txn.read_hashes_of_file(ino, start, size).boxed())
            .await?;
        Ok(Data::new(data))
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
                txn.clone().commit().await?;
                tracing::trace!(
                    "transaction committed in {} ms",
                    commit_start.elapsed().unwrap().as_millis()
                );
                Ok(v)
            }
            Err(e) => {
                txn.clone().rollback().await?;
                debug!("transaction rollbacked");
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

        let txn = Txn::begin_optimistic(
            self.instance_id,
            self.client.clone(),
            self.raw.clone(),
            &self.fs_config,
            block_cache,
            Self::MAX_NAME_LEN,
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
        self.spin(msg, None, f).await
    }

    #[tracing::instrument]
    pub fn map_storage_attr_to_fuser(&self,
        kind: InoKind,
        desc: &InoDescription,
        size: &InoSize,
        attr: &StorageFileAttr,
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

    pub async fn lookup_all_info(&self, parent: StorageIno, name: ByteString
    ) -> TiFsResult<FileAttr> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("lookup parent({}), name({})", parent, name),
            move |_, txn| {
                let name = name.clone();
                Box::pin(async move {
                    let dir_item = txn.clone().lookup_ino(parent, name.clone()).await?;
                    txn.clone().get_all_ino_data(dir_item.ino).await
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            InoKind::Regular, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    pub async fn lookup_all_info_logical(
        &self,
        parent: StorageIno,
        name: ByteString,
        kind: InoKind,
    ) -> TiFsResult<FileAttr> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("lookup parent({}), name({})", parent, name),
            move |_, txn| {
                let name = name.clone();
                Box::pin(async move {
                    let item = txn.clone().lookup_ino(parent, name.clone()).await?;
                    txn.clone().get_all_ino_data(item.ino).await
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            kind, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    pub async fn get_all_file_attributes(&self, l_ino: LogicalIno
    ) -> TiFsResult<FileAttr> {
        let (desc, attr, size, atime) =
            self.spin_no_delay(format!("get attrs ino({:?})", l_ino),
            move |_, txn| {
                Box::pin(async move {
                    txn.clone().get_all_ino_data(l_ino.storage_ino()).await
                })
            }).await?;
        let stat = self.map_storage_attr_to_fuser(
            l_ino.kind, &desc, &size, &attr, Some(atime.0));
        Ok(stat)
    }

    #[tracing::instrument]
    pub async fn read_dir1(&self, ino: StorageIno) -> Result<Directory> {
        let arc = self.weak.upgrade().unwrap();
        let dir = arc.spin_no_delay(format!("read_dir"), move |_, txn| Box::pin(txn.read_dir(ino)))
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

        trace!("read_dir1 - out: {dir_complete:?}");
        Ok(dir_complete)
    }

    pub async fn read_inode(&self, ino: StorageIno) -> Result<Arc<InoDescription>> {
        let arc = self.weak.upgrade().unwrap();
        let ino = arc
            .spin_no_delay(format!("read_inode"), move |_, txn| Box::pin(txn.read_inode_arc(ino)))
            .await?;
        Ok(ino)
    }

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

    pub fn check_file_name(name: &str) -> Result<()> {
        if name.len() <= Self::MAX_NAME_LEN as usize {
            Ok(())
        } else {
            Err(FsError::NameTooLong {
                file: name.to_string(),
            })
        }
    }

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
            let mut lock = handler.write_cache.write().await;
            lock.wait_finish_all().await;
            let results = lock.get_results_so_far();
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
        let mut write_cache = file_handler.write_cache.write().await;
        write_cache.wait_finish_all().await;
        let results = write_cache.get_results_so_far();
        for r in results {
            r?;
        }
        Ok(())
    }
}

impl Debug for TiFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TiFs"))
    }
}
