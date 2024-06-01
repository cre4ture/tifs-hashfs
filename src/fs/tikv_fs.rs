use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::future::Future;
use std::mem::{self, size_of};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::anyhow;
use async_trait::async_trait;
use bimap::BiHashMap;
use bytes::Bytes;
use bytestring::ByteString;
use fuser::consts::FOPEN_DIRECT_IO;
use fuser::{FileAttr, FileType, KernelConfig, TimeOrNow};
use futures::FutureExt;
use libc::{F_RDLCK, F_UNLCK, F_WRLCK, SEEK_CUR, SEEK_END, SEEK_SET};
use moka::future::Cache;
use tikv_client::Config;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;
use lazy_static::lazy_static;

use crate::fs::reply::{DirItem, InoKind};
use super::async_fs::AsyncFileSystem;
use super::error::{FsError, Result};
use super::file_handler::FileHandler;
use super::fs_config::{MountOption, TiFsConfig};
use super::inode::{Inode, StorageDirItem, StorageDirItemKind, StorageFilePermission, StorageIno, TiFsHash};
use super::key::ROOT_INODE;
use super::mode::{as_file_kind, as_file_perm};
use super::reply::{
    get_time, Attr, Create, Data, Dir, Directory, Entry, LogicalIno, Lock, Lseek, Open, StatFs, Write, Xattr
};
use super::transaction::{Txn, TxnArc};
use super::transaction_client_mux::TransactionClientMux;

pub const DIR_SELF: ByteString = ByteString::from_static(".");
pub const DIR_PARENT: ByteString = ByteString::from_static("..");

pub type TiFsBlockCache = Cache<TiFsHash, Arc<Vec<u8>>>;
pub type TiFsInodeCache = Cache<StorageIno, (Instant, Arc<Inode>)>;

lazy_static! {
    static ref FILE_TYPE_MAP: BiHashMap<StorageDirItemKind, FileType> = {
        let mut m = BiHashMap::new();
        m.insert(StorageDirItemKind::Directory, FileType::Directory);
        m.insert(StorageDirItemKind::File, FileType::RegularFile);
        m.insert(StorageDirItemKind::Symlink, FileType::Symlink);
        m
    };
}

#[derive(Clone, Debug)]
pub struct InoUse {
    instance: Weak<TiFs>,
    ino: StorageIno,
    use_id: Uuid,
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
    pub inode: TiFsInodeCache,
}

struct TiFsMutable {
    pub freed_fhs: HashSet<u64>,
    pub next_fh: u64,
    pub opened_ino: HashMap<u64, Weak<InoUse>>,
    pub file_handlers: HashMap<u64, Arc<FileHandler>>,
    pub caches: TiFsCaches,
}

impl TiFsMutable {

    fn new(fs_config: &TiFsConfig) -> Self {
        Self {
            freed_fhs: HashSet::new(),
            next_fh: 0,
            opened_ino: HashMap::new(),
            file_handlers: HashMap::new(),
            caches: TiFsCaches{
                block: Cache::new(fs_config.hashed_blocks_cache_size as u64 / fs_config.block_size),
                inode: Cache::new(fs_config.inode_cache_size as u64 / (fs_config.block_size / 16)),
            }
        }
    }

    fn get_free_fh(&mut self) -> u64 {
        let reused = self.freed_fhs.iter().next().map(u64::clone);
        if let Some(Some(reused)) = reused.map(|x| self.freed_fhs.take(&x)) {
            return reused;
        }

        let fh = self.next_fh;
        self.next_fh = self.next_fh + 1;
        fh
    }

    fn get_ino_use(&self, ino: u64) -> Option<Arc<InoUse>> {
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
    pub client: TransactionClientMux,
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
        let client = TransactionClientMux::new(pd_endpoints.clone().into_iter().map(|s|s.into()).collect::<Vec<_>>(), cfg.clone())
            .await
            .map_err(|err| anyhow!("{}", err))?;
        info!("connected to pd endpoints: {:?}", pd_endpoints);

        let fs_config = TiFsConfig::from_options(&options);

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
            move |_, txn| Box::pin(txn.read_meta()))
            .await?;
        if let Some(meta) = metadata {
            let cfg_flags = meta.config_flags.unwrap_or_default();
            if cfg_flags.hashed_blocks != self.fs_config.hashed_blocks {
                panic!("stored config information mismatch: hashed_blocks desired: {}, actual: {}", self.fs_config.hashed_blocks, cfg_flags.hashed_blocks);
            }
        }

        Ok(())
    }

    async fn with_mut_data<F, R>(&self, f: F) -> Result<R>
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

    async fn get_file_handler_checked(&self, fh: u64) -> Result<Arc<FileHandler>> {
        self.get_file_handler(fh).await.ok_or(FsError::FhNotFound { fh })
    }

    async fn read_kind_regular(
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

    async fn read_kind_hash(
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
    async fn read_kind_hashes(
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
    async fn process_txn<'b, F, T>(&self, txn: TxnArc, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        match f(self, txn.clone()).await {
            Ok(v) => {
                let commit_start = SystemTime::now();
                txn.clone().commit().await?;
                eprintln!(
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
            &self.client,
            self.raw.clone(),
            &self.fs_config,
            block_cache,
            Self::MAX_NAME_LEN,
        )
        .await?;
        self.process_txn(txn, f).await
    }

    async fn spin<F, T>(self: TiFsArc, msg: String, delay: Option<Duration>, mut f: F) -> Result<T>
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

    async fn spin_no_delay<F, T>(&self, msg: String, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        let arc = self.weak.upgrade().unwrap();
        arc.spin_no_delay_arc(msg, f).await
    }

    async fn spin_no_delay_arc<F, T>(self: TiFsArc, msg: String, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, TxnArc) -> BoxedFuture<'a, T>,
    {
        self.spin(msg, None, f).await
    }

    fn map_storage_dir_item_kind_to_file_type(kind: StorageDirItemKind) -> FileType {
        FILE_TYPE_MAP.get_by_left(&kind).unwrap().clone()
    }

    fn map_file_type_to_storage_dir_item_kind(typ: FileType) -> Result<StorageDirItemKind> {
        FILE_TYPE_MAP.get_by_right(&typ).cloned().ok_or(FsError::WrongFileType)
    }

    fn map_storage_attr_to_fuser(&self, ino_kind: InoKind, ino_data: &Inode) -> FileAttr {
        let mut stat = FileAttr {
            ino: LogicalIno {
                kind: ino_kind,
                storage_ino: ino_data.storage_ino(),
            }.to_raw(),
            size: ino_data.size(),
            blocks: ino_data.blocks(),
            atime: ino_data.attr.atime,
            mtime: ino_data.attr.mtime,
            ctime: ino_data.attr.ctime,
            crtime: ino_data.attr.crtime,
            kind: Self::map_storage_dir_item_kind_to_file_type(ino_data.typ),
            perm: ino_data.attr.perm.0,
            nlink: 1,
            uid: ino_data.attr.uid,
            gid: ino_data.attr.gid,
            rdev: ino_data.attr.rdev,
            blksize: self.fs_config.block_size as u32,
            flags: ino_data.attr.flags
        };
        self.adapt_inode(&mut stat);
        stat
    }

    #[tracing::instrument]
    async fn read_dir1(&self, ino: StorageIno) -> Result<Directory> {
        let arc = self.weak.upgrade().unwrap();
        let dir = arc.spin_no_delay(format!("read_dir"), move |_, txn| Box::pin(txn.read_dir(ino)))
            .await?;

        let mut dir_complete = Vec::with_capacity(dir.len() * 3);
        for StorageDirItem{ino, name, typ} in dir.into_iter() {
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
                typ: Self::map_storage_dir_item_kind_to_file_type(typ),
            };
            dir_complete.push(regular_entry);
        }

        Ok(dir_complete)
    }

    async fn read_inode(&self, ino: StorageIno) -> Result<Arc<Inode>> {
        let arc = self.weak.upgrade().unwrap();
        let ino = arc
            .spin_no_delay(format!("read_inode"), move |_, txn| Box::pin(txn.read_inode_arc(ino)))
            .await?;
        Ok(ino)
    }

    async fn setlkw(
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
                    let mut inode = txn.clone().read_inode(ino).await?.deref().clone();
                    match typ {
                        F_WRLCK => {
                            if inode.lock_state.owner_set.len() > 1 {
                                Ok(false)
                            } else if inode.lock_state.owner_set.is_empty() {
                                inode.lock_state.lk_type = F_WRLCK;
                                inode.lock_state.owner_set.insert(lock_owner);
                                txn.clone().save_inode(&Arc::new(inode)).await?;
                                Ok(true)
                            } else if inode.lock_state.owner_set.get(&lock_owner)
                                == Some(&lock_owner)
                            {
                                inode.lock_state.lk_type = F_WRLCK;
                                txn.clone().save_inode(&Arc::new(inode)).await?;
                                Ok(true)
                            } else {
                                Err(FsError::InvalidLock)
                            }
                        }
                        F_RDLCK => {
                            if inode.lock_state.lk_type == F_WRLCK {
                                Ok(false)
                            } else {
                                inode.lock_state.lk_type = F_RDLCK;
                                inode.lock_state.owner_set.insert(lock_owner);
                                txn.save_inode(&Arc::new(inode)).await?;
                                Ok(true)
                            }
                        }
                        _ => Err(FsError::InvalidLock),
                    }
                })
            })
            .await?
        {}
        Ok(())
    }

    fn check_file_name(name: &str) -> Result<()> {
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

    #[instrument(skip(self, _txn))]
    pub async fn release_file_handler<'fl>(&self, _txn: Arc<Txn>, fh: u64) -> Result<()>{
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

    pub fn adapt_inode(&self, stat: &mut FileAttr) {
        match LogicalIno::from_raw(stat.ino).kind {
            InoKind::Regular => {},
            InoKind::Hash => {
                stat.size = size_of::<TiFsHash>() as u64;
                stat.blocks = 0;
            }
            InoKind::Hashes => {
                stat.size = size_of::<TiFsHash>() as u64 * stat.blocks;
                stat.blocks = 0;
            }
        }
    }
}

impl Debug for TiFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TiFs"))
    }
}

#[async_trait]
impl AsyncFileSystem for TiFs {
    #[tracing::instrument]
    async fn init(&self, gid: u32, uid: u32, config: &mut KernelConfig) -> Result<()> {
        let arc = self.weak.upgrade().unwrap();
        // config
        //     .add_capabilities(fuser::consts::FUSE_POSIX_LOCKS)
        //     .expect("kernel config failed to add cap_fuse FUSE_POSIX_LOCKS");
        #[cfg(not(target_os = "macos"))]
        config
            .add_capabilities(fuser::consts::FUSE_FLOCK_LOCKS)
            .expect("kernel config failed to add cap_fuse FUSE_CAP_FLOCK_LOCKS");

        arc.spin_no_delay(format!("init"), move |fs, txn| {
            Box::pin(async move {
                info!("initializing tifs on {:?} ...", &fs.pd_endpoints);
                if let Some(meta) = txn.clone().read_meta().await? {
                    if meta.block_size != txn.block_size() {
                        let err = FsError::block_size_conflict(meta.block_size, txn.block_size());
                        error!("{}", err);
                        return Err(err);
                    }
                }

                let root_inode = txn.clone().read_inode(ROOT_INODE).await;
                if let Err(FsError::InodeNotFound { inode: _ }) = root_inode {
                    let attr = txn
                        .mkdir(
                            StorageIno(0),
                            Default::default(),
                            StorageFilePermission(0o777),
                            gid,
                            uid,
                        )
                        .await?;
                    debug!("make root directory {:?}", &attr);
                    Ok(())
                } else {
                    root_inode.map(|_| ())
                }
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn lookup(&self, parent: u64, name: ByteString) -> Result<Entry> {
        let p_ino = LogicalIno::from_raw(parent);

        let (filename, kind) = parse_filename(name);

        Self::check_file_name(&filename)?;
        let ino_data = self.spin_no_delay(format!("lookup, parent: {parent}, name: {}", filename.escape_debug()), move |_, txn| {
            let filename = filename.clone();
            Box::pin(async move {
                let ino = txn.clone().lookup(p_ino.storage_ino(), filename).await?;
                Ok(txn.read_inode(ino).await?.deref().clone())
            })
        })
        .await?;

        let entry = Entry {
            generation: 0,
            time: Duration::from_millis(0),
            stat: self.map_storage_attr_to_fuser(kind, &ino_data),
        };

        Ok(entry)
    }

    #[tracing::instrument]
    async fn getattr(&self, ino: u64) -> Result<Attr> {
        let l_ino = LogicalIno::from_raw(ino);
        let ino_data = self.read_inode(l_ino.storage_ino()).await?;
        let stat = self.map_storage_attr_to_fuser(l_ino.kind, &ino_data);
        Ok(Attr::new(stat))
    }

    #[tracing::instrument]
    async fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<Attr> {
        let l_ino = LogicalIno::from_raw(ino);
        let ino_data = self.spin_no_delay(format!("setattr"), move |_, txn| {
            Box::pin(async move {
                // TODO: how to deal with fh, chgtime, bkuptime?
                let mut inode_data = txn.clone().read_inode(l_ino.storage_ino()).await?.deref().clone();
                inode_data.attr.perm = match mode {
                    Some(m) => StorageFilePermission(as_file_perm(m)),
                    None => inode_data.attr.perm,
                };
                inode_data.attr.uid = uid.unwrap_or(inode_data.attr.uid);
                inode_data.attr.gid = gid.unwrap_or(inode_data.attr.gid);
                inode_data.set_size(size.unwrap_or(inode_data.size()), txn.block_size());
                inode_data.attr.atime = match atime {
                    None => inode_data.attr.atime,
                    Some(TimeOrNow::SpecificTime(t)) => t,
                    Some(TimeOrNow::Now) => SystemTime::now(),
                };
                inode_data.attr.mtime = match mtime {
                    Some(TimeOrNow::SpecificTime(t)) => t,
                    Some(TimeOrNow::Now) | None => SystemTime::now(),
                };
                inode_data.attr.ctime = ctime.unwrap_or_else(SystemTime::now);
                inode_data.attr.crtime = crtime.unwrap_or(inode_data.attr.crtime);
                inode_data.attr.flags = flags.unwrap_or(inode_data.attr.flags);
                txn.save_inode(&Arc::new(inode_data.clone())).await?;
                Ok(inode_data)
            })
        })
        .await?;

        Ok(Attr {
            time: get_time(),
            attr: self.map_storage_attr_to_fuser(l_ino.kind, &ino_data),
        })
    }

    #[tracing::instrument]
    async fn readdir(&self, ino: u64, _fh: u64, offset: i64) -> Result<Dir> {
        let l_ino = LogicalIno::from_raw(ino);
        let mut dir = Dir::offset(offset as usize);
        let directory = self.read_dir1(l_ino.storage_ino()).await?;
        for item in directory.into_iter().skip(offset as usize) {
            dir.push(item)
        }
        debug!("read directory {:?}", &dir);
        Ok(dir)
    }

    #[tracing::instrument]
    async fn open(&self, ino: u64, flags: i32) -> Result<Open> {
        // TODO: deal with flags
        let l_ino = LogicalIno::from_raw(ino);
        let mut ino_use = self.with_mut_data(|d| d.get_ino_use(ino)).await?;
        if ino_use.is_none() {
            // not opened yet on this instance. open it:
            let new_use_id = Uuid::new_v4();
            self
                .spin_no_delay(format!("open ino: {ino}, flags: {flags}"),
                move |_, txn| Box::pin(txn.open(l_ino.storage_ino(), new_use_id)))
                .await?;
            // file exists and registration was successful, register use locally:
            let new_ino_use = Arc::new(InoUse{
                instance: self.weak.to_owned(),
                ino: l_ino.storage_ino(),
                use_id: new_use_id
            });
            self.with_mut_data(|d: &mut TiFsMutable| d.opened_ino.insert(ino, Arc::downgrade(&new_ino_use))).await?;
            ino_use = Some(new_ino_use);
        }

        let Some(ino_use) = ino_use else {
            return Err(FsError::UnknownError("failed to get ino_use!".into()));
        };

        let file_handler = FileHandler::new(ino_use, self.fs_config.write_in_progress_limit);
        let fh = self.with_mut_data(|d| {
            let new_fh = d.get_free_fh();
            d.file_handlers.insert(new_fh, Arc::new(file_handler));
            new_fh
        }).await?;

        let mut open_flags = 0;
        #[cfg(target_os = "linux")]
        if self.direct_io || flags & libc::O_DIRECT != 0 {
            open_flags |= FOPEN_DIRECT_IO;
        }
        #[cfg(not(target_os = "linux"))]
        if self.direct_io {
            open_flags |= FOPEN_DIRECT_IO;
        }
        Ok(Open::new(fh, open_flags))
    }



    #[tracing::instrument]
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
    ) -> Result<Data> {
        let l_ino = LogicalIno::from_raw(ino);
        let file_handler = self.get_file_handler_checked(fh).await?;
        let lock = file_handler.mut_data.read().await;
        let start = lock.cursor.get(&l_ino.kind).cloned().unwrap_or(0) as i64 + offset;
        if start < 0 {
            return Err(FsError::InvalidOffset { ino, offset: start });
        }
        mem::drop(lock);

        let data = match l_ino.kind {
            InoKind::Regular => self.read_kind_regular(l_ino.storage_ino(), file_handler, start as u64, size, flags, lock_owner).await,
            InoKind::Hash => self.read_kind_hash(l_ino.storage_ino(), start as u64, size).await,
            InoKind::Hashes => self.read_kind_hashes(l_ino.storage_ino(), start as u64, size).await,
        }?;

        trace!("read() result len: {}", data.data.len());

        Ok(data)
    }

    #[tracing::instrument(skip(data))]
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Write> {
        let l_ino = LogicalIno::from_raw(ino);
        let data: Bytes = data.into();
        let file_handler = self.get_file_handler_checked(fh).await?;
        let file_handler_data = file_handler.mut_data.read().await;
        let start = file_handler_data.cursor.get(&l_ino.kind).cloned().unwrap_or(0) as i64 + offset;
        let size = data.len();
        drop(file_handler_data);
        if start < 0 {
            return Err(FsError::InvalidOffset { ino, offset: start });
        }

        let mut write_cache = file_handler.write_cache.write().await;
        let fh_clone = file_handler.clone();
        let arc = self.weak.upgrade().unwrap();

        let fut =
            arc.clone().spin_no_delay_arc(format!("write, ino:{ino}, fh:{fh}, offset:{offset}, data.len:{}", size),
                move |_me, txn| Box::pin(txn.write(fh_clone.clone(), start as u64, data.clone())));

        write_cache.push(fut.boxed()).await;
        let results = write_cache.get_results_so_far();
        for r in results {
            r?;
        }

        Ok(Write::new(size as u32))
    }

    /// Create a directory.
    #[tracing::instrument]
    async fn mkdir(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
    ) -> Result<Entry> {
        let p_ino = LogicalIno::from_raw(parent);
        Self::check_file_name(&name)?;
        let perm = StorageFilePermission(as_file_perm(mode));
        let ino_data = self
            .spin_no_delay(format!("mkdir"), move |_, txn| Box::pin(txn.mkdir(p_ino.storage_ino(), name.clone(), perm, gid, uid)))
            .await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(InoKind::Regular, &ino_data), 0))
    }

    #[tracing::instrument]
    async fn rmdir(&self, parent: u64, raw_name: ByteString) -> Result<()> {
        let p_ino = LogicalIno::from_raw(parent);
        Self::check_file_name(&raw_name)?;
        self.spin_no_delay(format!("rmdir"), move |_, txn| Box::pin(txn.rmdir(p_ino.storage_ino(), raw_name.clone())))
            .await
    }

    #[tracing::instrument]
    async fn mknod(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
        rdev: u32,
    ) -> Result<Entry> {
        let p_ino = LogicalIno::from_raw(parent);
        let perm = StorageFilePermission(as_file_perm(mode));
        let typ = Self::map_file_type_to_storage_dir_item_kind(as_file_kind(mode))?;
        Self::check_file_name(&name)?;
        let ino_data = self
            .spin_no_delay(format!("mknod"), move |_, txn| {
                Box::pin(txn.make_inode(p_ino.storage_ino(), name.clone(), typ, perm, gid, uid, rdev))
            })
            .await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(InoKind::Regular, &ino_data), 0))
    }

    #[tracing::instrument]
    async fn access(&self, _ino: u64, _mask: i32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        uid: u32,
        gid: u32,
        parent: u64,
        name: ByteString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<Create> {
        Self::check_file_name(&name)?;
        let entry = self.mknod(parent, name, mode, gid, uid, umask, 0).await?;
        let open = self.open(entry.stat.ino, flags).await?;
        Ok(Create::new(
            entry.stat,
            entry.generation,
            open.fh,
            open.flags,
        ))
    }

    async fn lseek(&self, ino: u64, fh: u64, offset: i64, whence: i32) -> Result<Lseek> {
        let l_ino = LogicalIno::from_raw(ino);
        eprintln!("lseek-begin(ino:{ino},fh:{fh},offset:{offset},whence:{whence}");
        let file_handler = self.get_file_handler_checked(fh).await?;
        let file_handler_data = file_handler.mut_data.read().await;
        let current_cursor = file_handler_data.cursor.get(&l_ino.kind).cloned().unwrap_or(0);
        let result = self.spin_no_delay(format!("lseek"), move |_, txn| {
            Box::pin(async move {

                let target_cursor = match whence {
                    SEEK_SET => offset,
                    SEEK_CUR => current_cursor as i64 + offset,
                    SEEK_END => {
                        let inode = txn.read_inode(l_ino.storage_ino()).await?;
                        inode.size() as i64 + offset
                    }
                    _ => return Err(FsError::UnknownWhence { whence }),
                };

                if target_cursor < 0 {
                    return Err(FsError::InvalidOffset {
                        ino,
                        offset: target_cursor,
                    });
                }
                Ok(Lseek::new(target_cursor))
            })
        })
        .await?;

        self.with_mut_data(|d| -> Result<()> {
            let file_handler = d.file_handlers.get_mut(&fh).ok_or(FsError::FhNotFound { fh: fh })?;
            file_handler.mut_data.blocking_write().cursor.insert(l_ino.kind, result.offset as u64);
            Ok(())
        }).await??;

        eprintln!("lseek-ok(ino:{ino},fh:{fh},offset:{offset},whence:{whence},current_cursor:{current_cursor},new_cursor:{})", result.offset);

        Ok(result)
    }

    async fn release(
        &self,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
    ) -> Result<()> {
        self.spin_no_delay(format!("release fh {fh}"),
            move |me, txn| {
                Box::pin(me.release_file_handler(txn, fh))
            })
            .await?;
        Ok(())
    }

    /// Create a hard link.
    async fn link(&self, ino: u64, new_parent: u64, new_name: ByteString) -> Result<Entry> {
        let l_ino = LogicalIno::from_raw(ino);
        let p_ino = LogicalIno::from_raw(new_parent);
        Self::check_file_name(&new_name)?;
        let inode = self
            .spin_no_delay(format!("link"), move |_, txn| Box::pin(txn.link(l_ino.storage_ino(), p_ino.storage_ino(), new_name.clone())))
            .await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(l_ino.kind, &inode), 0))
    }

    async fn unlink(&self, parent: u64, raw_name: ByteString) -> Result<()> {
        let p_ino = LogicalIno::from_raw(parent);
        self.spin_no_delay(format!("unlink, parent:{parent}, raw_name:{}", raw_name.escape_debug()),
            move |_, txn| Box::pin(txn.unlink(p_ino.storage_ino(), raw_name.clone())))
            .await
    }

    async fn rename(
        &self,
        parent: u64,
        raw_name: ByteString,
        new_parent: u64,
        new_raw_name: ByteString,
        _flags: u32,
    ) -> Result<()> {
        let p_ino = LogicalIno::from_raw(parent);
        let np_ino = LogicalIno::from_raw(new_parent);
        Self::check_file_name(&raw_name)?;
        Self::check_file_name(&new_raw_name)?;
        self.spin_no_delay(format!("rename"), move |_, txn| {
            let name = raw_name.clone();
            let new_name = new_raw_name.clone();
            Box::pin(async move {
                let ino = txn.clone().lookup(p_ino.storage_ino(), name.clone()).await?;
                txn.clone().link(ino, np_ino.storage_ino(), new_name).await?;
                txn.clone().unlink(p_ino.storage_ino(), name).await?;
                let inode = txn.clone().read_inode(ino).await?;
                if inode.typ == StorageDirItemKind::Directory {
                    txn.clone().unlink(ino, DIR_PARENT).await?;
                    txn.link(np_ino.storage_ino(), ino, DIR_PARENT).await?;
                }
                Ok(())
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: u64,
        name: ByteString,
        link: ByteString,
    ) -> Result<Entry> {
        let p_ino = LogicalIno::from_raw(parent);
        Self::check_file_name(&name)?;
        let name_clone1 = name.clone();
        let ino = self.spin_no_delay(format!("inode for symlink"), move |_, txn| {
            let name = name_clone1.clone();
            Box::pin(async move {
                if txn.clone().check_if_dir_entry_exists(p_ino.storage_ino(), &name).await? {
                    return Err(FsError::FileExist {
                        file: name.to_string(),
                    });
                }
                txn.clone().reserve_new_ino().await
            })
        }).await?;

        let mut inode = Inode::new(ino, StorageDirItemKind::Symlink, StorageFilePermission(0o777), gid, uid, 0);
        Txn::set_fresh_inode_to_link(self.fs_config.block_size, &mut inode, link.into_bytes());
        let ptr = Arc::new(inode);
        let ptr_clone1 = ptr.clone();

        self.spin_no_delay(format!("symlink inode"), move |_, txn| {
            let name = name.clone();
            let ptr = ptr_clone1.clone();
            Box::pin(async move {
                txn.clone().save_inode(&ptr).await?;
                txn.connect_inode_to_directory(p_ino.storage_ino(), &name, ptr.deref()).await
            })
        }).await?;

        Ok(Entry::new(self.map_storage_attr_to_fuser(InoKind::Regular, &ptr), 0))
    }

    async fn readlink(&self, ino: u64) -> Result<Data> {
        let l_ino = LogicalIno::from_raw(ino);
        let arc = self.weak.upgrade().unwrap();
        arc.spin(format!("readlink"), None, move |_, txn| {
            Box::pin(async move { Ok(Data::new(txn.read_link(l_ino.storage_ino()).await?)) })
        })
        .await
    }

    #[tracing::instrument]
    async fn fallocate(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
    ) -> Result<()> {
        let l_ino = LogicalIno::from_raw(ino);
        self.spin_no_delay(format!("fallocate"), move |_, txn| {
            Box::pin(async move {
                let mut inode = txn.clone().read_inode(l_ino.storage_ino()).await?.deref().clone();
                txn.fallocate(&mut inode, offset, length).await
            })
        })
        .await?;
        Ok(())
    }

    // TODO: Find an api to calculate total and available space on tikv.
    async fn statfs(&self, _ino: u64) -> Result<StatFs> {
        self.spin_no_delay(format!("statfs"), |_, txn| Box::pin(txn.statfs())).await
    }

    #[tracing::instrument]
    async fn setlk(
        &self,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
    ) -> Result<()> {
        let l_ino = LogicalIno::from_raw(ino);
        #[cfg(any(target_os = "freebsd", target_os = "macos"))]
        let typ = typ as i16;
        let not_again = self.spin_no_delay(format!("setlk"), move |_, txn| {
            Box::pin(async move {
                let mut inode = txn.clone().read_inode(l_ino.storage_ino()).await?.deref().clone();
                warn!("setlk, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                if inode.typ == StorageDirItemKind::Directory {
                    return Err(FsError::InvalidLock);
                }
                match typ {
                    F_RDLCK if inode.lock_state.lk_type == F_WRLCK => {
                        if sleep {
                            warn!("setlk F_RDLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        } else {
                            Err(FsError::InvalidLock)
                        }
                    }
                    F_RDLCK => {
                        inode.lock_state.owner_set.insert(lock_owner);
                        inode.lock_state.lk_type = F_RDLCK;
                        warn!("setlk F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                        txn.clone().save_inode(&Arc::new(inode)).await?;
                        Ok(true)
                    }
                    F_WRLCK => match inode.lock_state.lk_type {
                        F_RDLCK if inode.lock_state.owner_set.len() == 1
                        && inode.lock_state.owner_set.get(&lock_owner) == Some(&lock_owner)  => {
                            inode.lock_state.lk_type = F_WRLCK;
                            warn!("setlk F_WRLCK on F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            txn.save_inode(&Arc::new(inode)).await?;
                            Ok(true)
                        }
                        F_RDLCK if sleep => {
                            warn!("setlk F_WRLCK on F_RDLCK sleep return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        },
                        F_RDLCK => Err(FsError::InvalidLock),
                        F_UNLCK => {
                            inode.lock_state.owner_set.clear();
                            inode.lock_state.owner_set.insert(lock_owner);
                            inode.lock_state.lk_type = F_WRLCK;
                            warn!("setlk F_WRLCK on F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            txn.save_inode(&Arc::new(inode)).await?;
                            Ok(true)
                        },
                        F_WRLCK if sleep => {
                            warn!("setlk F_WRLCK on F_WRLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        }
                        F_WRLCK => Err(FsError::InvalidLock),
                        _ => Err(FsError::InvalidLock),
                    },
                    F_UNLCK => {
                        inode.lock_state.owner_set.remove(&lock_owner);
                        if inode.lock_state.owner_set.is_empty() {
                            inode.lock_state.lk_type = F_UNLCK;
                        }
                        warn!("setlk F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                        txn.save_inode(&Arc::new(inode)).await?;
                        Ok(true)
                    }
                    _ => Err(FsError::InvalidLock),
                }
            })
        })
        .await?;

        if !not_again {
            self.setlkw(l_ino.storage_ino(), lock_owner, typ).await
        } else {
            Ok(())
        }
    }

    #[tracing::instrument]
    async fn getlk(
        &self,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        pid: u32,
    ) -> Result<Lock> {
        let l_ino = LogicalIno::from_raw(ino);
        // TODO: read only operation need not txn?
        self.spin_no_delay(format!("getlk"), move |_, txn| {
            Box::pin(async move {
                let inode = txn.read_inode(l_ino.storage_ino()).await?;
                warn!("getlk, inode:{:?}, pid:{:?}", inode, pid);
                Ok(Lock::_new(0, 0, inode.lock_state.lk_type as i32, 0))
            })
        })
        .await
    }

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        _ino: u64,
        _name: ByteString,
        _value: Vec<u8>,
        _flags: i32,
        _position: u32,
    ) -> Result<()> {
        // TODO: implement me
        Ok(())
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn getxattr(&self, _ino: u64, _name: ByteString, size: u32) -> Result<Xattr> {
        // TODO: implement me
        if size == 0 {
            Ok(Xattr::size(0))
        } else {
            Ok(Xattr::data(Vec::new()))
        }
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn listxattr(&self, _ino: u64, size: u32) -> Result<Xattr> {
        // TODO: implement me
        if size == 0 {
            Ok(Xattr::size(0))
        } else {
            Ok(Xattr::data(Vec::new()))
        }
    }

    /// Remove an extended attribute.
    async fn removexattr(&self, _ino: u64, _name: ByteString) -> Result<()> {
        // TODO: implement me
        Ok(())
    }
}
