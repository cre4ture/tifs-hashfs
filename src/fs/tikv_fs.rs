use std::ffi::OsString;
use std::fmt::{self, Debug};
use std::future::Future;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use std::pin::Pin;

use anyhow::anyhow;
use async_trait::async_trait;
use fuser::*;
use futures::future::try_join_all;
use tikv_client::{Config, Key, Transaction, TransactionClient};
use tracing::trace;

use super::async_fs::AsyncFileSystem;
use super::dir::Directory;
use super::error::{FsError, Result};
use super::file_handler::{FileHandler, FileHub};
use super::inode::Inode;
use super::key::{ScopedKey, ROOT_INODE};
use super::reply::*;

pub struct TiFs {
    inode_next: AtomicU64,
    pd_endpoints: Vec<String>,
    config: Config,
    client: TransactionClient,

    hub: FileHub,
    // inode_cache: RwLock<LruCache<u64, Inode>>,
    // block_cache: RwLock<LruCache<ScopedKey, Vec<u8>>>,
    // dir_cache: RwLock<LruCache<u64, Directory>>,
}

trait WithTransaction<T> {
    type InputFuture<'b>: 'b + Future<Output=Result<T>>;
    type ResultFuture<'a>: 'a + Future<Output=Result<T>>;

    fn with_txn<'b, F>(&'b self, f: F) -> Self::ResultFuture<'b>
    where
        F: for <'a> FnOnce(&'a mut Transaction) -> Self::InputFuture<'a>;
}

impl <T: 'static> WithTransaction<T> for TiFs {
    type InputFuture<'b> = impl Future<Output=Result<T>> + 'b;
    type ResultFuture<'a> = impl Future<Output=Result<T>> + 'a;

    fn with_txn<'b, F>(&'b self, f: F) -> Self::ResultFuture<'b>
    where
        F: 'static + for <'a> FnOnce(&'a mut Transaction) -> Self::InputFuture<'a>,
    {
        async move {
            let mut txn = self.client.begin().await?;
            match f(&mut txn).await {
                Ok(v) => {
                    txn.commit().await?;
                    Ok(v)
                }
                Err(e) => {
                    txn.rollback().await?;
                    Err(e)
                }
            }
        }
    }
}


impl TiFs {
    pub const SCAN_LIMIT: u32 = 1 << 10;
    pub const BLOCK_SIZE: u64 = 1 << 12;
    pub const BLOCK_CACHE: usize = 1 << 25;
    pub const DIR_CACHE: usize = 1 << 24;
    pub const INODE_CACHE: usize = 1 << 24;

    pub async fn construct<S>(pd_endpoints: Vec<S>, cfg: Config) -> anyhow::Result<Self>
    where
        S: Clone + Into<String>,
    {
        Ok(TiFs {
            inode_next: AtomicU64::new(ROOT_INODE),
            pd_endpoints: pd_endpoints.clone().into_iter().map(Into::into).collect(),
            config: cfg.clone(),
            client: TransactionClient::new_with_config(pd_endpoints, cfg)
                .await
                .map_err(|err| anyhow!("{}", err))?,

            hub: FileHub::new(),
            // inode_cache: RwLock::new(LruCache::new(Self::INODE_CACHE / size_of::<Inode>())),
            // block_cache: RwLock::new(LruCache::new(Self::BLOCK_CACHE / Self::BLOCK_SIZE)),
            // dir_cache: RwLock::new(LruCache::new(Self::DIR_CACHE / Self::BLOCK_SIZE)),
        })
    }

    async fn read_fh(&self, ino: u64, fh: u64) -> Result<FileHandler> {
        self.hub
            .get(ino, fh)
            .await
            .ok_or_else(|| FsError::FhNotFound { fh })
    }

    async fn read_data(&self, ino: u64, start: u64, chunk_size: Option<u64>) -> Result<Vec<u8>> {
        let mut attr = self.read_inode(ino).await?;
        let size = chunk_size.unwrap_or_else(|| attr.size - start);
        let target = attr.size.min(start + size);

        let data_size = target - start;
        let start_block = start / Self::BLOCK_SIZE;
        let end_block = (target + Self::BLOCK_SIZE - 1) / Self::BLOCK_SIZE;

        self.with_txn(async move |txn|  {
            let pairs = txn
                .scan(
                    ScopedKey::block_range(ino, start_block..end_block),
                    (end_block - start_block) as u32,
                )
                .await?;
            let data = pairs.enumerate().fold(
                Vec::with_capacity(data_size as usize),
                |mut data, (i, pair)| {
                    let value = pair.into_value();
                    let mut slice = value.as_slice();
                    slice = match i {
                        0 => &slice[(start_block % Self::BLOCK_SIZE) as usize..],
                        n if (n + 1) * Self::BLOCK_SIZE as usize > data_size as usize => {
                            &slice[..(data_size % Self::BLOCK_SIZE) as usize]
                        }
                        _ => slice,
                    };

                    data.extend(slice);
                    data
                },
            );

            attr.atime = SystemTime::now();
            Self::save_inode(txn, attr.into()).await?;
            Ok(data)
        })
        .await
    }

    // async fn clear_data(&self, ino: u64) -> Result<usize> {
    //     let mut attr = self.read_inode(ino).await?;
    //     let end_block = (attr.size + Self::BLOCK_SIZE - 1) / Self::BLOCK_SIZE;
    //     let mut txn = self.client.begin().await?;
    //     let delete_tasks = (0..end_block).into_iter().map(|block| txn.delete(ScopedKey::new(ino, block).scoped()));
    //     try_join_all(delete_tasks).await?;

    //     let data = pairs.enumerate().fold(
    //         Vec::with_capacity(data_size as usize),
    //         |mut data, (i, pair)| {
    //             let value = pair.into_value();
    //             let mut slice = value.as_slice();
    //             slice = match i {
    //                 0 => &slice[(start_block % Self::BLOCK_SIZE) as usize..],
    //                 n if (n + 1) * Self::BLOCK_SIZE as usize > data_size as usize => {
    //                     &slice[..(data_size % Self::BLOCK_SIZE) as usize]
    //                 }
    //                 _ => slice,
    //             };

    //             data.extend(slice);
    //             data
    //         },
    //     );

    //     attr.atime = SystemTime::now();
    //     self.save_inode(&mut txn, attr.into()).await?;
    //     Ok(data)
    // }

    // async fn write_data(&self, ino: u64, start: u64, data: Vec<u8>) -> Result<usize> {
    //     let mut attr = self.read_inode(ino).await?;
    //     let size = chunk_size.unwrap_or_else(|| attr.size - start);
    //     let target = attr.size.min(start + size);

    //     let data_size = target - start;
    //     let start_block = start / Self::BLOCK_SIZE;
    //     let end_block = (target + Self::BLOCK_SIZE - 1) / Self::BLOCK_SIZE;
    //     let mut txn = self.client.begin().await?;
    //     let pairs = txn
    //         .scan(
    //             ScopedKey::block_range(ino, start_block..end_block),
    //             (end_block - start_block) as u32,
    //         )
    //         .await?;
    //     let data = pairs.enumerate().fold(
    //         Vec::with_capacity(data_size as usize),
    //         |mut data, (i, pair)| {
    //             let value = pair.into_value();
    //             let mut slice = value.as_slice();
    //             slice = match i {
    //                 0 => &slice[(start_block % Self::BLOCK_SIZE) as usize..],
    //                 n if (n + 1) * Self::BLOCK_SIZE as usize > data_size as usize => {
    //                     &slice[..(data_size % Self::BLOCK_SIZE) as usize]
    //                 }
    //                 _ => slice,
    //             };

    //             data.extend(slice);
    //             data
    //         },
    //     );

    //     attr.atime = SystemTime::now();
    //     self.save_inode(&mut txn, attr.into()).await?;
    //     Ok(data)
    // }

    async fn read_dir(&self, ino: u64) -> Result<Directory> {
        let data = self.read_data(ino, 0, None).await?;
        Directory::deserialize(&data)
    }

    async fn read_inode(&self, ino: u64) -> Result<FileAttr> {
        let mut txn = self.client.begin().await?;
        let value = txn
            .get(ScopedKey::inode(ino).scoped())
            .await?
            .ok_or_else(|| FsError::InodeNotFound { inode: ino })?;
        txn.rollback().await?;
        Ok(Inode::deserialize(&value)?.0)
    }

    async fn save_inode(txn: &mut Transaction, mut inode: Inode) -> Result<()> {
        inode.0.mtime = SystemTime::now();
        txn.put(ScopedKey::inode(inode.0.ino).scoped(), inode.serialize()?)
            .await?;
        Ok(())
    }
}

impl Debug for TiFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("tifs({:?})", self.pd_endpoints))
    }
}

#[async_trait]
impl AsyncFileSystem for TiFs {
    #[tracing::instrument]
    async fn init(&self) -> Result<()> {
        trace!("initializing tifs on {:?} ...", self.pd_endpoints);
        let mut txn = self.client.begin().await?;
        let mut start_inode = ROOT_INODE;
        let mut max_key: Option<Key> = None;
        loop {
            let keys: Vec<Key> = txn
                .scan_keys(ScopedKey::inode(start_inode).scoped().., Self::SCAN_LIMIT)
                .await?
                .collect();
            if keys.is_empty() {
                break;
            }
            max_key = Some(keys[keys.len() - 1].clone());
            start_inode += Self::SCAN_LIMIT as u64
        }

        if let Some(key) = max_key {
            self.inode_next
                .store(ScopedKey::from(key).key() + 1, Ordering::Relaxed)
        } else {
            let root = Inode(FileAttr {
                ino: FUSE_ROOT_ID,
                size: 0,
                blocks: 0,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: FileType::Directory,
                perm: 0o777,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: Self::BLOCK_SIZE as u32,
                padding: 0,
                flags: 0,
            });
            Self::save_inode(&mut txn, root).await?;
            self.inode_next.store(2, Ordering::Relaxed);
        }

        Ok(())
    }

    #[tracing::instrument]
    async fn lookup(&self, parent: u64, name: OsString) -> Result<Entry> {
        // TODO: use cache
        let dir = self.read_dir(parent).await?;
        let file = dir.get(&name).ok_or_else(|| FsError::FileNotFound {
            file: name.to_string_lossy().to_string(),
        })?;
        Ok(Entry::new(self.read_inode(file.ino).await?, 0))
    }

    #[tracing::instrument]
    async fn getattr(&self, ino: u64) -> Result<Attr> {
        Ok(Attr::new(self.read_inode(ino).await?))
    }

    async fn readdir(&self, ino: u64, _fh: u64, offset: i64) -> Result<Dir> {
        let directory = self.read_dir(ino).await?;
        let mut dir = Dir::offset(offset as usize);
        for (i, item) in directory.into_map().into_values().into_iter().enumerate() {
            if i >= offset as usize {
                dir.push(item)
            }
        }
        Ok(dir)
    }

    async fn open(&self, ino: u64, flags: i32) -> Result<Open> {
        // TODO: deal with flags
        let fh = self.hub.make(ino).await;
        Ok(Open::new(fh, flags as u32))
    }

    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Data> {
        let handler = self.read_fh(ino, fh).await?;
        let mut cursor = handler.cursor().await;
        *cursor = ((*cursor) as i64 + offset) as usize;
        let data = self
            .read_data(ino, *cursor as u64, Some(size as u64))
            .await?;
        *cursor += data.len();
        Ok(Data::new(data))
    }

    /// Create a directory.
    async fn mkdir(&self, parent: u64, name: OsString, mode: u32, _umask: u32) -> Result<Entry> {
        Inode(FileAttr {
            ino: FUSE_ROOT_ID,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::Directory,
            perm: 0o777,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: Self::BLOCK_SIZE as u32,
            padding: 0,
            flags: 0,
        });
        Err(FsError::unimplemented())
    }
}
