use std::{mem, ops::Deref, sync::Arc, time::{Duration, SystemTime}};

use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use fuser::{consts::FOPEN_DIRECT_IO, KernelConfig, TimeOrNow};
use futures::FutureExt;
use libc::{F_RDLCK, F_UNLCK, F_WRLCK, SEEK_CUR, SEEK_END, SEEK_SET};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::fs::{error::{FsError, Result}, inode::StorageFilePermission, key::{PARENT_OF_ROOT_INODE, ROOT_INODE}, reply::InoKind};

use super::{async_fs::AsyncFileSystem, file_handler::FileHandler, inode::StorageDirItemKind, mode::{as_file_kind, as_file_perm}, reply::{get_time, Attr, Create, Data, Dir, Entry, Lock, LogicalIno, Lseek, Open, StatFs, Write, Xattr}, tikv_fs::{map_file_type_to_storage_dir_item_kind, parse_filename, InoUse, TiFs, TiFsMutable}};



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
        config.add_capabilities(fuser::consts::FUSE_BIG_WRITES)
            .expect("kernel config failed to add cap_fuse FUSE_BIG_WRITES");
        config.add_capabilities(fuser::consts::FUSE_PARALLEL_DIROPS)
            .expect("kernel config failed to add cap_fuse FUSE_PARALLEL_DIROPS");
        let _ = config.set_max_write(self.fs_config.block_size as u32 * 128);

        if let Err(next_working) = config.set_max_readahead(self.fs_config.block_size as u32 * 100) {
            tracing::info!("max_readahead value adaped to nearest: {next_working}");
            config.set_max_readahead(next_working).map_err(|err|{
                tracing::warn!("setting of max_readahead failed with error: {err:?}");
            }).unwrap();
        }

        arc.spin_no_delay(format!("init"), move |fs, txn| {
            Box::pin(async move {
                info!("initializing tifs on {:?} ...", &fs.pd_endpoints);
                let root_inode = txn.clone().read_inode(ROOT_INODE).await;
                if let Err(FsError::KeyNotFound) = root_inode {
                    let attr = txn
                        .mkdir(
                            PARENT_OF_ROOT_INODE,
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
        let stat = self.lookup_all_info_logical(p_ino.storage_ino(), filename, kind).await?;

        let entry = Entry {
            generation: 0,
            time: Duration::from_millis(0),
            stat,
        };

        Ok(entry)
    }

    #[tracing::instrument]
    async fn getattr(&self, ino: u64) -> Result<Attr> {
        let l_ino = LogicalIno::from_raw(ino);
        let stat = self.get_all_file_attributes(l_ino).await?;
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
        if l_ino.kind != InoKind::Regular {
            return Err(FsError::InoKindNotSupported(l_ino.kind));
        }

        self.spin_no_delay(format!("setattr"), move |_, txn| {
            Box::pin(async move {
                txn.set_attributes(
                    l_ino.storage_ino(),
                    mode,
                    uid,
                    gid,
                    size,
                    atime,
                    mtime,
                    ctime,
                    _fh,
                    crtime,
                    _chgtime,
                    _bkuptime,
                    flags,
                ).await
            })
        })
        .await?;

        let attr = self.get_all_file_attributes(l_ino).await?;

        Ok(Attr {
            time: get_time(),
            attr,
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
                use_id: new_use_id,
            });
            self.with_mut_data(|d: &mut TiFsMutable| d.opened_ino.insert(ino, Arc::downgrade(&new_ino_use))).await?;
            ino_use = Some(new_ino_use);
        }

        let Some(ino_use) = ino_use else {
            return Err(FsError::UnknownError("failed to get ino_use!".into()));
        };

        let file_handler = FileHandler::new(ino_use, self.fs_config.clone());
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

    async fn fsync(&self, _ino: u64, fh: u64, _datasync: bool) -> Result<()> {
        self.flush_write_cache(fh).await
    }

    async fn flush(&self, _ino: u64, fh: u64, _lock_owner: u64) -> Result<()> {
        self.flush_write_cache(fh).await
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
        let (i_desc, i_size, i_attr) = self
            .spin_no_delay(format!("mkdir"), move |_, txn| {
                Box::pin(txn.mkdir(p_ino.storage_ino(), name.clone(), perm, gid, uid))
                }).await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(
            InoKind::Regular, &i_desc, &i_size, &i_attr,
            Some(i_size.last_change) // TODO: fetch real atime?
        ), 0))
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
        let typ = map_file_type_to_storage_dir_item_kind(as_file_kind(mode))?;
        Self::check_file_name(&name)?;
        let (i_desc, i_size, i_attr) = self
            .spin_no_delay(format!("mknod"), move |_, txn| {
                Box::pin(txn.make_inode(p_ino.storage_ino(), name.clone(), typ, perm, gid, uid, rdev, None))
            })
            .await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(
            InoKind::Regular, &i_desc, &i_size, &i_attr, Some(i_size.last_change) // TODO: fetch real atime?
        ), 0))
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
                        let inode = txn.read_ino_size(l_ino.storage_ino()).await?;
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
        self.flush_write_cache(fh).await?;
        self.release_file_handler(fh).await
    }

    /// Create a hard link.
    async fn link(&self, ino: u64, new_parent: u64, new_name: ByteString) -> Result<Entry> {
        let l_ino = LogicalIno::from_raw(ino);
        let p_ino = LogicalIno::from_raw(new_parent);
        Self::check_file_name(&new_name)?;
        let _inode = self
            .spin_no_delay(format!("link"), move |_, txn| Box::pin(txn.add_hard_link(l_ino.storage_ino(), p_ino.storage_ino(), new_name.clone())))
            .await?;
        let (i_desc, i_attr,
             i_size, atime) = self
            .spin_no_delay(format!("link"), move |_, txn| Box::pin(txn.get_all_ino_data(l_ino.storage_ino())))
            .await?;
        Ok(Entry::new(self.map_storage_attr_to_fuser(l_ino.kind, &i_desc, &i_size, &i_attr,
             Some(atime.0)), 0))
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
                let dir_item = txn.clone().lookup_ino(p_ino.storage_ino(), name.clone()).await?;
                txn.clone().add_hard_link(dir_item.ino, np_ino.storage_ino(), new_name).await?;
                txn.clone().unlink(p_ino.storage_ino(), name).await?;
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
        let link_data_vec = link.as_bytes().to_vec();
        Self::check_file_name(&name)?;
        let name_clone1 = name.clone();
        let (i_desc, i_size, i_attr ) =
            self.spin_no_delay(format!("inode for symlink"), move |_, txn| {
            let name = name_clone1.clone();
            let link_data_vec = link_data_vec.clone();
            Box::pin(async move {
                if txn.clone().check_if_dir_entry_exists(p_ino.storage_ino(), &name).await? {
                    return Err(FsError::FileExist {
                        file: name.to_string(),
                    });
                }
                txn.clone().make_inode(
                    p_ino.storage_ino(),
                    name,
                    StorageDirItemKind::Symlink,
                    StorageFilePermission(0o777),
                    gid, uid, 0,
                    Some(link_data_vec),
                ).await
            })
        }).await?;

        Ok(Entry::new(self.map_storage_attr_to_fuser(InoKind::Regular, &i_desc, &i_size, &i_attr,
            Some(i_size.last_change)), 0)) // TODO: use real atime?
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
        fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
    ) -> Result<()> {
        let l_ino: LogicalIno = LogicalIno::from_raw(ino);
        if l_ino.kind != InoKind::Regular {
            return Err(FsError::InoKindNotSupported(l_ino.kind));
        }
        let fh = self.get_file_handler_checked(fh).await?;
        self.spin_no_delay(format!("fallocate"), move |_, txn| {
            let fh = fh.clone();
            Box::pin(async move {
                txn.f_allocate(fh.clone(), l_ino.storage_ino(), offset, length).await
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
                let i_desc = txn.clone().read_inode(l_ino.storage_ino()).await?.deref().clone();
                let mut i_lock_state = txn.clone().read_ino_lock_state(l_ino.storage_ino()).await?.deref().clone();
                warn!("setlk, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                if i_desc.typ == StorageDirItemKind::Directory {
                    return Err(FsError::InvalidLock);
                }
                match typ {
                    F_RDLCK if i_lock_state.lk_type == F_WRLCK => {
                        if sleep {
                            warn!("setlk F_RDLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        } else {
                            Err(FsError::InvalidLock)
                        }
                    }
                    F_RDLCK => {
                        i_lock_state.owner_set.insert(lock_owner);
                        i_lock_state.lk_type = F_RDLCK;
                        warn!("setlk F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                        txn.clone().write_ino_lock_state(l_ino.storage_ino(), i_lock_state).await?;
                        Ok(true)
                    }
                    F_WRLCK => match i_lock_state.lk_type {
                        F_RDLCK if i_lock_state.owner_set.len() == 1
                        && i_lock_state.owner_set.get(&lock_owner) == Some(&lock_owner)  => {
                            i_lock_state.lk_type = F_WRLCK;
                            warn!("setlk F_WRLCK on F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                            txn.clone().write_ino_lock_state(l_ino.storage_ino(), i_lock_state).await?;
                            Ok(true)
                        }
                        F_RDLCK if sleep => {
                            warn!("setlk F_WRLCK on F_RDLCK sleep return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        },
                        F_RDLCK => Err(FsError::InvalidLock),
                        F_UNLCK => {
                            i_lock_state.owner_set.clear();
                            i_lock_state.owner_set.insert(lock_owner);
                            i_lock_state.lk_type = F_WRLCK;
                            warn!("setlk F_WRLCK on F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                            txn.clone().write_ino_lock_state(l_ino.storage_ino(), i_lock_state).await?;
                            Ok(true)
                        },
                        F_WRLCK if sleep => {
                            warn!("setlk F_WRLCK on F_WRLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                            Ok(false)
                        }
                        F_WRLCK => Err(FsError::InvalidLock),
                        _ => Err(FsError::InvalidLock),
                    },
                    F_UNLCK => {
                        i_lock_state.owner_set.remove(&lock_owner);
                        if i_lock_state.owner_set.is_empty() {
                            i_lock_state.lk_type = F_UNLCK;
                        }
                        warn!("setlk F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", i_lock_state, pid, typ, i_lock_state.lk_type, lock_owner, sleep);
                        txn.clone().write_ino_lock_state(l_ino.storage_ino(), i_lock_state).await?;
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
                let ino_lock_state = txn.read_ino_lock_state(l_ino.storage_ino()).await?;
                warn!("getlk, inode:{:?}, pid:{:?}", ino_lock_state, pid);
                Ok(Lock::_new(0, 0, ino_lock_state.lk_type as i32, 0))
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
