use std::{ffi::OsStr, path::Path, sync::Arc, time::SystemTime};

use fuser::{Filesystem, KernelConfig, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow};
use tokio::spawn;

use super::{block_on, spawn_reply, AsyncFileSystem};
use std::fmt::Debug;


pub struct AsyncFs<T>(pub Arc<T>);

impl<T: AsyncFileSystem> From<T> for AsyncFs<T> {
    fn from(inner: T) -> Self {
        Self(Arc::new(inner))
    }
}

impl<T: Debug> Debug for AsyncFs<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsyncFileSystem + 'static> Filesystem for AsyncFs<T> {
    fn init(
        &mut self,
        req: &Request,
        config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        let uid = req.uid();
        let gid = req.gid();

        block_on(self.0.init(gid, uid, config)).map_err(|err| err.into())
    }

    fn destroy(&mut self) {
        block_on(self.0.destroy())
    }

    #[tracing::instrument(skip(self))]
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.lookup(parent, name).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        let async_impl = self.0.clone();

        // TODO: union the spawn function for request without reply
        spawn(async move {
            async_impl.forget(ino, nlookup).await;
        });
    }

    #[tracing::instrument(skip(self))]
    fn getattr(&mut self, req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let async_impl = self.0.clone();
        spawn_reply(
            req.unique(),
            reply,
            async move { async_impl.getattr(ino).await },
        );
    }

    #[tracing::instrument(skip(self))]
    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .setattr(
                    ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime,
                    flags,
                )
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn readlink(&mut self, req: &Request, ino: u64, reply: ReplyData) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.readlink(ino).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        let uid = req.uid();
        let gid = req.gid();

        spawn_reply(req.unique(), reply, async move {
            async_impl
                .mknod(parent, name, mode, gid, uid, umask, rdev)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn mkdir(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        let uid = req.uid();
        let gid = req.gid();

        spawn_reply(req.unique(), reply, async move {
            async_impl.mkdir(parent, name, mode, gid, uid, umask).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.unlink(parent, name).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.rmdir(parent, name).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        let link = link.to_string_lossy().to_string().into();
        let uid = req.uid();
        let gid = req.gid();

        spawn_reply(req.unique(), reply, async move {
            async_impl.symlink(gid, uid, parent, name, link).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn rename(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        let newname = newname.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .rename(parent, name, newparent, newname, flags)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn link(
        &mut self,
        req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let async_impl = self.0.clone();
        let newname = newname.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.link(ino, newparent, newname).await
        });
    }

    fn open(&mut self, req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.open(ino, flags).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn read(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let async_impl = self.0.clone();
        //eprintln!("read request: id:{}, ino:{ino}, fh:{fh}, pos:{offset}, size:{size}, flags:{flags}", req.unique());
        spawn_reply(req.unique(), reply, async move {
            let r = async_impl
                .read(ino, fh, offset, size, flags, lock_owner)
                .await;
            // if let Ok(data) = &r {
            //     eprintln!("read done: len: {}", data.data.len());
            // } else {
            //     eprintln!("read done: err: {:?}", r);
            // }
            r
        });
    }

    #[tracing::instrument(skip(self))]
    fn write(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let async_impl = self.0.clone();
        let data = data.to_owned();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .write(ino, fh, offset, data, write_flags, flags, lock_owner)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn flush(&mut self, req: &Request, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.flush(ino, fh, lock_owner).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn release(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        flags: i32,
        lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.release(ino, fh, flags, lock_owner, flush).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn fsync(&mut self, req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.fsync(ino, fh, datasync).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn opendir(&mut self, req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.opendir(ino, flags).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, reply: ReplyDirectory) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.readdir(ino, fh, offset).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn readdirplus(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectoryPlus,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.readdirplus(ino, fh, offset).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn fsyncdir(&mut self, req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.fsyncdir(ino, fh, datasync).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn statfs(&mut self, req: &Request, ino: u64, reply: ReplyStatfs) {
        let async_impl = self.0.clone();
        spawn_reply(
            req.unique(),
            reply,
            async move { async_impl.statfs(ino).await },
        );
    }

    #[tracing::instrument(skip(self))]
    fn setxattr(
        &mut self,
        req: &Request,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        let value = value.to_owned();
        spawn_reply(req.unique(), reply, async move {
            async_impl.setxattr(ino, name, value, flags, position).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn getxattr(&mut self, req: &Request, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.getxattr(ino, name, size).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn listxattr(&mut self, req: &Request, ino: u64, size: u32, reply: ReplyXattr) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.listxattr(ino, size).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn removexattr(&mut self, req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl.removexattr(ino, name).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn access(&mut self, req: &Request, ino: u64, mask: i32, reply: ReplyEmpty) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.access(ino, mask).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let uid = req.uid();
        let gid = req.gid();

        let async_impl = self.0.clone();
        let name = name.to_string_lossy().to_string().into();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .create(uid, gid, parent, name, mode, umask, flags)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn getlk(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .getlk(ino, fh, lock_owner, start, end, typ, pid)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn setlk(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .setlk(ino, fh, lock_owner, start, end, typ, pid, sleep)
                .await
        });
    }

    #[tracing::instrument(skip(self))]
    fn bmap(&mut self, req: &Request, ino: u64, blocksize: u32, idx: u64, reply: ReplyBmap) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.bmap(ino, blocksize, idx).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn fallocate(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.fallocate(ino, fh, offset, length, mode).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn lseek(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl.lseek(ino, fh, offset, whence).await
        });
    }

    #[tracing::instrument(skip(self))]
    fn copy_file_range(
        &mut self,
        req: &Request,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: ReplyWrite,
    ) {
        let async_impl = self.0.clone();
        spawn_reply(req.unique(), reply, async move {
            async_impl
                .copy_file_range(
                    ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags,
                )
                .await
        });
    }
}
