use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{FileAttr, FileType, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};

use super::error::Result;
use super::inode::StorageIno;
use super::key::ROOT_INODE;
use super::utils::common_prints::debug_print_start_and_end_bytes_of_buffer;

pub fn get_time() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

#[derive(Debug)]
pub struct Entry {
    pub time: Duration,
    pub stat: FileAttr,
    pub generation: u64,
}

impl Entry {
    pub fn new(stat: FileAttr, generation: u64) -> Self {
        Self {
            time: get_time(),
            stat,
            generation,
        }
    }
}

#[derive(Debug)]
pub struct Open {
    pub fh: u64,
    pub flags: u32,
}
impl Open {
    pub fn new(fh: u64, flags: u32) -> Self {
        Self { fh, flags }
    }
}

#[derive(Debug)]
pub struct Attr {
    pub time: Duration,
    pub attr: FileAttr,
}
impl Attr {
    pub fn new(attr: FileAttr) -> Self {
        Self {
            time: get_time(),
            attr,
        }
    }
}

pub struct Data {
    pub data: Vec<u8>,
}
impl Data {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}
impl Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", debug_print_start_and_end_bytes_of_buffer(16, &self.data))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InoKind {
    Regular,
    Hash,
    Hashes,
}

impl InoKind {
    pub const fn kind_id(&self) -> u64 {
        match self {
            InoKind::Regular => 0u64,
            InoKind::Hash => 1u64,
            InoKind::Hashes => 2u64,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalIno {
    pub storage_ino: StorageIno,
    pub kind: InoKind,
}

pub const LOGICAL_INO_FACTOR: u64 = 8;

impl LogicalIno {
    pub const fn from_raw(ino: u64) -> Self {
        let (base_ino, kind_id) = {
            let r = ino - ROOT_INODE.0;
            let kind_id =  r % LOGICAL_INO_FACTOR;
            let base_ino = r / LOGICAL_INO_FACTOR;
            (1 + base_ino, kind_id)
        };

        let kind = match kind_id {
            0 => InoKind::Regular,
            1 => InoKind::Hash,
            2 => InoKind::Hashes,
            _ => InoKind::Regular,
        };

        Self {
            storage_ino: StorageIno(base_ino),
            kind,
        }
    }

    pub const fn to_raw(&self) -> u64 {
        let kind_id = self.kind.kind_id();
        let ino = ROOT_INODE.0 + ((self.storage_ino.0 - 1) * LOGICAL_INO_FACTOR) + kind_id;
        ino
    }

    pub const fn storage_ino(&self) -> StorageIno {
        self.storage_ino
    }
}

#[derive(Debug, Clone)]
pub struct DirItem {
    pub ino: LogicalIno,
    pub name: String,
    pub typ: FileType,
}

#[derive(Debug, Default)]
pub struct Dir {
    offset: usize,
    items: Vec<DirItem>,
}

impl Dir {
    pub fn offset(offset: usize) -> Self {
        Self {
            offset,
            items: Default::default(),
        }
    }

    pub fn new() -> Self {
        Default::default()
    }

    pub fn push(&mut self, item: DirItem) {
        self.items.push(item)
    }
}

pub type Directory = Vec<DirItem>;

#[derive(Debug, Default)]
pub struct DirPlus {
    offset: usize,
    items: Vec<(DirItem, Entry)>,
}

impl DirPlus {
    pub fn offset(offset: usize) -> Self {
        Self {
            offset,
            items: Default::default(),
        }
    }

    pub fn new() -> Self {
        Default::default()
    }

    pub fn push(&mut self, item: DirItem, entry: Entry) {
        self.items.push((item, entry))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct StatFs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

impl StatFs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
    ) -> Self {
        Self {
            blocks,
            bfree,
            bavail,
            files,
            ffree,
            bsize,
            namelen,
            frsize,
        }
    }
}

#[derive(Debug)]
pub struct Write {
    pub size: u32,
}
impl Write {
    pub fn new(size: u32) -> Self {
        Self { size }
    }
}

#[derive(Debug)]
pub struct Create {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
    pub fh: u64,
    pub flags: u32,
}
impl Create {
    pub fn new(attr: FileAttr, generation: u64, fh: u64, flags: u32) -> Self {
        Self {
            ttl: get_time(),
            attr,
            generation,
            fh,
            flags,
        }
    }
}

#[derive(Debug)]
pub struct Lock {
    pub start: u64,
    pub end: u64,
    pub typ: i32,
    pub pid: u32,
}

impl Lock {
    pub fn _new(start: u64, end: u64, typ: i32, pid: u32) -> Self {
        Self {
            start,
            end,
            typ,
            pid,
        }
    }
}

#[derive(Debug)]
pub enum Xattr {
    Data { data: Vec<u8> },
    Size { size: u32 },
}
impl Xattr {
    pub fn data(data: Vec<u8>) -> Self {
        Xattr::Data { data }
    }
    pub fn size(size: u32) -> Self {
        Xattr::Size { size }
    }
}

#[derive(Debug)]
pub struct Bmap {
    block: u64,
}

impl Bmap {
    pub fn new(block: u64) -> Self {
        Self { block }
    }
}

#[derive(Debug)]
pub struct Lseek {
    pub offset: i64,
}

impl Lseek {
    pub fn new(offset: i64) -> Self {
        Self { offset }
    }
}

pub trait FsReply<T: Debug>: Sized {
    fn reply_ok(self, item: T);
    fn reply_err(self, err: libc::c_int);

    fn reply(self, id: u64, result: Result<T>) {
        match result {
            Ok(item) => {
                trace!("ok. reply for request({}): {item:?}", id);
                //eprintln!("ok. reply for request({}) - type: {}", id, type_name::<T>());
                self.reply_ok(item)
            }
            Err(err) => {
                debug!("err. reply with {} for request ({})", err, id);
                //eprintln!("err. reply with {} for request ({}) - type: {}", err, id, type_name::<T>());

                let err = err.into();
                if err == -1 {
                    error!("returned -1");
                }
                self.reply_err(err)
            }
        }
    }
}

impl FsReply<Entry> for ReplyEntry {
    fn reply_ok(self, item: Entry) {
        self.entry(&item.time, &item.stat, item.generation);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Open> for ReplyOpen {
    fn reply_ok(self, item: Open) {
        self.opened(item.fh, item.flags);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Attr> for ReplyAttr {
    fn reply_ok(self, item: Attr) {
        self.attr(&item.time, &item.attr);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Data> for ReplyData {
    fn reply_ok(self, item: Data) {
        self.data(item.data.as_slice());
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Dir> for ReplyDirectory {
    fn reply_ok(mut self, dir: Dir) {
        for (index, item) in dir.items.into_iter().enumerate() {
            if self.add(
                item.ino.to_raw(),
                (index + 1 + dir.offset) as i64,
                item.typ,
                item.name,
            ) {
                break;
            }
        }
        self.ok()
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<DirPlus> for ReplyDirectoryPlus {
    fn reply_ok(mut self, dir: DirPlus) {
        for (index, (item, entry)) in dir.items.into_iter().enumerate() {
            if self.add(
                item.ino.to_raw(),
                (dir.offset + index) as i64,
                item.name,
                &entry.time,
                &entry.stat,
                entry.generation,
            ) {
                break;
            }
        }
        self.ok()
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<StatFs> for ReplyStatfs {
    fn reply_ok(self, item: StatFs) {
        self.statfs(
            item.blocks,
            item.bfree,
            item.bavail,
            item.files,
            item.ffree,
            item.bsize,
            item.namelen,
            item.frsize,
        )
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Write> for ReplyWrite {
    fn reply_ok(self, item: Write) {
        self.written(item.size);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Create> for ReplyCreate {
    fn reply_ok(self, item: Create) {
        self.created(&item.ttl, &item.attr, item.generation, item.fh, item.flags);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Lock> for ReplyLock {
    fn reply_ok(self, item: Lock) {
        self.locked(item.start, item.end, item.typ, item.pid);
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Xattr> for ReplyXattr {
    fn reply_ok(self, item: Xattr) {
        use Xattr::*;
        match item {
            Data { data } => self.data(data.as_slice()),
            Size { size } => self.size(size),
        }
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Bmap> for ReplyBmap {
    fn reply_ok(self, item: Bmap) {
        self.bmap(item.block)
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<Lseek> for ReplyLseek {
    fn reply_ok(self, item: Lseek) {
        self.offset(item.offset)
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}

impl FsReply<()> for ReplyEmpty {
    fn reply_ok(self, _: ()) {
        self.ok();
    }
    fn reply_err(self, err: libc::c_int) {
        self.error(err);
    }
}


#[cfg(test)]
mod test {
    use crate::fs::{key::{ROOT_INODE, ROOT_LOGICAL_INODE}, reply::{InoKind, LOGICAL_INO_FACTOR}};

    use super::LogicalIno;

    #[test]
    fn root_inode_check() {
        assert_eq!(ROOT_LOGICAL_INODE.kind, InoKind::Regular);
        assert_eq!(ROOT_LOGICAL_INODE.to_raw(), 1);
        assert_eq!(ROOT_LOGICAL_INODE.storage_ino().0, 1);
        assert_eq!(ROOT_INODE.0, 1);
    }

    #[test]
    fn hash_kind_check_root_ino() {
        let l_ino = LogicalIno::from_raw(2);
        assert_eq!(l_ino.kind, InoKind::Hash);
        assert_eq!(l_ino.to_raw(), 2);
        assert_eq!(l_ino.storage_ino().0, 1);
    }

    #[test]
    fn hashes_kind_check_root_ino() {
        let l_ino = LogicalIno::from_raw(3);
        assert_eq!(l_ino.kind, InoKind::Hashes);
        assert_eq!(l_ino.to_raw(), 3);
        assert_eq!(l_ino.storage_ino().0, 1);
    }

    #[test]
    fn check_second_ino() {
        let l_ino = LogicalIno::from_raw(1 + LOGICAL_INO_FACTOR);
        let l_ino_hash = LogicalIno::from_raw(2 + LOGICAL_INO_FACTOR);
        let l_ino_hashes = LogicalIno::from_raw(3 + LOGICAL_INO_FACTOR);
        assert_eq!(l_ino.kind, InoKind::Regular);
        assert_eq!(l_ino_hash.kind, InoKind::Hash);
        assert_eq!(l_ino_hashes.kind, InoKind::Hashes);
        assert_eq!(l_ino.to_raw(), 1 + LOGICAL_INO_FACTOR);
        assert_eq!(l_ino_hash.to_raw(), 2 + LOGICAL_INO_FACTOR);
        assert_eq!(l_ino_hashes.to_raw(), 3 + LOGICAL_INO_FACTOR);
        assert_eq!(l_ino.storage_ino().0, 2);
        assert_eq!(l_ino_hash.storage_ino().0, 2);
        assert_eq!(l_ino_hashes.storage_ino().0, 2);
    }

    #[test]
    fn check_3rd_ino() {
        let l_ino = LogicalIno::from_raw(1 + LOGICAL_INO_FACTOR * 2);
        let l_ino_hash = LogicalIno::from_raw(2 + LOGICAL_INO_FACTOR * 2);
        let l_ino_hashes = LogicalIno::from_raw(3 + LOGICAL_INO_FACTOR * 2);
        assert_eq!(l_ino.kind, InoKind::Regular);
        assert_eq!(l_ino_hash.kind, InoKind::Hash);
        assert_eq!(l_ino_hashes.kind, InoKind::Hashes);
        assert_eq!(l_ino.to_raw(), 1 + LOGICAL_INO_FACTOR * 2);
        assert_eq!(l_ino_hash.to_raw(), 2 + LOGICAL_INO_FACTOR * 2);
        assert_eq!(l_ino_hashes.to_raw(), 3 + LOGICAL_INO_FACTOR * 2);
        assert_eq!(l_ino.storage_ino().0, 3);
        assert_eq!(l_ino_hash.storage_ino().0, 3);
        assert_eq!(l_ino_hashes.storage_ino().0, 3);
    }
}
