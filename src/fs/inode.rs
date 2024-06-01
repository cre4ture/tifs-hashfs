use std::collections::HashSet;
use std::fmt::Display;
use std::time::SystemTime;


use serde::{Deserialize, Serialize};

use super::error::{FsError, Result};
use super::serialize::{deserialize, serialize, ENCODING};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LockState {
    pub owner_set: HashSet<u64>,
    #[cfg(target_os = "linux")]
    pub lk_type: i32,
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    pub lk_type: i16,
}

pub type Hash = blake3::Hash;
pub type TiFsHash = blake3::Hash;
pub type UInode = u64;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BlockAddress {
    pub ino: StorageIno,
    pub index: u64,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageIno(pub u64);

impl Display for StorageIno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageDirItemKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDirItem {
    pub ino: StorageIno,
    pub name: String,
    pub typ: StorageDirItemKind,
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct StorageFilePermission(pub u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct StorageFileAttr {
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Permissions
    pub perm: StorageFilePermission,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Inode {
    pub ino: StorageIno,
    pub typ: StorageDirItemKind,
    pub attr: StorageFileAttr,
    pub lock_state: LockState,
    size: u64,
    blocks: u64,
    inline_data: Option<Vec<u8>>,
    pub data_hash: Option<Hash>,
}

impl Inode {
    pub fn new(ino: StorageIno, typ: StorageDirItemKind, perm: StorageFilePermission, gid: u32, uid: u32, rdev: u32,) -> Self {
        let inode = Self {
            ino,
            typ,
            size: 0,
            blocks: 0,
            attr: StorageFileAttr {
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                perm,
                uid,
                gid,
                rdev,
                flags: 0,
            },
            lock_state: LockState::new(HashSet::new(), 0),
            inline_data: None,
            data_hash: None,
        };
        inode
    }

    pub fn storage_ino(&self) -> StorageIno {
        self.ino
    }

    fn update_blocks(&mut self, block_size: u64) {
        self.blocks = self.size.div_ceil(block_size);
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn blocks(&self) -> u64 {
        self.blocks
    }

    pub fn set_size(&mut self, size: u64, block_size: u64) {
        self.size = size;
        self.update_blocks(block_size);
        self.data_hash = None;
    }

    pub fn inline_data(&self) -> Option<&Vec<u8>> {
        self.inline_data.as_ref()
    }

    pub fn take_inline_data(&mut self) -> Option<Vec<u8>> {
        self.data_hash = None;
        self.inline_data.take()
    }

    pub fn set_inline_data(&mut self, data: Vec<u8>) {
        self.inline_data = Some(data);
        self.data_hash = None;
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|err| FsError::Serialize {
            target: "inode",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes).map_err(|err| FsError::Serialize {
            target: "inode",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }
}

impl From<Inode> for LockState {
    fn from(inode: Inode) -> Self {
        inode.lock_state
    }
}

impl LockState {
    #[cfg(target_os = "linux")]
    pub fn new(owner_set: HashSet<u64>, lk_type: i32) -> LockState {
        LockState { owner_set, lk_type }
    }
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    pub fn new(owner_set: HashSet<u64>, lk_type: i16) -> LockState {
        LockState { owner_set, lk_type }
    }
}
