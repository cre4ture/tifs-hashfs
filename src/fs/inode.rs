use std::collections::HashSet;
use std::fmt::Display;
use std::time::SystemTime;


use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InoLockState {
    pub owner_set: HashSet<u64>,
    #[cfg(target_os = "linux")]
    pub lk_type: i32,
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    pub lk_type: i16,
}

pub type TiFsHash = Vec<u8>;

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

#[derive(Debug, Clone)]
pub struct DirectoryItem {
    pub ino: StorageIno,
    pub name: String,
    pub typ: StorageDirItemKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDirItem {
    pub ino: StorageIno,
    pub typ: StorageDirItemKind,
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct StorageFilePermission(pub u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub struct StorageFileAttr {
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
    /// time when this struct changed last time
    pub last_change: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccessTime(pub SystemTime);


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModificationTime(pub SystemTime);


/// These parameters do not change during the lifetime of an inode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InoDescription {
    pub ino: StorageIno,
    pub typ: StorageDirItemKind,
    /// Time of creation
    pub creation_time: SystemTime,
}

impl InoDescription {
    pub fn storage_ino(&self) -> StorageIno {
        self.ino
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InoSize {
    size: u64,
    blocks: u64,
    inline_data: Option<Vec<u8>>,
    pub data_hash: Option<TiFsHash>,
    pub last_change: SystemTime,
    pub change_iteration: u64,
}

impl InoSize {
    pub fn new() -> Self {
        Self {
            size: 0,
            blocks: 0,
            inline_data: None,
            data_hash: None,
            last_change: SystemTime::now(),
            change_iteration: 0,
        }
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
        if self.size != size {
            self.size = size;
            self.update_blocks(block_size);
            self.data_hash = None;
        }
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
}

/*
impl InoDescription {
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

impl From<InoDescription> for LockState {
    fn from(inode: InoDescription) -> Self {
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
*/
