
use std::{collections::{BTreeMap, HashMap, HashSet}, ops::Range, sync::Arc, time::SystemTime};

use bytestring::ByteString;
use fuser::TimeOrNow;
use num_bigint::BigUint;
use uuid::Uuid;

use super::{inode::{ParentStorageIno, StorageDirItem}, meta::MetaStatic};
use super::inode::{InoAccessTime, DirectoryItem, InoDescription, InoSize, StorageDirItemKind, InoStorageFileAttr, StorageFilePermission, StorageIno, TiFsHash};

#[derive(Debug)]
pub enum HashFsError {
    Unspecific(String),
    RawGrpcStatus(Option<tonic::Status>),
    FsNotInitialized,
    FsHasInvalidData(Option<String>),
    FileNotFound,
    FileAlreadyExists,
    InodeHasNoInlineData,
    GrpcMessageIncomplete,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
     Hash, derive_more::Add, derive_more::Sub, derive_more::Display
    )]
pub struct BlockIndex(pub u64);

impl std::iter::Step for BlockIndex {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        u64::steps_between(&start.0, &end.0)
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        u64::forward_checked(start.0, count).map(BlockIndex)
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        u64::backward_checked(start.0, count).map(BlockIndex)
    }
}

pub type HashFsResult<V> = Result<V, HashFsError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GotOrMadePure {
    ExistedAlready,
    NewlyCreated
}

pub enum GotOrMade<R> {
    ExistedAlready(R),
    NewlyCreated(R),
}

impl<R> GotOrMade<R> {
    pub fn was_made(&self) -> bool {
        if let Self::NewlyCreated(_r) = self {
            true
        } else {
            false
        }
    }

    pub fn existed_before(&self) -> bool {
        !self.was_made()
    }

    pub fn value(self) -> R {
        match self {
            GotOrMade::ExistedAlready(r) => r,
            GotOrMade::NewlyCreated(r) => r,
        }
    }

    pub fn unpack(self) -> (GotOrMadePure, R) {
        match self {
            GotOrMade::ExistedAlready(r) => (GotOrMadePure::ExistedAlready, r),
            GotOrMade::NewlyCreated(r) => (GotOrMadePure::NewlyCreated, r),
        }
    }
}

#[async_trait::async_trait]
pub trait HashFsInterface: Send + Sync  {
    async fn init(&self, gid: u32, uid: u32) -> HashFsResult<StorageDirItem>;
    async fn meta_static_read(&self) -> HashFsResult<MetaStatic>;
    async fn directory_read_children(&self, dir_ino: StorageIno) -> HashFsResult<Vec<DirectoryItem>>;
    async fn directory_add_child_checked_new_inode(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        typ: StorageDirItemKind,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
        rdev: u32,
        inline_data: Option<Vec<u8>>,
    ) -> HashFsResult<GotOrMade<StorageDirItem>>;
    async fn directory_add_child_checked_existing_inode(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        ino: StorageIno,
    ) -> HashFsResult<()>;
    async fn directory_remove_child_file(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()>;
    async fn directory_remove_child_directory(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()>;
    async fn directory_rename_child(
        &self,
        parent: ParentStorageIno,
        child_name: ByteString,
        new_parent: ParentStorageIno,
        new_child_name: ByteString,
    ) -> HashFsResult<()>;
    async fn directory_child_get_all_attributes(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)>;
    async fn directory_add_new_symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: ParentStorageIno,
        name: ByteString,
        link: ByteString,
    ) -> HashFsResult<StorageDirItem>;
    async fn inode_get_all_attributes(
        &self,
        ino: StorageIno,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)>;
    async fn inode_set_all_attributes(
        &self,
        ino: StorageIno,
        mode: Option<StorageFilePermission>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> HashFsResult<()>;
    async fn inode_open(&self, ino: StorageIno) -> HashFsResult<Uuid>;
    async fn inode_close(&self, ino: StorageIno, use_id: Uuid) -> HashFsResult<()>;
    async fn inode_allocate_size(
        &self,
        ino: StorageIno,
        offset: i64,
        length: i64,
    ) -> HashFsResult<()>;
    async fn inode_read_inline_data(
        &self,
        ino: StorageIno
    ) -> HashFsResult<Vec<u8>>;
    async fn inode_read_block_hashes_data_range(
        &self,
        ino: StorageIno,
        start: u64,
        read_size: u64,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>>;
    async fn inode_read_block_hashes_block_range(
        &self,
        ino: StorageIno,
        block_range: Range<BlockIndex>,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>>;
    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, Arc<Vec<u8>>>>;
    async fn file_get_hash(&self, ino: StorageIno) -> HashFsResult<Vec<u8>>;
    async fn file_read_block_hashes(
        &self,
        ino: StorageIno,
        block_range: Range<BlockIndex>
    ) -> HashFsResult<Vec<u8>>;
    // Increments the reference counter by provided delta and returns previous counter value.
    // Also succeeds if block didn't exist before. Returns previous counter value 0 in this case.
    async fn hb_increment_reference_count(
        &self,
        hash: &TiFsHash,
        cnt: u64
    ) -> HashFsResult<BigUint>;
    // This uploads data for a new block. Will not do any reference count changes.
    // Will also succeed when the block already existed.
    async fn hb_upload_new_block(
        &self,
        block_hash: TiFsHash,
        data: Vec<u8>
    ) -> HashFsResult<()>;
    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        ino: StorageIno,
        block_hash: TiFsHash,
        blocks_size: u64,
        block_ids: Vec<BlockIndex>,
    ) -> HashFsResult<()>;
}

impl From<tonic::Status> for HashFsError {
    fn from(value: tonic::Status) -> Self {
        Self::RawGrpcStatus(Some(value))
    }
}
