
use std::ops::Range;
use std::{convert::TryInto, time::SystemTime};

use fuser::TimeOrNow;
use num_bigint::BigUint;
use tifs::fs::hash_fs_interface::BlockIndex;
use tifs::fs::{hash_fs_interface::HashFsError, key::PARENT_OF_ROOT_INODE, meta::MetaStatic};
use tifs::fs::inode::{InoAccessTime, InoDescription, InoSize, InoStorageFileAttr, ParentStorageIno, StorageDirItem, StorageDirItemKind, StorageFilePermission, StorageIno};
use uuid::Uuid;
use super::super::grpc;

impl From<StorageIno> for grpc::hash_fs::StorageIno {
    fn from(value: StorageIno) -> Self {
        Self { t: value.0 }
    }
}

impl From<grpc::hash_fs::StorageIno> for StorageIno {
    fn from(val: grpc::hash_fs::StorageIno) -> Self {
        StorageIno {
            0: val.t
        }
    }
}

impl From<ParentStorageIno> for grpc::hash_fs::ParentStorageIno {
    fn from(value: ParentStorageIno) -> Self {
        Self { t: Some(value.0.into()) }
    }
}

impl From<grpc::hash_fs::ParentStorageIno> for ParentStorageIno {
    fn from(val: grpc::hash_fs::ParentStorageIno) -> Self {
        ParentStorageIno(val.t.map(|v|v.into()).unwrap_or(
            PARENT_OF_ROOT_INODE.0
        ).into())
    }
}

impl From<&grpc::hash_fs::HashFsError> for HashFsError {
    fn from(val: &grpc::hash_fs::HashFsError) -> Self {
        use grpc::hash_fs::FsErrorId as gId;
        use HashFsError as nId;
        match val.id() {
            gId::FileAlreadyExists => nId::FileAlreadyExists,
            gId::FileNotFound => nId::FileNotFound,
            gId::FsHasInvalidData => nId::FsHasInvalidData(Some(val.msg.clone())),
            gId::FsNotInitialized => nId::FsNotInitialized,
            gId::InodeHasNoInlineData => nId::InodeHasNoInlineData,
            gId::RawGrpcStatus => nId::RawGrpcStatus(None),
            gId::Unspecific => nId::Unspecific(val.msg.clone()),
            gId::GrpcMessageIncomplete => nId::GrpcMessageIncomplete,
            gId::RawTonicTransportError => nId::RawTonicTransportError(val.msg.clone()),
            gId::FsDataIsMissing => nId::FsHasMissingData(Some(val.msg.clone())),
        }
    }
}
impl From<grpc::hash_fs::HashFsError> for HashFsError {
    fn from(value: grpc::hash_fs::HashFsError) -> Self {
        (&value).into()
    }
}

impl From<&HashFsError> for grpc::hash_fs::HashFsError {
    fn from(val: &HashFsError) -> Self {
        use grpc::hash_fs::FsErrorId as nId;
        use HashFsError as gId;
        let mut msg_out = String::new();
        let id = match val {
            gId::FileAlreadyExists => nId::FileAlreadyExists,
            gId::FileNotFound => nId::FileNotFound,
            gId::FsHasInvalidData(msg) => {
                msg_out = msg.clone().unwrap_or(format!(""));
                nId::FsHasInvalidData
            }
            gId::FsNotInitialized => nId::FsNotInitialized,
            gId::InodeHasNoInlineData => nId::InodeHasNoInlineData,
            gId::RawGrpcStatus(status) => {
                msg_out = format!("status: {:?}", status);
                nId::RawGrpcStatus
            }
            gId::Unspecific(msg) => {
                msg_out = msg.clone();
                nId::Unspecific
            }
            gId::GrpcMessageIncomplete => nId::GrpcMessageIncomplete,
            gId::RawTonicTransportError(msg) => {
                msg_out = msg.clone();
                nId::RawTonicTransportError
            }
            gId::FsHasMissingData(msg) => {
                msg_out = msg.clone().unwrap_or(format!(""));
                nId::FsDataIsMissing
            }
        };
        let mut o = grpc::hash_fs::HashFsError::default();
        o.set_id(id);
        o.msg = msg_out;
        o
    }
}

impl From<HashFsError> for grpc::hash_fs::HashFsError {
    fn from(value: HashFsError) -> Self {
        (&value).into()
    }
}

impl From<&grpc::hash_fs::StorageDirItemKind> for StorageDirItemKind {
    fn from(val: &grpc::hash_fs::StorageDirItemKind) -> Self {
        use grpc::hash_fs::StorageDirItemKind as gId;
        use StorageDirItemKind as nId;
        match val {
            gId::File => nId::File,
            gId::Directory => nId::Directory,
            gId::Symlink => nId::Symlink,
        }
    }
}

impl From<grpc::hash_fs::StorageDirItemKind> for StorageDirItemKind {
    fn from(val: grpc::hash_fs::StorageDirItemKind) -> Self {
        (&val).into()
    }
}

impl From<StorageDirItemKind> for grpc::hash_fs::StorageDirItemKind {
    fn from(val: StorageDirItemKind) -> Self {
        use grpc::hash_fs::StorageDirItemKind as nId;
        use StorageDirItemKind as gId;
        match val {
            gId::File => nId::File,
            gId::Directory => nId::Directory,
            gId::Symlink => nId::Symlink,
        }
    }
}

impl From<grpc::hash_fs::StorageDirItem> for StorageDirItem {
    fn from(val: grpc::hash_fs::StorageDirItem) -> Self {
        Self {
            typ: val.typ().into(),
            ino: val.ino.map(|n|n.into())
                .unwrap_or(PARENT_OF_ROOT_INODE.0),
        }
    }
}

impl From<StorageDirItem> for grpc::hash_fs::StorageDirItem {
    fn from(val: StorageDirItem) -> Self {
        let mut o = Self::default();
        o.set_typ(val.typ.into());
        o.ino = Some(val.ino.into());
        o
    }
}

impl From<grpc::hash_fs::MetaStatic> for MetaStatic {
    fn from(val: grpc::hash_fs::MetaStatic) -> Self {
        MetaStatic{
            block_size: val.block_size,
            hashed_blocks: val.hashed_blocks,
            hash_algorithm: val.hash_algorithm,
        }
    }
}

impl From<MetaStatic> for grpc::hash_fs::MetaStatic {
    fn from(val: MetaStatic) -> Self {
        let mut o = Self::default();
        o.block_size = val.block_size;
        o.hashed_blocks = val.hashed_blocks;
        o.hash_algorithm = val.hash_algorithm;
        o
    }
}

pub fn grpc_time_to_system_time(timestamp: prost_types::Timestamp
) -> SystemTime {
    timestamp.try_into().ok().unwrap_or(SystemTime::UNIX_EPOCH)
}

pub fn grpc_time_opt_to_system_time(timestamp_opt: &Option<prost_types::Timestamp>
) -> SystemTime {
    let time = timestamp_opt.clone()
        .and_then(|t| -> Option<SystemTime> {
            t.clone().try_into().ok()
        })
        .unwrap_or(SystemTime::UNIX_EPOCH);
    time
}

impl From<grpc::hash_fs::InoAccessTime> for InoAccessTime {
    fn from(val: grpc::hash_fs::InoAccessTime) -> Self {
        InoAccessTime(grpc_time_opt_to_system_time(&val.atime))
    }
}

impl From<InoAccessTime> for grpc::hash_fs::InoAccessTime {
    fn from(val: InoAccessTime) -> Self {
        Self{atime: Some(val.0.into())}
    }
}

impl From<grpc::hash_fs::InoDescription> for InoDescription {
    fn from(val: grpc::hash_fs::InoDescription) -> Self {
        InoDescription{
            ino: val.ino.as_ref().map(|v|v.clone().into())
                .unwrap_or(PARENT_OF_ROOT_INODE.0),
            typ: val.typ().into(),
            creation_time: grpc_time_opt_to_system_time(
                &val.creation_time),
        }
    }
}

impl From<InoDescription> for grpc::hash_fs::InoDescription {
    fn from(val: InoDescription) -> Self {
        let mut o = grpc::hash_fs::InoDescription::default();
        o.ino = Some(val.ino.into());
        o.set_typ(val.typ.into());
        o.creation_time = Some(val.creation_time.into());
        o
    }
}

impl From<grpc::hash_fs::StorageFilePermission> for StorageFilePermission {
    fn from(value: grpc::hash_fs::StorageFilePermission) -> Self {
        StorageFilePermission((value.t & 0xFFFF) as u16)
    }
}

impl From<StorageFilePermission> for grpc::hash_fs::StorageFilePermission {
    fn from(val: StorageFilePermission) -> Self {
        grpc::hash_fs::StorageFilePermission{
            t: val.0 as u32,
        }
    }
}

impl From<grpc::hash_fs::InoStorageFileAttr> for InoStorageFileAttr {
    fn from(val: grpc::hash_fs::InoStorageFileAttr) -> Self {
        InoStorageFileAttr{
            perm: val.perm.map(|v|v.into())
                .unwrap_or(StorageFilePermission(0o000)),
            uid: val.uid,
            gid: val.gid,
            rdev: val.rdev,
            flags: val.flags,
            last_change: grpc_time_opt_to_system_time(
                &val.last_change),
        }
    }
}

impl From<InoStorageFileAttr> for grpc::hash_fs::InoStorageFileAttr {
    fn from(val: InoStorageFileAttr) -> Self {
        let mut o = grpc::hash_fs::InoStorageFileAttr::default();
        o.perm = Some(val.perm.into());
        o.uid = val.uid;
        o.gid = val.gid;
        o.rdev = val.rdev;
        o.flags = val.flags;
        o.last_change = Some(val.last_change.into());
        o
    }
}

impl From<grpc::hash_fs::InoSize> for InoSize {
    fn from(val: grpc::hash_fs::InoSize) -> Self {
        InoSize{
            size: val.size,
            blocks: val.blocks,
            last_change: grpc_time_opt_to_system_time(
                &val.last_change),
        }
    }
}

impl From<InoSize> for grpc::hash_fs::InoSize {
    fn from(val: InoSize) -> Self {
        let mut o = Self::default();
        o.size = val.size();
        o.blocks = val.blocks();
        o.last_change = Some(val.last_change.into());
        o
    }
}

impl From<grpc::hash_fs::TimeOrNow> for TimeOrNow {
    fn from(value: grpc::hash_fs::TimeOrNow) -> Self {
        if let Some(time) = value.time {
            TimeOrNow::SpecificTime(grpc_time_opt_to_system_time(
                &Some(time)
            ))
        } else {
            TimeOrNow::Now
        }
    }
}

impl From<TimeOrNow> for grpc::hash_fs::TimeOrNow {
    fn from(val: TimeOrNow) -> Self {
        let time_opt: Option<prost_types::Timestamp> = match  val {
            TimeOrNow::SpecificTime(time) => Some(time.into()),
            TimeOrNow::Now => None,
        };
        Self { time: time_opt }
    }
}


impl From<grpc::hash_fs::Uuid> for Uuid {
    fn from(value: grpc::hash_fs::Uuid) -> Self {
        Uuid::parse_str(&value.t)
            .unwrap_or(Uuid::nil())
    }
}

impl From<Uuid> for grpc::hash_fs::Uuid {
    fn from(val: Uuid) -> Self {
        Self {
            t: val.to_string(),
        }
    }
}

impl From<grpc::hash_fs::BlockIndex> for BlockIndex {
    fn from(value: grpc::hash_fs::BlockIndex) -> Self {
        Self(value.t)
    }
}

impl From<BlockIndex> for grpc::hash_fs::BlockIndex {
    fn from(val: BlockIndex) -> Self {
        Self {
            t: val.0,
        }
    }
}

impl From<grpc::hash_fs::BlockRange> for Range<BlockIndex> {
    fn from(value: grpc::hash_fs::BlockRange) -> Self {
        Self {
            start: value.start.map(Into::into).unwrap_or(BlockIndex(0)),
            end: value.end.map(Into::into).unwrap_or(BlockIndex(0)),
        }
    }
}

impl From<Range<BlockIndex>> for grpc::hash_fs::BlockRange {
    fn from(val: Range<BlockIndex>) -> Self {
        Self {
            start: Some(val.start.into()),
            end: Some(val.end.into()),
        }
    }
}


impl From<grpc::hash_fs::BigUint> for BigUint {
    fn from(value: grpc::hash_fs::BigUint) -> Self {
        Self::from_bytes_be(&value.big_endian_value)
    }
}

impl From<BigUint> for grpc::hash_fs::BigUint {
    fn from(val: BigUint) -> Self {
        Self {
            big_endian_value: val.to_bytes_be(),
        }
    }
}
