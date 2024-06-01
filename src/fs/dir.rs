use super::error::{FsError, Result};
use super::inode::StorageDirItem;
use super::serialize::{deserialize, serialize, ENCODING};

pub type StorageDirectory = Vec<StorageDirItem>;

pub fn encode(dir: &[StorageDirItem]) -> Result<Vec<u8>> {
    serialize(dir).map_err(|err| FsError::Serialize {
        target: "directory",
        typ: ENCODING,
        msg: err.to_string(),
    })
}

pub fn decode(bytes: &[u8]) -> Result<StorageDirectory> {
    deserialize(bytes).map_err(|err| FsError::Serialize {
        target: "directory",
        typ: ENCODING,
        msg: err.to_string(),
    })
}

pub fn encode_item(item: &StorageDirItem) -> Result<Vec<u8>> {
    serialize(item).map_err(|err| FsError::Serialize {
        target: "dir item",
        typ: ENCODING,
        msg: err.to_string(),
    })
}

pub fn decode_item(bytes: &[u8]) -> Result<StorageDirItem> {
    deserialize(bytes).map_err(|err| FsError::Serialize {
        target: "dir item",
        typ: ENCODING,
        msg: err.to_string(),
    })
}
