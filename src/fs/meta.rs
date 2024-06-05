use serde::{Deserialize, Serialize};

use super::error::{FsError, Result};
use super::key::ROOT_INODE;
use super::reply::StatFs;
use super::serialize::{deserialize, serialize, ENCODING};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MetaStatic{
    pub block_size: u64,
    pub hashed_blocks: bool,
    pub hash_algorithm: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Meta {
    pub inode_next: u64, // TODO: get rid of global counter
    pub last_stat: Option<StatFs>,
}

impl Meta {
    pub const fn new() -> Self {
        Self {
            inode_next: ROOT_INODE.0,
            last_stat: None,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|err| FsError::Serialize {
            target: "meta",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes).map_err(|err| FsError::Serialize {
            target: "meta",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }
}


impl MetaStatic {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|err| FsError::Serialize {
            target: "meta_static",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes).map_err(|err| FsError::Serialize {
            target: "meta_static",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }
}
