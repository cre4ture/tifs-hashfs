use serde::{Deserialize, Serialize};

use super::error::{FsError, Result};
use super::key::ROOT_INODE;
use super::reply::StatFs;
use super::serialize::{deserialize, serialize, ENCODING};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct StaticFsParameters{
    pub hashed_blocks: bool,
}

impl StaticFsParameters {
    pub const fn new() -> Self {
        Self {
            hashed_blocks: false,
        }
    }
}

impl Default for StaticFsParameters {
    fn default() -> Self {
        Self {
            hashed_blocks: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Meta {
    pub inode_next: u64, // TODO: get rid of global counter
    pub block_size: u64,
    pub last_stat: Option<StatFs>,
    pub config_flags: Option<StaticFsParameters>,
}

impl Meta {
    pub const fn new(block_size: u64, config: StaticFsParameters) -> Self {
        Self {
            inode_next: ROOT_INODE,
            block_size,
            last_stat: None,
            config_flags: Some(config),
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
