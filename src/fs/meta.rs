use serde::{Deserialize, Serialize};

use super::error::{FsError, Result};
use super::key::ROOT_INODE;
use super::reply::StatFs;
use super::serialize::{deserialize, serialize, ENCODING};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MetaStatic{
    pub block_size: u64,
    pub hashed_blocks: bool, // TODO: convert to Option<AlgoName>
    pub hash_algorithm: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MetaMutable {
    pub inode_next: u64,
    pub last_stat: Option<StatFs>,
}

/*
delete strategy: 2 step sync:
1. step: check without lock
2. validate with lock
Means:
- manage a delete pending list
- after updating delete pending list, wait until all running processes have finished + some buffer?
- enter critical section, check again if block is not used, delete if still not used
- new processes check delete pending list, enter critical section if conflict, add use of block in critical section

- manage a iteration counter for the pending delete list.
- every writer periodically checks this pending delete list for changes and announces its own current state.
- this way, the deleter can check if any writer exists that wasn't yet updated.
- add a small? time delay for this check
- ensure that the time between read and announcement of the writers state if relatively short.
*/

impl MetaMutable {
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
