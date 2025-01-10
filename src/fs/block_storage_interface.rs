
use std::{collections::{HashMap, HashSet}, fmt::Debug};

use super::hash_fs_interface::{HashFsData, HashFsResult};
use super::inode::TiFsHash;

#[async_trait::async_trait]
pub trait BlockStorageInterface: Send + Sync  {
    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, HashFsData>>;
    async fn hb_upload_new_blocks(
        &self,
        blocks: &[(&TiFsHash, HashFsData)],
    ) -> HashFsResult<()>;
    async fn hb_upload_new_block_by_composition(
        &self,
        original_hash: &TiFsHash,
        new_hash: &TiFsHash,
        new_block_data_offset: u64,
        new_block_data: HashFsData,
    ) -> HashFsResult<()>;
}

impl Debug for dyn BlockStorageInterface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockStorageI")
    }
}
