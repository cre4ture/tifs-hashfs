use std::{collections::{BTreeMap, HashMap, HashSet}, ops::Deref, sync::Arc};

use crate::fs::{fs_config::TiFsConfig, hash_fs_interface::BlockIndex, hashed_block::HashedBlock};
use crate::fs::inode::{StorageIno, TiFsHash};

use super::block_splitter::BlockIndexAndData;

pub struct UpdateIrregularBlock<'ol> {
    fs_config: TiFsConfig,
    ino: StorageIno,
    block_splitter_data: BlockIndexAndData<'ol>,
    block_splitter_data_start_position: usize,
    original_data_hash: Option<TiFsHash>,
}

impl<'ol> UpdateIrregularBlock<'ol> {
    pub fn get_and_add_original_block_hash(
        fs_config: TiFsConfig,
        ino: StorageIno,
        block_splitter_data: BlockIndexAndData<'ol>,
        block_splitter_data_start_position: usize,
        hash_list_prev: &BTreeMap<BlockIndex, TiFsHash>,
        pre_data_hash_request: &mut HashSet<TiFsHash>,
    ) -> Self {
        let first_data_hash = if block_splitter_data.data.len() > 0 {
            if let Some(hash) = hash_list_prev.get(&block_splitter_data.block_index) {
                pre_data_hash_request.insert(hash.clone());
                Some(hash)
            } else { None }
        } else { None };

        Self {
            fs_config,
            ino,
            block_splitter_data,
            block_splitter_data_start_position,
            original_data_hash: first_data_hash.map(|x|x.to_owned()),
        }
    }

    pub fn get_and_modify_block_and_publish_hash(
        &self,
        pre_data: &HashMap<TiFsHash, Arc<Vec<u8>>>,
        new_blocks: &mut HashMap<TiFsHash, Arc<Vec<u8>>>,
        new_block_hashes: &mut HashMap<BlockIndex, TiFsHash>,
    ) {
        if self.block_splitter_data.data.len() > 0 {
            let original = self.original_data_hash.as_ref().and_then(|h| pre_data.get(h));
            let mut modifiable = if let Some(orig) = original {
                orig.deref().clone()
            } else {
                Vec::new()
            };
            HashedBlock::update_data_range_in_vec(self.block_splitter_data_start_position, self.block_splitter_data.data, &mut modifiable);
            let new_hash = self.fs_config.calculate_hash(&modifiable);
            let _ = new_blocks.try_insert(new_hash.clone(), Arc::new(modifiable));
            new_block_hashes.insert(self.block_splitter_data.block_index, new_hash);
        }
    }
}
