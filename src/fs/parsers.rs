use std::{collections::{BTreeMap, HashMap}, ops::Deref, sync::Arc};


use super::{hash_block::block_splitter::BlockSplitterRead, hash_fs_interface::BlockIndex, inode::{StorageIno, TiFsHash}};

use crate::fs::error::Result;


pub fn hb_read_from_blocks(
    ino: StorageIno,
    bs: &BlockSplitterRead,
    block_hashes: &BTreeMap<BlockIndex, TiFsHash>,
    blocks_data: &HashMap<TiFsHash, Arc<Vec<u8>>>
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(bs.block_size as usize * block_hashes.len());
    for block_index in bs.first_block_index..bs.end_block_index {
        let rel_index = block_index - bs.first_block_index;

        let block_data = if let Some(block_hash) = block_hashes.get(&block_index) {
            if let Some(data) = blocks_data.get(block_hash) {
                data.deref()
            } else { &vec![] }
        } else { &vec![] };

        let (rd_start, rd_size) = match rel_index {
            0 => (bs.first_block_read_offset as usize, bs.bytes_to_read_first_block as usize),
            _ => (0, (bs.size as usize - result.len()).min(bs.block_size as usize)),
        };

        if rd_start < block_data.len() {
            // do a copy, as some blocks might be used multiple times
            let rd_end = (rd_start + rd_size).min(block_data.len());
            //eprintln!("extend (result.len(): {}) from slice ({block_index}): {rd_start}..{rd_end}", result.len());
            result.extend_from_slice(&block_data[rd_start..rd_end]);
        }
    }
    Ok(result)
}
