use std::{collections::{HashMap, HashSet}, ops::{Deref, Range}, sync::Arc};


use futures::future::BoxFuture;
use tikv_client::{BoundRange, Key, KvPair};

use super::{error::FsError, hash_block::block_splitter::BlockSplitterRead, hashed_block::HashedBlock, inode::{self, BlockAddress, TiFsHash}, key::ScopedKeyBuilder, tikv_fs::{TiFsCaches, TiFsConfig}, utils::stop_watch::AutoStopWatch};

use crate::fs::error::Result;

pub async fn hb_get_block_hash_list_by_block_range<'fl, F>(
    key_builder: &ScopedKeyBuilder<'fl>,
    ino: u64,
    block_range: Range<u64>,
    scan_f: F,
) -> Result<HashMap<BlockAddress, inode::Hash>>
where
    F: FnOnce(BoundRange, u32) -> BoxFuture<'fl, tikv_client::Result<Vec<KvPair>>>,
{
    let range = key_builder.block_hash_range(ino, block_range.clone());
    let iter = scan_f(
            range.into(),
            block_range.count() as u32,
        )
        .await?;
    Ok(iter.into_iter().filter_map(|pair| {
        let Some(key) = key_builder.parse_key_block_address(pair.key().into()) else {
            tracing::error!("failed parsing block address from response 1");
            return None;
        };
        let Some(hash) = ScopedKeyBuilder::parse_hash_from_dyn_sized_array(pair.value()) else {
            tracing::error!("failed parsing hash value from response 2");
            return None;
        };
        Some((key, hash))
    }).collect())
}


pub fn hb_read_from_blocks(
    ino: u64, block_range: &Range<u64>, bs: &BlockSplitterRead,
    block_hashes: &HashMap<BlockAddress, TiFsHash>,
    blocks_data: &HashMap<TiFsHash, Arc<Vec<u8>>>) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    for block_index in block_range.clone() {
        let rel_index = block_index - block_range.start;

        let addr = BlockAddress{ino, index: block_index};
        let block_data = if let Some(block_hash) = block_hashes.get(&addr) {
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

pub async fn hb_get_block_data_by_hashes<'fl, F>(
    fs_config: &TiFsConfig,
    caches: &TiFsCaches, key_builder: &ScopedKeyBuilder<'fl>, hash_list: &HashSet<TiFsHash>,
    batch_get_f: F,
) -> Result<HashMap<inode::Hash, Arc<Vec<u8>>>>
where
    F: FnOnce(Vec<Key>) -> BoxFuture<'fl, tikv_client::Result<Vec<KvPair>>>,
{
    let mut watch = AutoStopWatch::start("get_hash_blocks_data");

    let mut result = HashMap::new();
    let mut keys = Vec::<Key>::new();
    for hash in hash_list {
        if let Some(cached) = caches.block.get(hash).await {
            result.insert(*hash, cached);
        } else {
            keys.push(key_builder.hashed_block(hash).into());
        }
    }

    watch.sync("cached");

    let mut uncached_blocks = HashMap::new();
    let rcv_data_list = batch_get_f(keys).await?;
    for pair in rcv_data_list {
        if let Some(hash) = key_builder.parse_key_hashed_block(pair.key().into()) {
            let value = Arc::new(pair.into_value());
            uncached_blocks.insert(hash, value.clone());
            caches.block.insert(hash, value).await;
        } else {
            tracing::error!("failed parsing hash from response!");
        }
    }

    watch.sync("fetch");

    if fs_config.validate_read_hashes {
        for (hash, value) in uncached_blocks.iter() {
            let actual_hash = HashedBlock::calculate_hash(&value);
            if hash != &actual_hash {
                return Err(FsError::ChecksumMismatch{hash: hash.clone(), actual_hash});
            }
        }

        watch.sync("validate");
    }

    result.extend(uncached_blocks.into_iter());

    watch.sync("done");

    Ok(result)
}
