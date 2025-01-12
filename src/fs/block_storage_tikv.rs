use tikv_client::{Key, KvPair};

use super::mini_transaction::{mutation_delete, mutation_put};
use super::{block_storage_interface::BlockStorageInterface, flexible_transaction::FlexibleTransaction, inode::TiFsHash};
use super::hash_fs_interface::{HashFsData, HashFsResult};
use super::fs_config::TiFsConfig;
use std::collections::{HashSet, HashMap};


pub struct TikvBasedBlockStorage {
    fs_config: TiFsConfig,
    f_txn: FlexibleTransaction,
}

impl TikvBasedBlockStorage {
    pub fn new(fs_config: TiFsConfig, f_txn: FlexibleTransaction) -> Self {
        return Self{
            f_txn,
            fs_config,
        }
    }
}

#[async_trait::async_trait]
impl BlockStorageInterface for TikvBasedBlockStorage {
    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, HashFsData>> {
        let mut keys = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let key = self.fs_config.key_builder().hashed_block(*hash);
            keys.push(Key::from(key));
        }
        let rcv_data_list = self.f_txn.batch_get(keys).await?;
        let mut hashed_block_data = HashMap::with_capacity(rcv_data_list.len());
        for KvPair(k, v) in rcv_data_list {
            let hash = self.fs_config.key_parser_b(k)?.parse_key_hashed_block()?;
            let value: HashFsData = v.into();
            hashed_block_data.insert(hash.clone(), value.clone());
        }
        Ok(hashed_block_data)
    }

    async fn hb_upload_new_blocks(&self, blocks: &[(&TiFsHash, HashFsData)]) -> HashFsResult<()> {
        let mutations = blocks.iter().map(|(h,d)|{
            let key = self.fs_config.key_builder().hashed_block(h);
            mutation_put(key, d.to_vec())
        }).collect::<Vec<_>>();

        self.f_txn.batch_mutate(mutations).await?;
        Ok(())
    }

    async fn hb_upload_new_block_by_composition(
        &self,
        _original_hash: &TiFsHash,
        _new_hash: &TiFsHash,
        _new_block_data_offset: u64,
        _new_block_data: HashFsData,
    ) -> HashFsResult<()> {
        unimplemented!()
    }

    async fn hb_delete_blocks(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<()> {
        let mutations = hashes.iter().map(|h|{
            mutation_delete(Key::from(self.fs_config.key_builder().hashed_block(h)))
        }).collect::<Vec<_>>();
        self.f_txn.batch_mutate(mutations).await?;
        Ok(())
    }
}
