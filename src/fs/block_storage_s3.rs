use core::clone::Clone;
use core::convert::From;
use core::unimplemented;
use std::collections::{HashMap, HashSet};

use super::hash_fs_interface::{HashFsData, HashFsError};
use super::{block_storage_interface::BlockStorageInterface, hash_fs_interface::HashFsResult, inode::TiFsHash};

use aws_sdk_s3::{config::Region, Client};

pub fn from_s3_error<T: aws_sdk_s3::error::ProvideErrorMetadata>(value: T) -> HashFsError {
        HashFsError::Unspecific(format!(
            "{}: {}",
            value
                .code()
                .map(String::from)
                .unwrap_or("unknown code".into()),
            value
                .message()
                .map(String::from)
                .unwrap_or("missing reason".into()),
        ))
}

pub struct S3BasedBlockStorage {
    region: Region,
    client: Client,
    bucket_name: String,
}

impl S3BasedBlockStorage {
    pub fn new(
        region: Region,
        client: Client,
        bucket_name: String,
    ) -> Self {
        return Self {
            region,
            client,
            bucket_name,
        }
    }
}

#[async_trait::async_trait]
impl BlockStorageInterface for S3BasedBlockStorage {
    async fn hb_get_block_data_by_hashes(
        &self,
        _hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, HashFsData>>{
        unimplemented!()
    }
    async fn hb_upload_new_blocks(
        &self,
        blocks: &[(&TiFsHash, HashFsData)],
    ) -> HashFsResult<()> {
        for (b_hash, b_data) in blocks {
            let hex_hash = hex::encode(b_hash);
            self.client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(hex_hash)
            .body(aws_sdk_s3::primitives::ByteStream::from(b_data.clone()))
            .send()
            .await
            .map_err(from_s3_error)?;
        }

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
}
