use core::clone::Clone;
use core::convert::{From, Into};
use core::iter::Extend;
use core::unimplemented;
use std::collections::{HashMap, HashSet};

use super::hash_fs_interface::{HashFsData, HashFsError};
use super::utils::stop_watch::AutoStopWatch;
use super::{block_storage_interface::BlockStorageInterface, hash_fs_interface::HashFsResult, inode::TiFsHash};

use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::Client;
use futures::TryFutureExt;

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

pub fn from_bs_error(value: aws_sdk_s3::primitives::ByteStreamError) -> HashFsError {
    HashFsError::Unspecific(format!(
        "ByteStreamError: {}", value
    ))
}

pub struct S3BasedBlockStorage {
    client: Client,
    bucket_name: String,
    prefix_path: String,
}

impl S3BasedBlockStorage {
    pub fn new(
        client: Client,
        bucket_name: String,
        prefix_path: String,
    ) -> Self {
        return Self {
            client,
            bucket_name,
            prefix_path,
        }
    }

    pub fn get_hash_str_representation(&self, hash: &TiFsHash) -> String {
        let hex_str = hex::encode(hash);
        return hex_str[0..3].to_string() + "/" + &hex_str[3..6] + "/" + &hex_str[6..];
    }

    pub fn get_hash_block_location(&self, hash: &TiFsHash) -> String {
        return self.prefix_path.clone() + &self.get_hash_str_representation(hash);
    }

    pub async fn get_block_data(&self, b_hash: &TiFsHash) -> HashFsResult<bytes::Bytes> {
        let hex_hash = self.get_hash_block_location(&b_hash);
        let mut object = self.client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(self.prefix_path.clone() + &hex_hash)
            .send()
            .map_err(from_s3_error)
            .await?;
        let mut data = bytes::BytesMut::with_capacity(64 << 10);
        while let Some(bytes) = object.body.try_next().map_err(from_bs_error).await? {
            data.extend(bytes);
        }
        Ok(data.freeze())
    }
}

#[async_trait::async_trait]
impl BlockStorageInterface for S3BasedBlockStorage {

    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, HashFsData>>{
        let mut watch = AutoStopWatch::start("hb_get_block_data_by_hashes-s3");
        let r = futures::future::join_all(hashes.iter().map(async |b_hash| -> HashFsResult<(TiFsHash, HashFsData)> {
            Ok(((*b_hash).clone(), self.get_block_data(b_hash).await?))
        }).collect::<Vec<_>>()).await;

        watch.sync("download");

        let mut result = HashMap::new();
        for rs in r.into_iter() {
            let (hash, data) = rs?;
            result.insert(hash, data);
        }
        return Ok(result)
    }

    async fn hb_upload_new_blocks(
        &self,
        blocks: &[(&TiFsHash, HashFsData)],
    ) -> HashFsResult<()> {
        let mut watch = AutoStopWatch::start("hb_upload_new_blocks-s3");
        let r = futures::future::join_all(blocks.iter().map(|(b_hash, b_data)| {
            let hex_hash = self.get_hash_block_location(b_hash);
            let stream = ByteStream::new(SdkBody::from(b_data.clone()));
            self.client
                .put_object()
                .bucket(self.bucket_name.clone())
                .key(hex_hash)
                .body(stream)
                .send()
                .map_err(from_s3_error)
        }).collect::<Vec<_>>()).await;
        watch.sync("upload");
        for rs in r.into_iter() {
            rs?;
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

    async fn hb_delete_blocks(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<()> {
        let mut watch = AutoStopWatch::start("hb_delete_blocks-s3");
        let r = futures::future::join_all(hashes.iter().map(|b_hash| {
            let hex_hash = self.get_hash_block_location(b_hash);
            self.client
                .delete_object()
                .bucket(self.bucket_name.clone())
                .key(hex_hash)
                .send()
                .map_err(from_s3_error)
        }).collect::<Vec<_>>()).await;
        watch.sync("delete");
        for rs in r.into_iter() {
            rs?;
        }
        Ok(())
    }

}
