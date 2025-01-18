use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use bytestring::ByteString;
use fuser::TimeOrNow;
use num_bigint::BigUint;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::grpc::hash_fs::{self as grpc_fs, InitRq, MetaStaticReadRq};
use crate::utils::object_pool::{HandedOutPoolElement, Pool};
use tifs::fs::hash_fs_interface::{BlockIndex, GotOrMade, HashFsData, HashFsError, HashFsInterface, HashFsResult};
use tifs::fs::inode::{DirectoryItem, InoAccessTime, InoDescription, InoSize, InoStorageFileAttr, ParentStorageIno, StorageDirItem, StorageDirItemKind, StorageFilePermission, StorageIno, TiFsHash};
use tifs::fs::meta::MetaStatic;
use tokio::time::sleep;

type RawGrpcClient = grpc_fs::hash_fs_client::HashFsClient<tonic::transport::Channel>;

fn handle_error(err_opt: &Option<grpc_fs::HashFsError>) -> HashFsResult<()> {
    if let Some(err) = err_opt {
        return Err(err.into());
    }
    Ok(())
}

fn parse_all_attrs(
    all_opt: Option<grpc_fs::InoAllAttributes>
) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
    let Some(mut all) = all_opt else {
        tracing::debug!("all missing");
        return Err(HashFsError::GrpcMessageIncomplete);
    };
    let Some(desc) = mem::take(&mut all.desc) else {
        tracing::debug!("desc missing");
        return Err(HashFsError::GrpcMessageIncomplete);
    };
    let Some(attr) = mem::take(&mut all.attrs) else {
        tracing::debug!("attrs missing");
        return Err(HashFsError::GrpcMessageIncomplete);
    };
    let Some(size) = mem::take(&mut all.size) else {
        tracing::debug!("size missing");
        return Err(HashFsError::GrpcMessageIncomplete);
    };
    let Some(atime) = mem::take(&mut all.atime) else {
        tracing::debug!("atime missing");
        return Err(HashFsError::GrpcMessageIncomplete);
    };
    Ok((
        Arc::new(desc.into()),
        Arc::new(attr.into()),
        Arc::new(size.into()),
        atime.into(),
    ))
}

pub struct HashFsClient {
    grpc_endpoint: tonic::transport::Endpoint,
    grpc_client_pool: Pool<RawGrpcClient>,
}

impl HashFsClient {
    pub fn new(grpc_endpoint: tonic::transport::Endpoint) -> Self {
        Self {
            grpc_endpoint: grpc_endpoint.clone(),
            grpc_client_pool: Pool::new(),
        }
    }

    async fn connect_with_retries(grpc_endpoint: tonic::transport::Endpoint) -> core::result::Result<RawGrpcClient, tonic::transport::Error> {
        let start_time = std::time::Instant::now();
        loop {
            let hash_fs_client = RawGrpcClient::connect(grpc_endpoint.clone()).await;
            match hash_fs_client {
                Ok(client) => return Ok(client),
                Err(err) => {
                    let time_passed = start_time.elapsed();
                    if time_passed > std::time::Duration::from_mins(10) {
                        tracing::error!("failed to connect to grpc sever. timeout passed");
                        return  Err(err);
                    }
                    tracing::warn!("failed to connect to grpc sever. trying again ...");
                    sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn lock_grpc<'pool>(&'pool self) -> core::result::Result<HandedOutPoolElement<'pool, RawGrpcClient>, tonic::transport::Error> {
        let next = self.grpc_client_pool.get_one_free();
        if let Some(next) = next {
            return Ok(next);
        }

        let new_one = Self::connect_with_retries(self.grpc_endpoint.clone()).await?;
        Ok(self.grpc_client_pool.add_new_busy_to_pool(new_one))
    }
}

#[tonic::async_trait]
impl HashFsInterface for HashFsClient {

    async fn init(&self, gid: u32, uid: u32) -> HashFsResult<StorageDirItem> {
        let rs = self.lock_grpc().await?
            .init(InitRq{gid, uid}).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(v) = rs.value else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(v.into())
    }

    async fn meta_static_read(&self) -> HashFsResult<MetaStatic> {
        let rs = self.lock_grpc().await?
            .meta_static_read(MetaStaticReadRq{}).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(v) = rs.value else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(v.into())
    }

    async fn directory_read_children(&self, dir_ino: StorageIno) -> HashFsResult<Vec<DirectoryItem>> {
        let rs = self.lock_grpc().await?
        .directory_read_children(grpc_fs::DirectoryReadChildrenRq{
            dir_ino: Some(dir_ino.into()),
        }).await?;
        Ok(rs.into_inner().value.into_iter().filter_map(|item|{
            Some(DirectoryItem{
                ino: item.ino.clone()?.into(),
                typ: item.typ().into(),
                name: item.name,
            })
        }).collect::<Vec<_>>())
    }

    async fn directory_add_child_checked_existing_inode(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        ino: StorageIno,
    ) -> HashFsResult<()> {
        let rs = self.lock_grpc().await?
            .directory_add_child_checked_existing_inode(
                grpc_fs::DirectoryAddChildCheckedExistingInodeRq{
                    ino: Some(ino.into()),
                    name: name.into(),
                    parent: Some(parent.into()),
            }).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn directory_add_child_checked_new_inode(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        typ: StorageDirItemKind,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
        rdev: u32,
        inline_data: Option<Vec<u8>>,
    ) -> HashFsResult<GotOrMade<StorageDirItem>> {
        let mut rq = grpc_fs::DirectoryAddChildCheckedNewInodeRq
            ::default();
        rq.parent = Some(parent.into());
        rq.name = name.to_string();
        rq.set_typ(typ.into());
        rq.perm = Some(grpc_fs::StorageFilePermission { t: perm.0 as u32 });
        rq.gid = gid;
        rq.uid = uid;
        rq.rdev = rdev;
        if let Some(data) = inline_data {
            rq.inline_data = data;
        }
        let rs = self.lock_grpc().await?
            .directory_add_child_checked_new_inode(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let existed_before = rs.existed_already;
        let Some(item) = rs.item else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        if existed_before {
            Ok(GotOrMade::ExistedAlready(item.into()))
        } else {
            Ok(GotOrMade::NewlyCreated(item.into()))
        }
    }

    async fn directory_remove_child_file(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::DirectoryRemoveChildFileRq::default();
        rq.parent = Some(parent.into());
        rq.name = name.to_string();
        let rs = self.lock_grpc().await?
            .directory_remove_child_file(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn directory_remove_child_directory(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::DirectoryRemoveChildDirectoryRq::default();
        rq.parent = Some(parent.into());
        rq.name = name.to_string();
        let rs = self.lock_grpc().await?
            .directory_remove_child_directory(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn directory_rename_child(
        &self,
        parent: ParentStorageIno,
        child_name: ByteString,
        new_parent: ParentStorageIno,
        new_child_name: ByteString,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::DirectoryRenameChildRq::default();
        rq.parent = Some(parent.into());
        rq.child_name = child_name.to_string();
        rq.new_parent =  Some(new_parent.into());
        rq.new_child_name = new_child_name.to_string();
        let rs = self.lock_grpc().await?
            .directory_rename_child(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn directory_child_get_all_attributes(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
        let mut rq = grpc_fs::DirectoryChildGetAllAttributesRq::default();
        rq.parent = Some(parent.into());
        rq.name = name.to_string();
        let mut rs = self.lock_grpc().await?
            .directory_child_get_all_attributes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        parse_all_attrs(mem::take(&mut rs.all))
    }

    async fn directory_add_new_symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: ParentStorageIno,
        name: ByteString,
        link: ByteString,
    ) -> HashFsResult<StorageDirItem> {
        let mut rq = grpc_fs::DirectoryAddNewSymlinkRq::default();
        rq.parent = Some(parent.into());
        rq.name = name.to_string();
        rq.link = link.to_string();
        rq.gid = gid;
        rq.uid = uid;
        let rs = self.lock_grpc().await?
            .directory_add_new_symlink(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(item) = rs.item else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(item.into())
    }

    async fn inode_get_all_attributes(
        &self,
        ino: StorageIno,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
        let mut rq = grpc_fs::InodeGetAllAttributesRq::default();
        rq.ino = Some(ino.into());
        let mut rs = self.lock_grpc().await?
            .inode_get_all_attributes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        parse_all_attrs(mem::take(&mut rs.all))
    }

    async fn inode_set_all_attributes(
        &self,
        ino: StorageIno,
        mode: Option<StorageFilePermission>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::InodeSetAllAttributesRq::default();
        rq.ino = Some(ino.into());
        rq.mode = mode.map(Into::into);
        rq.uid = uid;
        rq.gid = gid;
        rq.size = size;
        rq.atime = atime.map(Into::into);
        rq.mtime = mtime.map(Into::into);
        rq.ctime = ctime.map(Into::into);
        rq.crtime = crtime.map(Into::into);
        rq.chgtime = chgtime.map(Into::into);
        rq.bkuptime = bkuptime.map(Into::into);
        rq.flags = flags;
        let rs = self.lock_grpc().await?
            .inode_set_all_attributes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_open(&self, ino: StorageIno) -> HashFsResult<uuid::Uuid> {
        let mut rq = grpc_fs::InodeOpenRq::default();
        rq.ino = Some(ino.into());
        let rs = self.lock_grpc().await?
            .inode_open(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(use_id) = rs.use_id else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(use_id.into())
    }

    async fn inode_close(&self, ino: StorageIno, use_id: uuid::Uuid
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::InodeCloseRq::default();
        rq.ino = Some(ino.into());
        rq.use_id = Some(use_id.into());
        let rs = self.lock_grpc().await?
            .inode_close(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_allocate_size(
        &self,
        ino: StorageIno,
        offset: i64,
        length: i64,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::InodeAllocateSizeRq::default();
        rq.ino = Some(ino.into());
        rq.offset = offset;
        rq.length = length;
        let rs = self.lock_grpc().await?
            .inode_allocate_size(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_read_inline_data(
        &self,
        ino: StorageIno
    ) -> HashFsResult<Vec<u8>> {
        let mut rq = grpc_fs::InodeReadInlineDataRq::default();
        rq.ino = Some(ino.into());
        let rs = self.lock_grpc().await?
            .inode_read_inline_data(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.data)
    }

    async fn inode_read_block_hashes_data_range(
        &self,
        ino: StorageIno,
        start: u64,
        read_size: u64,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let mut rq = grpc_fs::InodeReadBlockHashesDataRangeRq::default();
        rq.ino = Some(ino.into());
        rq.start = start;
        rq.read_size = read_size;
        let rs = self.lock_grpc().await?
            .inode_read_block_hashes_data_range(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.block_hashes.into_iter().map(|(k,v)|{
            (BlockIndex(k), v.data)
        }).collect::<BTreeMap<_, _>>())
    }

    async fn inode_read_block_hashes_block_range(
        &self,
        ino: StorageIno,
        block_ranges: &[Range<BlockIndex>],
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let mut rq = grpc_fs::InodeReadBlockHashesBlockRangeRq::default();
        rq.ino = Some(ino.into());
        rq.ranges = block_ranges.iter().map(|br| (*br).clone().into())
            .collect::<Vec<grpc_fs::BlockRange>>();
        let rs = self.lock_grpc().await?
            .inode_read_block_hashes_block_range(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.block_hashes.into_iter().map(|(k,v)|{
            (BlockIndex(k), v.data)
        }).collect::<BTreeMap<_, _>>())
    }

    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, HashFsData>> {
        let mut rq = grpc_fs::HbGetBlockDataByHashesRq::default();
        rq.hashes = hashes.iter().map(|h|{
            grpc_fs::Hash{ data: h.to_vec() }
        }).collect::<Vec<_>>();
        let mut rs = self.lock_grpc().await?
            .hb_get_block_data_by_hashes(rq).await?.into_inner();

        let mut result = HashMap::new();
        while let Some(part) = rs.message().await? {
            handle_error(&part.error)?;
            let mut iter = part.block_data.into_iter();
            while let Some(block) = iter.next() {
                result.insert(block.hash.unwrap().data, Bytes::from(block.data));
            }
        }
        Ok(result)
    }

    async fn file_get_hash(&self, ino: StorageIno) -> HashFsResult<Vec<u8>> {
        let mut rq = grpc_fs::FileGetHashRq::default();
        rq.ino = Some(ino.into());
        let rs = self.lock_grpc().await?
            .file_get_hash(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(hash) = rs.hash else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(hash.data)
    }

    async fn file_read_block_hashes(
        &self,
        ino: StorageIno,
        block_range: Range<BlockIndex>
    ) -> HashFsResult<Vec<u8>> {
        let mut rq = grpc_fs::FileReadBlockHashesRq::default();
        rq.ino = Some(ino.into());
        rq.block_range = Some(block_range.into());
        let rs = self.lock_grpc().await?
            .file_read_block_hashes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.hashes)
    }

    async fn hb_increment_reference_count(
        &self,
        blocks: &[(&TiFsHash, u64)],
    ) -> HashFsResult<HashMap<TiFsHash, BigUint>> {
        if blocks.len() == 0 {
            return Ok(HashMap::new());
        }
        let mut rq = grpc_fs::HbIncrementReferenceCountRq::default();
        rq.increments = blocks.iter().map(|(h,c)|{
            grpc_fs::HashBlockIncrements {
                hash: Some(grpc_fs::Hash{data: (**h).clone()}),
                inc: *c,
            }
        }).collect();
        let rs = self.lock_grpc().await?
            .hb_increment_reference_count(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let previous_cnt = rs.previous_counts.into_iter()
            .filter_map(|grpc_fs::HashBlockCount{hash,count}|{
                Some((hash?.data, count?.into()))
        }).collect::<HashMap<_,_>>();
        Ok(previous_cnt)
    }

    async fn hb_upload_new_blocks(
        &self,
        blocks: &[(&TiFsHash, HashFsData)],
    ) -> HashFsResult<()> {
        if blocks.len() == 0 {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel(10);
        let upload = ReceiverStream::new(rx);

        let values = blocks.iter().map(|(h,d)|{
            (h.to_vec(), d.clone())
        }).collect::<Vec<_>>();
        tokio::spawn(async move {
            for (h, data) in values.into_iter() {
                let mut rq = grpc_fs::HashBlockData::default();
                rq.hash = Some(grpc_fs::Hash{data: h});
                rq.data = data.to_vec();
                let mut rq_full = grpc_fs::HbUploadNewBlockRq::default();
                rq_full.blocks.push(rq);
                tx.send(rq_full).await.unwrap();
            }
        });

        let rs = self.lock_grpc().await?
            .hb_upload_new_block(upload).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        ino: StorageIno,
        blocks: &[(&TiFsHash, u64, Vec<BlockIndex>)],
    ) -> HashFsResult<()> {
        if blocks.len() == 0 {
            return Ok(());
        }
        let mut rq = grpc_fs::InodeWriteHashBlockToAddressesUpdateInoSizeAndCleaningPreviousBlockHashesRq::default();
        rq.ino = Some(ino.into());
        rq.blocks = blocks.iter().map(|(h,l,ids)|{
            grpc_fs::HashBlockAddresses {
                hash: Some(grpc_fs::Hash { data: (*h).clone() }),
                block_actual_length: *l,
                block_ids: ids.iter().cloned().map(Into::into).collect(),
            }
        }).collect();
        let rs = self.lock_grpc().await?
            .inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(rq)
            .await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn snapshot_create(&self, name: ByteString) -> HashFsResult<GotOrMade<StorageDirItem>> {
        let mut rq = grpc_fs::SnapshotCreateRq::default();
        rq.name = name.to_string();
        let rs = self.lock_grpc().await?.snapshot_create(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let existed_before = rs.existed_already;
        let Some(item) = rs.item else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        if existed_before {
            Ok(GotOrMade::ExistedAlready(item.into()))
        } else {
            Ok(GotOrMade::NewlyCreated(item.into()))
        }

    }
}
