use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use clap::ArgAction;
use clap::{crate_version, builder::Arg};

use rust_grpc_example::grpc::hash_fs::hash_fs_server::HashFsServer;
use rust_grpc_example::grpc_time_to_system_time;
use tifs::fs::fs_config::{self};
use tifs::fs::hash_fs_interface::HashFsInterface;
use tonic::{transport::Server, Request, Response, Status};

use rust_grpc_example::grpc::greeter::greeter_server::{Greeter, GreeterServer};
use rust_grpc_example::grpc::greeter::{HelloResponse, HelloRequest};
use rust_grpc_example::grpc::hash_fs::{self as grpc_fs};


// Implement the service skeleton for the "Greeter" service
// defined in the proto
#[derive(Debug, Default)]
pub struct MyGreeter {}

// Implement the service function(s) defined in the proto
// for the Greeter service (SayHello...)
#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        println!("Received request from: {:?}", request);

        let response = HelloResponse {
            message: format!("Hello {}!", request.into_inner().name).into(),
        };

        Ok(Response::new(response))
    }
}

struct HashFsGrpcServer {
    fs_impl: Arc<dyn HashFsInterface>,
}

impl HashFsGrpcServer {
    pub async fn new(
        pd_endpoints: Vec<String>,
        options: Vec<fs_config::MountOption>,
    ) -> anyhow::Result<Self> {

        let fs_impl = tifs::fs::tikv_fs::TiFs::construct_hash_fs_server(
            pd_endpoints, options).await?;

        Ok(HashFsGrpcServer {
            fs_impl,
        })
    }
}

// Implement the service function(s) defined in the proto
// for the Greeter service (SayHello...)
#[tonic::async_trait]
impl grpc_fs::hash_fs_server::HashFs for HashFsGrpcServer {
    async fn init(
        &self,
        request: tonic::Request<grpc_fs::InitRq>,
    ) -> std::result::Result<tonic::Response<grpc_fs::InitRs>, tonic::Status> {
        let r = self.fs_impl.init(
            request.get_ref().gid, request.get_ref().uid).await;
        let mut rsp = grpc_fs::InitRs::default();
        match r {
            Err(error) => rsp.error = Some(error.into()),
            Ok(item) => rsp.value = Some(item.into()),
        };
        Ok(tonic::Response::new(rsp))
    }
    async fn meta_static_read(
        &self,
        _request: tonic::Request<grpc_fs::MetaStaticReadRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::MetaStaticReadRs>,
        tonic::Status,
    >{
        let rs = self.fs_impl
            .meta_static_read().await;
        let mut rsp = grpc_fs::MetaStaticReadRs::default();
        match rs {
            Err(err) => rsp.error = Some(err.into()),
            Ok(val) => rsp.value = Some(val.into()),
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn directory_read_children(
        &self,
        request: tonic::Request<grpc_fs::DirectoryReadChildrenRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryReadChildrenRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(dir_ino) = rq.dir_ino.clone() else {
            return Err(tonic::Status::invalid_argument("dir_ino parameter is required!"));
        };
        let r = self.fs_impl.directory_read_children(dir_ino.into()).await;
        let mut rsp = grpc_fs::DirectoryReadChildrenRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(val) =>
                rsp.value = val.into_iter().map(|i|{
                    let mut o = grpc_fs::DirectoryItem::default();
                    o.ino = Some(i.ino.into());
                    o.name = i.name;
                    o.set_typ(i.typ.into());
                    o
                }).collect::<Vec<_>>(),
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn directory_add_child_checked_new_inode(
        &self,
        request: tonic::Request<grpc_fs::DirectoryAddChildCheckedNewInodeRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryAddChildCheckedNewInodeRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent.clone() else {
            return Err(tonic::Status::invalid_argument("parent parameter is required!"));
        };
        let Some(perm) = rq.perm.clone() else {
            return Err(tonic::Status::invalid_argument("perm parameter is required!"));
        };
        let r = self.fs_impl
            .directory_add_child_checked_new_inode(
                parent.into(),
                rq.name.clone().into(),
                rq.typ().into(),
                perm.into(),
                rq.gid,
                rq.uid,
                rq.rdev,
                (rq.inline_data.len() > 0).then(||rq.inline_data),
            ).await;
        let mut rsp = grpc_fs::DirectoryAddChildCheckedNewInodeRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(val) => {
                rsp.existed_already = val.existed_before();
                rsp.item = Some(val.value().into());
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn directory_add_child_checked_existing_inode(
        &self,
        request: tonic::Request<grpc_fs::DirectoryAddChildCheckedExistingInodeRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryAddChildCheckedExistingInodeRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino is required!"));
        };
        let r = self.fs_impl
            .directory_add_child_checked_existing_inode(
            parent.into(), rq.name.into(), ino.into()).await;
        let mut rs = grpc_fs::DirectoryAddChildCheckedExistingInodeRs::default();
        if let Err(err) = r {
            rs.error = Some(err.into());
        }
        Ok(tonic::Response::new(rs))
    }
    async fn directory_remove_child_file(
        &self,
        request: tonic::Request<grpc_fs::DirectoryRemoveChildFileRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryRemoveChildFileRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let r = self.fs_impl
            .directory_remove_child_file(parent.into(), rq.name.into()).await;
        let mut rs = grpc_fs::DirectoryRemoveChildFileRs::default();
        if let Err(err) = r {
            rs.error = Some(err.into());
        }
        Ok(tonic::Response::new(rs))
    }
    async fn directory_remove_child_directory(
        &self,
        request: tonic::Request<grpc_fs::DirectoryRemoveChildDirectoryRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryRemoveChildDirectoryRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let r = self.fs_impl
            .directory_remove_child_directory(parent.into(), rq.name.into()).await;
        let mut rs = grpc_fs::DirectoryRemoveChildDirectoryRs::default();
        if let Err(err) = r {
            rs.error = Some(err.into());
        }
        Ok(tonic::Response::new(rs))
    }

    async fn directory_rename_child(
        &self,
        request: tonic::Request<grpc_fs::DirectoryRenameChildRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryRenameChildRs>,
        tonic::Status,
    > {
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let Some(new_parent) = rq.new_parent else {
            return Err(tonic::Status::invalid_argument("new_parent is required!"));
        };
        let r = self.fs_impl.directory_rename_child(
            parent.into(),
            rq.child_name.into(),
            new_parent.into(),
            rq.new_child_name.into()
        ).await;
        let mut rs = grpc_fs::DirectoryRenameChildRs::default();
        if let Err(err) = r {
            rs.error = Some(err.into());
        }
        Ok(tonic::Response::new(rs))
    }
    async fn directory_child_get_all_attributes(
        &self,
        request: tonic::Request<grpc_fs::DirectoryChildGetAllAttributesRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryChildGetAllAttributesRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let r = self.fs_impl
            .directory_child_get_all_attributes(
                parent.into(), rq.name.into()).await;
        let mut rsp = grpc_fs::DirectoryChildGetAllAttributesRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok((desc, attr, size, atime)) => {
                let mut o = grpc_fs::InoAllAttributes::default();
                o.desc = Some(desc.deref().clone().into());
                o.attrs = Some(attr.deref().clone().into());
                o.size = Some(size.deref().clone().into());
                o.atime = Some(atime.into());
                rsp.all = Some(o);
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn directory_add_new_symlink(
        &self,
        request: tonic::Request<grpc_fs::DirectoryAddNewSymlinkRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::DirectoryAddNewSymlinkRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(parent) = rq.parent else {
            return Err(tonic::Status::invalid_argument("parent is required!"));
        };
        let r = self.fs_impl
            .directory_add_new_symlink(
                rq.gid, rq.uid, parent.into(),
                rq.name.into(), rq.link.into()).await;
        let mut rsp = grpc_fs::DirectoryAddNewSymlinkRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(item) => {
                rsp.item = Some(item.into());
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_get_all_attributes(
        &self,
        request: tonic::Request<grpc_fs::InodeGetAllAttributesRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeGetAllAttributesRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl
            .inode_get_all_attributes(ino.into()).await;
        let mut rsp = grpc_fs::InodeGetAllAttributesRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok((desc, attr, size, atime)) => {
                let mut o = grpc_fs::InoAllAttributes::default();
                o.desc = Some(desc.deref().clone().into());
                o.attrs = Some(attr.deref().clone().into());
                o.size = Some(size.deref().clone().into());
                o.atime = Some(atime.into());
                rsp.all = Some(o);
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_set_all_attributes(
        &self,
        request: tonic::Request<grpc_fs::InodeSetAllAttributesRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeSetAllAttributesRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl.inode_set_all_attributes(
            ino.into(),
            rq.mode.map(Into::into),
            rq.uid,
            rq.gid,
            rq.size,
            rq.atime.map(Into::into),
            rq.mtime.map(Into::into),
            rq.ctime.map(grpc_time_to_system_time),
            rq.crtime.map(grpc_time_to_system_time),
            rq.chgtime.map(grpc_time_to_system_time),
            rq.bkuptime.map(grpc_time_to_system_time),
            rq.flags).await;
        let mut rs = grpc_fs::InodeSetAllAttributesRs::default();
        if let Err(err) = r {
            rs.error = Some(err.into());
        }
        Ok(tonic::Response::new(rs))
    }
    async fn inode_open(
        &self,
        request: tonic::Request<grpc_fs::InodeOpenRq>,
    ) -> std::result::Result<tonic::Response<grpc_fs::InodeOpenRs>, tonic::Status>{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl.inode_open(ino.into()).await;
        let mut rsp = grpc_fs::InodeOpenRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(use_id) => {
                rsp.use_id = Some(use_id.into());
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_close(
        &self,
        request: tonic::Request<grpc_fs::InodeCloseRq>,
    ) -> std::result::Result<tonic::Response<grpc_fs::InodeCloseRs>, tonic::Status>{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let Some(use_id) = rq.use_id else {
            return Err(tonic::Status::invalid_argument("use_id parameter is required!"));
        };
        let r = self.fs_impl.inode_close(
            ino.into(), use_id.into()).await;
        let mut rsp = grpc_fs::InodeCloseRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(()) => {}
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_allocate_size(
        &self,
        request: tonic::Request<grpc_fs::InodeAllocateSizeRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeAllocateSizeRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl
            .inode_allocate_size(ino.into(), rq.offset, rq.length).await;
        let mut rsp = grpc_fs::InodeAllocateSizeRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(()) => {}
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_read_inline_data(
        &self,
        request: tonic::Request<grpc_fs::InodeReadInlineDataRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeReadInlineDataRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl
            .inode_read_inline_data(ino.into()).await;
        let mut rsp = grpc_fs::InodeReadInlineDataRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.data = data;
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_read_block_hashes_data_range(
        &self,
        request: tonic::Request<grpc_fs::InodeReadBlockHashesDataRangeRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeReadBlockHashesDataRangeRs>,
        tonic::Status,
    > {
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl
            .inode_read_block_hashes_data_range(
                ino.into(), rq.start, rq.read_size).await;
        let mut rsp = grpc_fs::InodeReadBlockHashesDataRangeRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.block_hashes = data.into_iter().map(|(k,v)|{
                    (k.0, grpc_fs::Hash{data: v})
                }).collect::<HashMap<_,_>>();
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_read_block_hashes_block_range(
        &self,
        request: tonic::Request<grpc_fs::InodeReadBlockHashesBlockRangeRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::InodeReadBlockHashesBlockRangeRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let Some(block_range) = rq.range else {
            return Err(tonic::Status::invalid_argument("block_range parameter is required!"));
        };
        let r = self.fs_impl
            .inode_read_block_hashes_block_range(
                ino.into(), block_range.into()).await;
        let mut rsp = grpc_fs::InodeReadBlockHashesBlockRangeRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.block_hashes = data.into_iter().map(|(k,v)|{
                    (k.0, grpc_fs::Hash{data: v})
                }).collect::<HashMap<_,_>>();
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn hb_get_block_data_by_hashes(
        &self,
        request: tonic::Request<grpc_fs::HbGetBlockDataByHashesRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::HbGetBlockDataByHashesRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let hashes = rq.hashes.iter().map(|h|{
            &h.data
        }).collect::<HashSet<_>>();
        let r = self.fs_impl
            .hb_get_block_data_by_hashes(&hashes).await;
        let mut rsp = grpc_fs::HbGetBlockDataByHashesRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.block_data = data.into_iter().map(|(k,v)|{
                    grpc_fs::HashBlockData{
                        hash: Some(grpc_fs::Hash{data: k}),
                        data: v.deref().clone(),
                    }
                }).collect::<Vec<_>>();
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn file_get_hash(
        &self,
        request: tonic::Request<grpc_fs::FileGetHashRq>,
    ) -> std::result::Result<tonic::Response<grpc_fs::FileGetHashRs>, tonic::Status>{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let r = self.fs_impl
            .file_get_hash(ino.into()).await;
        let mut rsp = grpc_fs::FileGetHashRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.hash = Some(grpc_fs::Hash{ data });
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn file_read_block_hashes(
        &self,
        request: tonic::Request<grpc_fs::FileReadBlockHashesRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::FileReadBlockHashesRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let Some(block_range) = rq.block_range else {
            return Err(tonic::Status::invalid_argument("block_range parameter is required!"));
        };
        let r = self.fs_impl
            .file_read_block_hashes(ino.into(), block_range.into()).await;
        let mut rsp = grpc_fs::FileReadBlockHashesRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.hashes = data;
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn hb_increment_reference_count(
        &self,
        request: tonic::Request<grpc_fs::HbIncrementReferenceCountRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::HbIncrementReferenceCountRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(hash) = rq.hash else {
            return Err(tonic::Status::invalid_argument("hash parameter is required!"));
        };
        let r = self.fs_impl
            .hb_increment_reference_count(&hash.data, rq.cnt).await;
        let mut rsp = grpc_fs::HbIncrementReferenceCountRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(data) => {
                rsp.previous_cnt = Some(data.into());
            }
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn hb_upload_new_block(
        &self,
        request: tonic::Request<grpc_fs::HbUploadNewBlockRq>,
    ) -> std::result::Result<
        tonic::Response<grpc_fs::HbUploadNewBlockRs>,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(block_hash) = rq.block_hash else {
            return Err(tonic::Status::invalid_argument("block_hash parameter is required!"));
        };
        let r = self.fs_impl
            .hb_upload_new_block(block_hash.data, rq.data).await;
        let mut rsp = grpc_fs::HbUploadNewBlockRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(()) => {}
        }
        Ok(tonic::Response::new(rsp))
    }
    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        request: tonic::Request<
        grpc_fs::InodeWriteHashBlockToAddressesUpdateInoSizeAndCleaningPreviousBlockHashesRq,
        >,
    ) -> std::result::Result<
        tonic::Response<
        grpc_fs::InodeWriteHashBlockToAddressesUpdateInoSizeAndCleaningPreviousBlockHashesRs,
        >,
        tonic::Status,
    >{
        let rq = request.into_inner();
        let Some(ino) = rq.ino else {
            return Err(tonic::Status::invalid_argument("ino parameter is required!"));
        };
        let Some(block_hash) = rq.block_hash else {
            return Err(tonic::Status::invalid_argument("block_hash parameter is required!"));
        };
        let r = self.fs_impl
            .inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
                ino.into(),
                block_hash.data,
                rq.blocks_size,
                rq.block_ids.into_iter().map(|id|id.into()).collect::<Vec<_>>(),
            ).await;
        let mut rsp = grpc_fs::InodeWriteHashBlockToAddressesUpdateInoSizeAndCleaningPreviousBlockHashesRs::default();
        match r {
            Err(err) => rsp.error = Some(err.into()),
            Ok(()) => {}
        }
        Ok(tonic::Response::new(rsp))
    }
}

// Runtime to run our server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .try_init()
                .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

    let matches = clap::builder::Command::new("hashfs-tikv-server")
        .version(crate_version!())
        .author("Hexi Lee")
        .arg(
            Arg::new("device")
                .value_name("ENDPOINTS")
                .required(true)
                .help("all pd endpoints of the tikv cluster, separated by commas (e.g. tifs:127.0.0.1:2379)")
                .index(1)
                .action(ArgAction::Append)
        )
        .arg(
            Arg::new("mount-point")
                .value_name("MOUNT_POINT")
                .required(true)
                .help("act as a client, and mount FUSE at given path")
                .index(2)
        )
        .arg(
            Arg::new("tracer")
                .value_name("TRACER")
                .long("tracer")
                .short('t')
                .help("the tracer <logger | jaeger>, logger by default")
        )
        .arg(
            Arg::new("jaeger-collector")
                .value_name("JAEGER_COLLECTOR")
                .long("jaeger-collector")
                .short('c')
                .help("the jaeger collector endpoint (e.g. tifs:127.0.0.1:14268)")
        )
        .arg(
            Arg::new("jaeger-agent")
                .value_name("JAEGER_AGENT")
                .long("jaeger-agent")
                .short('a')
                .help("the jaeger agent endpoint (e.g. tifs:127.0.0.1:6831)")
        )
        .arg(
            Arg::new("options")
                .value_name("OPTION")
                .long("option")
                .short('o')
                .help("filesystem mount options")
                .action(ArgAction::Append)
        )
        .arg(
            Arg::new("foreground")
                .long("foreground")
                .short('f')
                .help("foreground operation")
        )
        .arg(
            Arg::new("serve")
                .long("serve")
                .help("run in server mode (implies --foreground)")
                .hide(true)
        )
        .arg(
            Arg::new("logfile")
                .long("log-file")
                .value_name("LOGFILE")
                .help("log file in server mode (ignored if --foreground is present)")
        )
        .get_matches();

    let endpoints = matches.get_many::<String>("device").map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    let pd_endpoints = endpoints.into_iter()
        .filter_map(|s|s.strip_prefix("tifs:").map(String::from))
        .collect::<Vec<_>>();

    let options_str = matches.get_many::<String>("options")
    .map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    let options = fs_config::MountOption::to_vec(
        options_str.iter().map(|s|s.as_str()));

    let addr = "[::]:50051".parse()?;
    let greeter = MyGreeter::default();
    let hash_fs = HashFsGrpcServer::new(
        pd_endpoints, options).await?;

    tracing::error!("v - Starting gRPC Server...");
    println!("Starting gRPC Server...");
    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .add_service(HashFsServer::new(hash_fs))
        .serve(addr)
        .await?;

    Ok(())
}
