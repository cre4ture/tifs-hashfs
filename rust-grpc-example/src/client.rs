
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use bytestring::ByteString;
use clap::{Arg, ArgAction};
use fuser::TimeOrNow;
use num_bigint::BigUint;
use rust_grpc_example::grpc::greeter::greeter_client::GreeterClient;
use rust_grpc_example::grpc::greeter::HelloRequest;

use rust_grpc_example::grpc::hash_fs::{self as grpc_fs, InitRq, MetaStaticReadRq};
use tifs::fs::hash_fs_interface::{BlockIndex, GotOrMade, HashFsError, HashFsInterface, HashFsResult};
use tifs::fs::inode::{DirectoryItem, InoAccessTime, InoDescription, InoSize, InoStorageFileAttr, ParentStorageIno, StorageDirItem, StorageDirItemKind, StorageFilePermission, StorageIno, TiFsHash};
use tifs::fs::meta::MetaStatic;
use tokio::sync::{RwLock, RwLockWriteGuard};

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
    grpc_client: RwLock<RawGrpcClient>,
}

impl HashFsClient {
    async fn lock_grpc(&self) -> RwLockWriteGuard<RawGrpcClient> {
        let r: RwLockWriteGuard<RawGrpcClient> = self.grpc_client.write().await;
        r
    }
}

#[tonic::async_trait]
impl HashFsInterface for HashFsClient {

    async fn init(&self, gid: u32, uid: u32) -> HashFsResult<StorageDirItem> {
        let rs = self.lock_grpc().await
            .init(InitRq{gid, uid}).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(v) = rs.value else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(v.into())
    }

    async fn meta_static_read(&self) -> HashFsResult<MetaStatic> {
        let rs = self.lock_grpc().await
            .meta_static_read(MetaStaticReadRq{}).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(v) = rs.value else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(v.into())
    }

    async fn directory_read_children(&self, dir_ino: StorageIno) -> HashFsResult<Vec<DirectoryItem>> {
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let mut rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let mut rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
            .inode_set_all_attributes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_open(&self, ino: StorageIno) -> HashFsResult<uuid::Uuid> {
        let mut rq = grpc_fs::InodeOpenRq::default();
        rq.ino = Some(ino.into());
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
            .inode_read_block_hashes_data_range(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.block_hashes.into_iter().map(|(k,v)|{
            (BlockIndex(k), v.data)
        }).collect::<BTreeMap<_, _>>())
    }

    async fn inode_read_block_hashes_block_range(
        &self,
        ino: StorageIno,
        block_range: std::ops::Range<BlockIndex> ,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let mut rq = grpc_fs::InodeReadBlockHashesBlockRangeRq::default();
        rq.ino = Some(ino.into());
        rq.range = Some(block_range.into());
        let rs = self.lock_grpc().await
            .inode_read_block_hashes_block_range(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.block_hashes.into_iter().map(|(k,v)|{
            (BlockIndex(k), v.data)
        }).collect::<BTreeMap<_, _>>())
    }

    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        let mut rq = grpc_fs::HbGetBlockDataByHashesRq::default();
        rq.hashes = hashes.iter().map(|h|{
            grpc_fs::Hash{ data: h.to_vec() }
        }).collect::<Vec<_>>();
        let rs = self.lock_grpc().await
            .hb_get_block_data_by_hashes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.block_data.into_iter().filter_map(|mut d|{
            if let Some(hash) = mem::take(&mut d.hash) {
                Some((hash.data, Arc::new(d.data)))
            } else {
                None
            }
        }).collect::<HashMap<_,_>>())
    }

    async fn file_get_hash(&self, ino: StorageIno) -> HashFsResult<Vec<u8>> {
        let mut rq = grpc_fs::FileGetHashRq::default();
        rq.ino = Some(ino.into());
        let rs = self.lock_grpc().await
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
        let rs = self.lock_grpc().await
            .file_read_block_hashes(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(rs.hashes)
    }

    async fn hb_increment_reference_count(
        &self,
        hash: &TiFsHash,
        cnt: u64,
    ) -> HashFsResult<BigUint> {
        let mut rq = grpc_fs::HbIncrementReferenceCountRq::default();
        rq.hash = Some(grpc_fs::Hash{data: hash.clone()});
        rq.cnt = cnt;
        let rs = self.lock_grpc().await
            .hb_increment_reference_count(rq).await?.into_inner();
        handle_error(&rs.error)?;
        let Some(previous_cnt) = rs.previous_cnt else {
            return Err(HashFsError::GrpcMessageIncomplete);
        };
        Ok(BigUint::from_bytes_be(&previous_cnt.big_endian_value))
    }

    async fn hb_upload_new_block(
        &self,
        block_hash: TiFsHash,
        data: Vec<u8>
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::HbUploadNewBlockRq::default();
        rq.block_hash = Some(grpc_fs::Hash { data: block_hash });
        rq.data = data;
        let rs = self.lock_grpc().await
            .hb_upload_new_block(rq).await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }

    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        ino: StorageIno,
        block_hash: TiFsHash,
        blocks_size: u64,
        block_ids: Vec<BlockIndex>,
    ) -> HashFsResult<()> {
        let mut rq = grpc_fs::InodeWriteHashBlockToAddressesUpdateInoSizeAndCleaningPreviousBlockHashesRq::default();
        rq.ino = Some(ino.into());
        rq.block_hash = Some(grpc_fs::Hash { data: block_hash });
        rq.blocks_size = blocks_size;
        rq.block_ids = block_ids.into_iter().map(Into::into).collect::<Vec<_>>();
        let rs = self.lock_grpc().await
            .inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(rq)
            .await?.into_inner();
        handle_error(&rs.error)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .try_init()
                .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

    let matches = clap::builder::Command::new("hashfs-tikv-server")
        .version(clap::crate_version!())
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

    // "hash-fs:http://[::1]:50051"
    let pd_endpoints = endpoints.into_iter()
        .filter_map(|s|s.strip_prefix("hash-fs:").map(String::from))
        .collect::<Vec<_>>();

    let options_str = matches.get_many::<String>("options")
    .map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    let options = tifs::fs::fs_config::MountOption::to_vec(
        options_str.iter().map(|s|s.as_str()));

    if pd_endpoints.len() == 0 {
        return Err(anyhow::anyhow!("missing endpoint parameter"));
    }

    let endpoint_str = pd_endpoints.first().unwrap();
    let grpc_endpoint = tonic::transport::Endpoint::from_str(endpoint_str)
        .map_err(|err| anyhow::anyhow!("parsing endpoint {} failed: Err: {:?}", endpoint_str, err))?;
    let mut client = GreeterClient::connect(grpc_endpoint.clone()).await?;

    let request = tonic::Request::new(HelloRequest {
        name: "Tonic".into(),
    });

    println!("Sending request to gRPC Server...");
    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    let hash_fs_client = Arc::new(HashFsClient{
        grpc_client: RwLock::new(
            rust_grpc_example::grpc::hash_fs::hash_fs_client::HashFsClient::connect(grpc_endpoint).await?),
        });

    let fs = tifs::fs::tikv_fs::TiFs::construct_hash_fs_client(pd_endpoints, options.clone(), hash_fs_client).await?;

    let mount_point = std::fs::canonicalize(
        &matches
            .get_one::<String>("mount-point")
            .ok_or_else(|| anyhow::anyhow!("mount-point is required"))?,
    )?
    .to_string_lossy()
    .to_string();

    tifs::fuse_mount_daemonize(mount_point, options, ||Ok(()), fs).await?;

    anyhow::Ok(())
}
