
use std::ops::{Deref, Range};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use bytestring::ByteString;
use fuser::TimeOrNow;
use num_bigint::BigUint;
use range_collections::range_set::RangeSetRange;
use range_collections::RangeSet2;
use strum::IntoEnumIterator;
use tikv_client::{Key, KvPair};
use tikv_client::transaction::Mutation;
use uuid::Uuid;

use crate::fs::hash_fs_interface::GotOrMadePure;
use crate::fs::inode::TiFsHash;
use crate::fs::key::{BlockAddress, PARENT_OF_ROOT_INODE};
use crate::fs::fuse_to_hashfs::MAX_TIKV_SCAN_LIMIT;
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

use super::error::FsError;
use super::index::deserialize_json;
use super::mini_transaction::{DeletionCheckResult, MiniTransaction};
use super::utils::lazy_lock_map::LazyLockMap;
use super::utils::stop_watch::AutoStopWatch;
use super::utils::txn_data_cache::{TxnFetch, TxnPut, TxnPutMut};
use super::{
    error::TiFsResult, flexible_transaction::FlexibleTransaction, fs_config::TiFsConfig, meta::MetaStatic
    };
use super::key::{check_file_name, InoMetadata, ROOT_INODE};
use super::hash_fs_interface::{
        BlockIndex, GotOrMade, HashFsError, HashFsInterface, HashFsResult};
use super::inode::{DirectoryItem, InoAccessTime, InoChangeIterationId, InoDescription, InoInlineData, InoSize, InoStorageFileAttr, ModificationTime, ParentStorageIno, StorageDirItem, StorageDirItemKind, StorageFilePermission, StorageIno};


fn get_time_from_time_or_now(time: TimeOrNow) -> SystemTime {
    match time {
        TimeOrNow::SpecificTime(t) => t,
        TimeOrNow::Now => SystemTime::now(),
    }
}

fn set_if_changed<T: std::cmp::PartialEq>(
    change_cnt: &mut usize,
    field: &mut T,
    new: Option<T>
) {
    if let Some(new_value) = new {
        let changed = *field != new_value;
        if changed {
            *change_cnt += 1;
            *field = new_value;
        }
    }
}

pub struct TikvBasedHashFs {
    weak: Weak<TikvBasedHashFs>,
    f_txn: FlexibleTransaction,
    fs_config: TiFsConfig,
    // this is for performance optimization only.
    // in cases when parallel writes are executed on the same client,
    // these locks are used to avoid unnecessary transaction spins
    // caused by conflicts the client created itself.
    // TODO: use heartbeat to cleanup this map from invalid references.
    pub local_ino_write_size_locks: LazyLockMap<StorageIno, ()>,
    pub local_ino_locks_full_hash: LazyLockMap<StorageIno, ()>,
    pub local_ino_locks_clear_full_hash: LazyLockMap<StorageIno, ()>,
    pub local_ino_locks_update_change_iter: LazyLockMap<StorageIno, ()>,
}

impl TikvBasedHashFs {
    pub fn new_arc(fs_config: TiFsConfig, f_txn: FlexibleTransaction) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            Self {
                weak: weak.clone(),
                f_txn,
                fs_config,
                local_ino_write_size_locks: LazyLockMap::new(),
                local_ino_locks_full_hash: LazyLockMap::new(),
                local_ino_locks_clear_full_hash: LazyLockMap::new(),
                local_ino_locks_update_change_iter: LazyLockMap::new(),
            }
        })
    }

    async fn directory_remove_child_generic(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        allowed_types: HashSet<StorageDirItemKind>,
    ) -> HashFsResult<()> {
        let mut mini = self.f_txn.spinning_mini_txn().await?;
        let unlinked_item: StorageDirItem = loop {
            let mut started = mini.start().await?;
            let r1 = started.directory_remove_child(
                parent, name.clone(), &allowed_types).await;
            if let Some(r) = started.finish(
                r1).await { break r?; }
        };
        drop(mini);

        let mut mini = self.f_txn.spinning_mini_txn().await?;
        let r: DeletionCheckResult = loop {
            let mut started = mini.start().await?;
            let r1 = started
                .inode_check_for_delete_and_delete_atomically_description(unlinked_item.ino).await;
            if let Some(r) = started.finish(r1
            ).await { break r?; }
        };
        drop(mini);

        if r == DeletionCheckResult::DeletedInoDesc {
            // clear remaining inode data:
            match unlinked_item.typ {
                StorageDirItemKind::File
                    => self.hb_clear_data_no_sync_no_size_update(unlinked_item.ino).await?,
                StorageDirItemKind::Directory => {/* directory needs to be empty before deletion */}
                StorageDirItemKind::Symlink => {/* symlink data is inline only */}
            }

            for meta in InoMetadata::iter() {
                let key = self.fs_config.key_builder().inode_x(unlinked_item.ino, meta).as_key();
                self.f_txn.delete(key).await?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range(
        &self,
        ino: StorageIno,
        block_range: Range<BlockIndex>,
    ) -> TiFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let range = self.fs_config.key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.f_txn.scan(
                range,
                block_range.count() as u32,
            )
            .await?;
        let mut result = BTreeMap::<BlockIndex, TiFsHash>::new();
        for KvPair(k, hash) in iter.into_iter() {
            let key = self.fs_config.key_parser_b(k)?.parse_key_block_address()?;
            if hash.len() != self.fs_config.hash_len {
                tracing::error!("hash length mismatch!");
                continue;
            };
            if key.ino != ino {
                tracing::error!("received wrong ino-block: expected: {ino}, got: {}", key.ino);
                continue;
            }
            result.insert(key.index, hash);
        }
        tracing::trace!("result: len:{}", result.len());
        Ok(result)
    }


    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range_chunked(
        &self, ino: StorageIno, block_range: Range<BlockIndex>
    ) -> TiFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let mut i = block_range.start;
        let mut result = BTreeMap::new();
        while i < block_range.end {
            let chunk_start = i;
            let chunk_end = BlockIndex(chunk_start.0+MAX_TIKV_SCAN_LIMIT as u64)
                                        .min(block_range.end);
            i = chunk_end;

            let chunk = self.hb_get_block_hash_list_by_block_range(
                ino, chunk_start..chunk_end).await?;
            result.extend(chunk.into_iter());
        }
        tracing::trace!("result: len:{}", result.len());
        Ok(result)
    }

    pub async fn hb_clear_data_single_block_arc(
        self: Arc<Self>,
        addr: BlockAddress,
        do_size_update: bool,
    ) -> TiFsResult<()> {
        self.hb_clear_data_single_block(addr, do_size_update).await
    }

    pub async fn hb_clear_data_single_block(&self, addr: BlockAddress, do_size_update: bool) -> TiFsResult<()> {
        let mut spin = MiniTransaction::new(&self.f_txn).await?;
        let prev_hash = loop {
            let mut started = spin.start().await?;
            let r1 = started
                .hb_replace_block_hash_for_address_no_size_update(
                    &[(addr, None)]).await;
            if let Some(result) = started.finish(
                r1).await { break result?; }
        };

        if do_size_update {
            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            loop {
                let lock = self.local_ino_write_size_locks.lock_write(&addr.ino).await;
                let mut started = spin.start().await?;
                let r1 = started
                    .hb_replace_block_hash_for_address_only_size_update(
                        &addr, 0).await;
                if let Some(result) = started.finish(
                    r1).await { break result?; }
                drop(lock);
            };
        }

        let Some((prev_h, _prev_cnt)) = prev_hash.into_iter().next() else {
            return Ok(());
        };

        let mut spin = MiniTransaction::new(&self.f_txn).await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
                &HashMap::from([(&prev_h, 1u64)])).await;
            if let Some(result) = started.finish(
                r1).await { break result?; }
        }

        Ok(())
    }

    pub async fn hb_clear_data_no_sync_no_size_update(&self, ino: StorageIno) -> TiFsResult<()> {

        let ino_size: Arc<InoSize> = self.f_txn.fetch(&ino).await?;
        let block_cnt = ino_size.size().div_ceil(self.fs_config.block_size);

        let mut parallel_executor = AsyncParallelPipeStage::new(
            self.fs_config.parallel_jobs_delete);
        for block in (0..block_cnt).map(BlockIndex) {
            let addr = BlockAddress { ino, index: block };
            let me = self.weak.upgrade().unwrap();
            parallel_executor.push(me.clone().hb_clear_data_single_block_arc(addr, false)).await;
        }
        parallel_executor.wait_finish_all().await;
        for r in parallel_executor.get_results_so_far() {
            r?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn read_hashes_of_file(&self, ino: StorageIno, block_range: Range<BlockIndex>
    ) -> HashFsResult<Vec<u8>> {
        let hashes = self.hb_get_block_hash_list_by_block_range_chunked(
            ino, block_range.clone()).await?;
        let mut result = Vec::with_capacity((block_range.clone().count() * self.fs_config.hash_len) as usize);
        let zero_hash = self.fs_config.calculate_hash(&[]);
        for block_id in block_range {
            let block_hash = hashes.get(&block_id).unwrap_or(&zero_hash);
            if block_hash.len() != self.fs_config.hash_len {
                return Err(HashFsError::FsHasInvalidData(
                    Some(format!("hash has invalid length. Expected: {}, actual: {}",
                                self.fs_config.hash_len, block_hash.len()))));
            }
            result.extend_from_slice(&block_hash);
        }
        tracing::trace!("res-len-end: {}", result.len());
        Ok(result)
    }

    pub async fn get_or_make_dir(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
    ) -> HashFsResult<GotOrMade<StorageDirItem>> {
        Ok(self.directory_add_child_checked_new_inode(
            parent, name, StorageDirItemKind::Directory,
            perm, gid, uid, 0, None).await?)
    }

    pub async fn meta_mutable_reserve_new_ino(&self) -> TiFsResult<StorageIno> {
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.meta_mutable_reserve_new_ino().await;
            if let Some(r) = started.finish(r1).await { break Ok(r?); }
        }
    }

    pub async fn inode_read_metadata_multi(
        &self,
        ino: StorageIno,
        elements: &[InoMetadata],
    ) -> TiFsResult<(Vec<TiFsHash>, Vec<InoChangeIterationId>, Vec<InoSize>)> {
        let keys = elements.iter().map(|meta|{
            Key::from(self.fs_config.key_builder().inode_x(ino, *meta).buf)
        }).collect::<Vec<_>>();
        let data = self.f_txn.batch_get(keys).await?;
        let mut existing_hash = Vec::new();
        let mut change_iter_id = Vec::new();
        let mut size = Vec::new();
        for KvPair(k, v) in data {
            match self.fs_config.key_parser_b(k)?.parse_ino()?.meta {
                InoMetadata::FullHash => {
                    existing_hash.push(v);
                }
                InoMetadata::ChangeIterationId => {
                    let _ = deserialize_json::<InoChangeIterationId>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoChangeIteration: {err:?}");
                        })
                        .map(|id| change_iter_id.push(id));
                }
                InoMetadata::Size => {
                    let _ = deserialize_json::<InoSize>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoSize: {err:?}");
                        })
                        .map(|s| size.push(s));
                }
                _ => {}
            }
        }
        Ok((existing_hash, change_iter_id, size))
    }
}

// ================================ INTERFACE ==============================================






// ================================ INTERFACE ==============================================

#[async_trait::async_trait]
impl HashFsInterface for TikvBasedHashFs {

    async fn init(&self, gid: u32, uid: u32) -> HashFsResult<StorageDirItem> {
        tracing::info!("initializing tifs on {:?} ...", self.f_txn.get_endpoints());
        let attr = self.get_or_make_dir(
                PARENT_OF_ROOT_INODE,
                Default::default(),
                StorageFilePermission(0o777),
                gid,
                uid,
            ).await?;
        let (status, item) = attr.unpack();
        if status == GotOrMadePure::NewlyCreated {
            tracing::info!("blank fs, new root node: {:?}", &item);
        } else {
            tracing::info!("use existing fs, root node: {:?}", &item);
        }
        Ok(item)
    }

    async fn meta_static_read(&self) -> HashFsResult<MetaStatic> {
        let key = Key::from(self.fs_config.key_builder().meta_static());
        let data = self.f_txn.get(key).await?.ok_or(HashFsError::FsNotInitialized)?;
        let result = MetaStatic::deserialize(&data).map_err(|err|{
            HashFsError::FsHasInvalidData(Some(format!("reading static metadata: {err:?}")))
        })?;
        Ok(result)
    }

    async fn directory_read_children(&self, dir_ino: StorageIno) -> HashFsResult<Vec<DirectoryItem>> {
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_scan_for_children(
                dir_ino, MAX_TIKV_SCAN_LIMIT).await;
            if let Some(result) =
                started.finish(r1).await { break Ok(result?); }
        }
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

        let new_ino = self.meta_mutable_reserve_new_ino().await?;

        let mut spin = self.f_txn.spinning_mini_txn().await?;
        let r = loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_add_child_checked_new_inode(
                parent, name.clone(), typ, perm, gid, uid, rdev, inline_data.clone(), new_ino).await;
            if let Some(r) = started.finish(r1).await { break r?; }
        };

        Ok(r)
    }

    async fn directory_add_child_checked_existing_inode(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
        ino: StorageIno,
    ) -> HashFsResult<()> {
        if parent < ROOT_INODE {
            return Ok(());
        }

        let mut mini = self.f_txn.spinning_mini_txn().await?;
        let r: GotOrMade<StorageDirItem> = loop {
            let mut started = mini.start().await?;
            let r1 = started
                .directory_add_child_checked_existing_inode(parent, name.clone(), ino).await;
            if let Some(result) = started.finish(
                r1).await { break result?; }
        };

        if let GotOrMade::ExistedAlready(_existing) = r {
            Err(HashFsError::FileAlreadyExists)
        } else {
            Ok(())
        }
    }

    async fn directory_add_new_symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: ParentStorageIno,
        name: ByteString,
        link: ByteString,
    ) -> HashFsResult<StorageDirItem> {

        let link_data_vec = link.as_bytes().to_vec();
        let r = self.directory_add_child_checked_new_inode(
            parent,
            name,
            StorageDirItemKind::Symlink,
            StorageFilePermission(0o777),
            gid, uid, 0,
            Some(link_data_vec),
        ).await?;

        if let GotOrMade::NewlyCreated(v) = r {
            Ok(v)
        } else {
            Err(HashFsError::FileAlreadyExists)
        }
    }

    async fn directory_remove_child_file(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()> {
        let allowed_types = HashSet::from([
            StorageDirItemKind::File,
            StorageDirItemKind::Symlink,
        ]);

        self.directory_remove_child_generic(parent, name, allowed_types).await
    }

    async fn directory_remove_child_directory(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<()> {
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        let _unlinked_item: StorageDirItem = loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_remove_child_empty_directory(
                parent, name.clone()).await;
            if let Some(r) = started.finish(r1).await
            { break r?; }
        };
        Ok(())
    }

    async fn directory_rename_child(
        &self,
        parent: ParentStorageIno,
        raw_name: ByteString,
        new_parent: ParentStorageIno,
        new_raw_name: ByteString,
    ) -> HashFsResult<()> {
        check_file_name(&raw_name)?;
        check_file_name(&new_raw_name)?;
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_rename_child(
                parent, raw_name.clone(), new_parent, new_raw_name.clone()).await;
            if let Some(result) = started.finish(r1).await { break result?; }
        }
        Ok(())
    }

    async fn directory_child_get_all_attributes(
        &self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        let entry_opt = loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_get_child(
                parent, name.clone()).await;
            if let Some(result) = started.finish(
                r1).await { break result?; }
        };
        let entry = entry_opt.ok_or(HashFsError::FileNotFound)?;
        self.inode_get_all_attributes(entry.ino).await
    }

    async fn inode_get_all_attributes(
        &self,
        ino: StorageIno,
    ) -> HashFsResult<(Arc<InoDescription>, Arc<InoStorageFileAttr>, Arc<InoSize>, InoAccessTime)> {
        let desc: Arc<InoDescription> = self.f_txn.fetch(&ino).await?;
        let attr: Arc<InoStorageFileAttr> = self.f_txn.fetch(&ino).await?;
        let size: Arc<InoSize> = self.f_txn.fetch(&ino).await?;
        let atime: InoAccessTime = self.f_txn.fetch(&ino).await.ok().as_deref().cloned().unwrap_or_else(||{
                InoAccessTime(size.last_change.max(attr.last_change))
            });
        Ok((desc, attr, size, atime))
    }

    async fn inode_set_all_attributes(
        &self,
        ino: StorageIno,
        mode: Option<StorageFilePermission>,
        uid: Option<u32>,
        gid: Option<u32>,
        _size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> HashFsResult<()> {

        if let Some(atime_modification) = atime {
            let new_atime = InoAccessTime(get_time_from_time_or_now(atime_modification));
            self.f_txn.put_json(&ino, Arc::new(new_atime)).await?;
        }

        if let Some(mtime_modification) = mtime {
            let new_mtime = ModificationTime(get_time_from_time_or_now(mtime_modification));
            self.f_txn.put_json(&ino, Arc::new(new_mtime)).await?;
        }

        // TODO: how to deal with fh, chgtime, bkuptime?
        let ino_attr_arc: Arc<InoStorageFileAttr> = self.f_txn.fetch(&ino).await?;
        let mut ino_attr = ino_attr_arc.deref().clone();

        let mut cnt = 0 as usize;
        set_if_changed(&mut cnt, &mut ino_attr.perm, mode);

        set_if_changed(&mut cnt, &mut ino_attr.uid, uid);
        set_if_changed(&mut cnt, &mut ino_attr.gid, gid);

        set_if_changed(&mut cnt, &mut ino_attr.last_change,
            Some(ctime.unwrap_or(SystemTime::now())));
        set_if_changed(&mut cnt, &mut ino_attr.flags, flags);

        if cnt > 0 {
            self.f_txn.put_json(&ino, Arc::new(ino_attr)).await?;
        }

        Ok(())
    }

    async fn inode_open(&self, ino: StorageIno) -> HashFsResult<Uuid> {
        let use_id = Uuid::new_v4();
        // publish opened state
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut mini = spin.start().await?;
            let r1 = mini.inode_open(ino, use_id).await;
            if let Some(r) = mini.finish(r1).await { break r?; }
        }
        Ok(use_id)
    }

    async fn inode_close(&self, ino: StorageIno, use_id: Uuid) -> HashFsResult<()> {
        // de-publish opened state
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut mini = spin.start().await?;
            let r1 = mini.inode_close(ino, use_id).await;
            if let Some(r) = mini.finish(r1).await { break r?; }
        }
        Ok(())
    }

    async fn inode_allocate_size(
        &self,
        ino: StorageIno,
        offset: i64,
        length: i64,
    ) -> HashFsResult<()> {
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.inode_allocate_size(ino, offset, length).await;
            if let Some(result) = started.finish(r1).await
            { break result?; }
        }
        Ok(())
    }

    async fn inode_read_inline_data(&self, ino: StorageIno) -> HashFsResult<Vec<u8>> {
        let ino_inline_data: Arc<InoInlineData> = self.f_txn.fetch(&ino).await?;
        Ok(ino_inline_data.inlined.clone())
    }

    async fn inode_read_block_hashes_data_range(
        &self,
        ino: StorageIno,
        start_addr: u64,
        read_size: u64,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>>
    {
        let end_addr = start_addr + read_size;
        let start_block = start_addr.div_floor(self.fs_config.block_size);
        let end_block = end_addr.div_ceil(self.fs_config.block_size); // end block is exclusive
        let block_range = BlockIndex(start_block)..BlockIndex(end_block);

        let block_hashes = self.hb_get_block_hash_list_by_block_range(
            ino, block_range).await?;
        Ok(block_hashes)
    }

    async fn inode_read_block_hashes_block_range(
        &self,
        ino: StorageIno,
        block_ranges: &[Range<BlockIndex>],
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>> {

        let mut range_collection = RangeSet2::empty();
        for block_range in block_ranges {
            let range = RangeSet2::from(block_range.start.0..block_range.end.0);
            range_collection.union_with(range.as_ref());
        }

        let mut block_hashes_all: Option<BTreeMap<BlockIndex, Vec<u8>>> = None;
        for range_sr in range_collection.iter() {
            let RangeSetRange::Range(range) = range_sr else {
                panic!("unsupported range: {:?}", range_sr);
            };
            let block_hashes = self
                .hb_get_block_hash_list_by_block_range_chunked(ino,
                    BlockIndex(*range.start)..BlockIndex(*range.end) ).await?;
            if let Some(hm) = &mut block_hashes_all {
                hm.extend(block_hashes.into_iter());
            } else {
                block_hashes_all = Some(block_hashes);
            }
        }

        Ok(block_hashes_all.unwrap_or_default())
    }

    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        ino: StorageIno,
        blocks: &[(&TiFsHash, u64, Vec<BlockIndex>)],
    ) -> HashFsResult<()> {
        let mut watch = AutoStopWatch::start("pm_register");

        let mut max_file_size = 0;
        let mut addresses_to_modify = Vec::new();
        let _ = blocks.iter().map(|(h,l,ids)| {
            for id in ids {
                let addr = BlockAddress{
                    ino, index: *id,
                };
                addresses_to_modify.push((addr, Some(*h)));
                max_file_size = max_file_size.max(id.0 * self.fs_config.block_size + l);
            }
            ()
        }).collect::<Vec<()>>();

        let mut spin = MiniTransaction::new(&self.f_txn).await?;
        let prev_hash_decrements = loop {
            let mut started = spin.start().await?;
            let r1 = started
                .hb_replace_block_hash_for_address_no_size_update(
                    &addresses_to_modify).await;
            if let Some(result) = started.finish(r1).await
            { break result?; }
        };
        drop(spin);

        watch.sync("replace");

        // clear full file hash:
        {
            let full_hash_key = self.fs_config.key_builder().inode_x(
                ino, super::key::InoMetadata::FullHash).buf;
            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            loop {
                let lock = self.local_ino_locks_clear_full_hash
                    .lock_write(&ino).await;
                let mut started = spin.start().await?;
                let r1 = started.mini.delete(full_hash_key.clone()).await;
                if let Some(result) = started.finish(
                    r1).await { break result?; }
                drop(lock);
            };
        }

        // change change iter id:
        {
            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            loop {
                let lock = self.local_ino_locks_update_change_iter
                    .lock_write(&ino).await;
                let mut started = spin.start().await?;
                let r1 = started.put(
                    &ino, Arc::new(InoChangeIterationId::random())).await;
                if let Some(result) = started.finish(
                    r1).await { break result?; }
                drop(lock);
            };
        }

        watch.sync("clear_hash");

        let do_size_update = true;
        if do_size_update {
            let size_peek_arc: Arc<InoSize> = self.f_txn.fetch(&ino).await?;
            let mut size_peek = size_peek_arc.deref().clone();
            if max_file_size > size_peek.size {
                size_peek.set_size(max_file_size, self.fs_config.block_size);
                let mut spin = MiniTransaction::new(&self.f_txn).await?;
                loop {
                    let lock = self.local_ino_write_size_locks.lock_write(&ino).await;
                    let mut started = spin.start().await?;
                    let r1 = started
                        .hb_replace_block_hash_for_address_only_size_update_b(
                            ino, max_file_size).await;
                    if let Some(result) = started.finish(
                        r1).await { break result?; }
                    drop(lock);
                };
            }
        }

        watch.sync("size_update");

        let prev_hash_decrements_ref = prev_hash_decrements.iter().map(|(h,dec)|{
            (h, *dec)
        }).collect::<HashMap<_,_>>();
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started
                .hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
                    &prev_hash_decrements_ref).await;
            if let Some(result) = started.finish(r1).await
            { break result?; }
        }

        watch.sync("dec");

        Ok(())
    }

    async fn file_get_hash(&self, ino: StorageIno) -> HashFsResult<Vec<u8>>
    {
        loop {
            let (hash, _, _)
                = self.inode_read_metadata_multi(ino, &[InoMetadata::FullHash]).await?;

            if let Some(hash) = hash.into_iter().next()  {
                return Ok(hash);
            }

            // avoid multiple times hash computation (at least on the same instance)
            let lock = self.local_ino_locks_full_hash.lock_write(&ino).await;

            let (hash, iter, size)
                = self.inode_read_metadata_multi(ino, &[
                    InoMetadata::FullHash,
                    InoMetadata::ChangeIterationId,
                    InoMetadata::Size,
                    ]).await?;

            if let Some(hash) = hash.into_iter().next() {
                return Ok(hash);
            }

            let Some(size) = size.into_iter().next() else {
                return Err(HashFsError::FileNotFound);
            };

            let change_iter_id = iter.into_iter().next().unwrap_or_default();

            let size_in_blocks = size.size().div_ceil(self.fs_config.block_size);
            let block_range = BlockIndex(0)..BlockIndex(
                size_in_blocks * self.fs_config.hash_len as u64);
            let hash_data = self.read_hashes_of_file(ino, block_range).await?;
            let new_full_hash = self.fs_config.calculate_hash(&hash_data);
            tracing::trace!("freshly_calculated_hash: {:x?}", &new_full_hash);

            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            let result = loop {
                let mut mini = spin.start().await?;
                let r1 = mini.checked_write_of_file_hash(
                    &ino, &change_iter_id, new_full_hash.clone()).await;
                if let Some(r) = mini.finish(r1).await { break r?; }
            };
            drop(lock);

            if result {
                return Ok(new_full_hash);
            }
        }
    }

    async fn file_read_block_hashes(&self, ino: StorageIno, block_range: Range<BlockIndex>
    ) -> HashFsResult<Vec<u8>> {
        self.read_hashes_of_file(ino, block_range).await
    }

    async fn hb_get_block_data_by_hashes(
        &self,
        hashes: &HashSet<&TiFsHash>,
    ) -> HashFsResult<HashMap<TiFsHash, Arc<Vec<u8>>>> {
        let mut keys = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let key = self.fs_config.key_builder().hashed_block(*hash);
            keys.push(Key::from(key));
        }
        let rcv_data_list = self.f_txn.batch_get(keys).await?;
        let mut hashed_block_data = HashMap::with_capacity(rcv_data_list.len());
        for KvPair(k, v) in rcv_data_list {
            let hash = self.fs_config.key_parser_b(k)?.parse_key_hashed_block()?;
            let value = Arc::new(v);
            hashed_block_data.insert(hash.clone(), value.clone());
        }
        Ok(hashed_block_data)
    }

    async fn hb_increment_reference_count(
        &self,
        blocks: &[(&TiFsHash, u64)],
    ) -> HashFsResult<HashMap<TiFsHash, BigUint>> {
        // get and increment block reference counter (with automatic retry)
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        let prev_cnt = loop {
            let mut started = spin.start().await?;
            let r1 = started.hb_increment_blocks_reference_count(
                blocks).await;
            if let Some(result) = started.finish(r1).await
            { break result?; };
        };
        Ok(prev_cnt)
    }

    async fn hb_upload_new_block(&self, blocks: &[(&TiFsHash, Arc<Vec<u8>>)]) -> HashFsResult<()> {
        let mutations = blocks.iter().map(|(h,d)|{
            let key = self.fs_config.key_builder().hashed_block(h);
            Mutation::Put(key.into(), d.deref().clone())
        }).collect::<Vec<_>>();

        self.f_txn.batch_mutate(mutations).await?;
        Ok(())
    }
} // interface impl end

impl From<HashFsError> for FsError {
    fn from(value: HashFsError) -> Self {
        match value {
            HashFsError::RawGrpcStatus(status) => FsError::UnknownError(format!("Error: GRPC Status: {status:?}")),
            HashFsError::FsNotInitialized => FsError::ConfigParsingFailed { msg: format!("Fs not initialized.") },
            HashFsError::FsHasInvalidData(msg) => FsError::UnknownError(format!("msg: {msg:?}")),
            HashFsError::FileNotFound => FsError::FileNotFound { file: format!("undefined") },
            HashFsError::Unspecific(msg) => FsError::UnknownError(msg),
            HashFsError::FileAlreadyExists => FsError::FileExist { file: format!("undefined") },
            HashFsError::InodeHasNoInlineData => FsError::WrongFileType,
            HashFsError::GrpcMessageIncomplete => FsError::GrpcMessageIncomplete,
            HashFsError::RawTonicTransportError(err) => FsError::UnknownError(format!("RawTonicTransportError: {:?}", err)),
        }
    }
}

impl From<FsError> for HashFsError {
    fn from(value: FsError) -> Self {
        match value {
            FsError::InodeNotFound { inode: _ } => HashFsError::FileNotFound,
            FsError::FileNotFound { file: _ } => HashFsError::FileNotFound,
            other => HashFsError::Unspecific(format!("FsError: {other:?}")),
        }
    }
}
