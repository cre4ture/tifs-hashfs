
use std::ops::{Deref, Range};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use bytestring::ByteString;
use fuser::TimeOrNow;
use num_bigint::BigUint;
use strum::IntoEnumIterator;
use tikv_client::{Key, KvPair};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use uuid::Uuid;

use crate::fs::hash_fs_interface::GotOrMadePure;
use crate::fs::inode::TiFsHash;
use crate::fs::key::{BlockAddress, PARENT_OF_ROOT_INODE};
use crate::fs::transaction::MAX_TIKV_SCAN_LIMIT;
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

use super::error::FsError;
use super::mini_transaction::{DeletionCheckResult, MiniTransaction};
use super::utils::stop_watch::AutoStopWatch;
use super::utils::txn_data_cache::{TxnFetch, TxnPut};
use super::{
    error::TiFsResult, flexible_transaction::FlexibleTransaction, fs_config::TiFsConfig, meta::MetaStatic
    };
use super::key::{check_file_name, InoMetadata, KeyParser, ScopedKeyBuilder, ROOT_INODE};
use super::hash_fs_interface::{
        BlockIndex, GotOrMade, HashFsError, HashFsInterface, HashFsResult};
use super::inode::{InoAccessTime, DirectoryItem, InoDescription, InoSize, ModificationTime, ParentStorageIno, StorageDirItem, StorageDirItemKind, InoStorageFileAttr, StorageFilePermission, StorageIno};


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
    pub local_ino_write_locks: Arc<RwLock<HashMap<StorageIno, Weak<RwLock<()>>>>>,
}

impl TikvBasedHashFs {
    pub fn new_arc(fs_config: TiFsConfig, f_txn: FlexibleTransaction) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            Self {
                weak: weak.clone(),
                f_txn,
                fs_config,
                local_ino_write_locks: Arc::new(RwLock::new(HashMap::new())),
            }
        })
    }

    pub fn key_builder(&self) -> ScopedKeyBuilder {
        ScopedKeyBuilder::new(&self.fs_config.key_prefix)
    }

    pub fn key_parser<'fl>(&'fl self, i: &'fl mut std::slice::Iter<'fl, u8>) -> TiFsResult<KeyParser<'fl>> {
        KeyParser::start(i, &self.fs_config.key_prefix, self.fs_config.hash_len)
    }

    pub async fn ino_lock_get(&self, ino: StorageIno) -> Arc<RwLock<()>> {
        let mut locks = self.local_ino_write_locks.write().await;
        let existing_lock = locks.get(&ino).and_then(|x|x.upgrade());
        if let Some(lock) = existing_lock {
            lock
        } else {
            let lock = Arc::new(RwLock::new(()));
            locks.insert(ino, Arc::downgrade(&lock));
            lock
        }
    }

    pub async fn ino_lock_lock_write(&self, ino: StorageIno) -> OwnedRwLockWriteGuard<()> {
        let lock = self.ino_lock_get(ino).await;
        let wl = lock.write_owned().await;
        wl
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
                let key = self.key_builder().inode_x(unlinked_item.ino, meta).as_key();
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
        let range = self.key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.f_txn.scan(
                range,
                block_range.count() as u32,
            )
            .await?;
        let mut result = BTreeMap::<BlockIndex, TiFsHash>::new();
        for KvPair(k, hash) in iter.into_iter() {
            let key_vec = Vec::from(k);
            let mut i = key_vec.iter();
            let key = self.key_parser(&mut i)?.parse_key_block_address()?;
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
                    &addr, None).await;
            if let Some(result) = started.finish(
                r1).await { break result?; }
        };

        if do_size_update {
            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            loop {
                let lock = self.ino_lock_lock_write(addr.ino).await;
                let mut started = spin.start().await?;
                let r1 = started
                    .hb_replace_block_hash_for_address_only_size_update(
                        &addr, 0).await;
                if let Some(result) = started.finish(
                    r1).await { break result?; }
                drop(lock);
            };
        }

        let Some(prev_h) = prev_hash else {
            return Ok(());
        };

        let mut spin = MiniTransaction::new(&self.f_txn).await?;
        loop {
            let mut started = spin.start().await?;
            let r1 = started.hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
                &prev_h, 1).await;
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
        let key = Key::from(self.key_builder().meta_static());
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
        let ino_size: Arc<InoSize> = self.f_txn.fetch(&ino).await?;
        ino_size.inline_data().cloned().ok_or(HashFsError::InodeHasNoInlineData)
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
        block_range: Range<BlockIndex>,
    ) -> HashFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let block_hashes = self.hb_get_block_hash_list_by_block_range_chunked(ino, block_range).await?;
        Ok(block_hashes)
    }

    async fn inode_write_hash_block_to_addresses_update_ino_size_and_cleaning_previous_block_hashes(
        &self,
        ino: StorageIno,
        block_hash: TiFsHash,
        blocks_size: u64,
        addresses: Vec<BlockIndex>,
    ) -> HashFsResult<()> {
        let mut watch = AutoStopWatch::start("pm_register");

        let mut decrement_cnts = HashMap::<TiFsHash, u64>::new();
        for index in addresses {
            let addr = BlockAddress{ ino, index };
            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            let prev_hash = loop {
                let lock = self.ino_lock_lock_write(ino).await;
                let mut started = spin.start().await?;
                let r1 = started
                    .hb_replace_block_hash_for_address_no_size_update(
                        &addr, Some(&block_hash)).await;
                if let Some(result) = started.finish(r1).await
                { break result?; }
                drop(lock);
            };

            watch.sync("replace");

            let do_size_update = true;
            if do_size_update {
                let mut spin = MiniTransaction::new(&self.f_txn).await?;
                loop {
                    let lock = self.ino_lock_lock_write(addr.ino).await;
                    let mut started = spin.start().await?;
                    let r1 = started
                        .hb_replace_block_hash_for_address_only_size_update(
                            &addr, blocks_size).await;
                    if let Some(result) = started.finish(
                        r1).await { break result?; }
                    drop(lock);
                };
            }

            watch.sync("size_update");

            if let Some(pre_hash) = prev_hash {
                decrement_cnts.insert(pre_hash.clone(),
                    1 + decrement_cnts.get(&pre_hash).copied().unwrap_or(0));
            }
        }

        for (prev_hash, dec_cnt) in decrement_cnts {
            let mut spin = self.f_txn.spinning_mini_txn().await?;
            loop {
                let mut started = spin.start().await?;
                let r1 = started
                    .hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
                        &prev_hash, dec_cnt).await;
                if let Some(result) = started.finish(r1).await
                { break result?; }
            }

            watch.sync("dec");
        }

        Ok(())
    }

    async fn file_get_hash(&self, ino: StorageIno) -> HashFsResult<Vec<u8>>
    {
        let mut ino_size_arc: Arc<InoSize> = self.f_txn.fetch(&ino).await?;
        while ino_size_arc.data_hash.is_none() {
            let change_iteration = ino_size_arc.change_iteration;
            let size_in_blocks = ino_size_arc.size().div_ceil(self.fs_config.block_size);
            let block_range = BlockIndex(0)..BlockIndex(
                size_in_blocks * self.fs_config.hash_len as u64);
            let hash_data = self.read_hashes_of_file(ino, block_range).await?;
            let new_full_hash = self.fs_config.calculate_hash(&hash_data);
            tracing::trace!("freshly_calculated_hash: {:x?}", &new_full_hash);

            let mut spin = MiniTransaction::new(&self.f_txn).await?;
            let result = loop {
                let mut mini = spin.start().await?;
                let r1 = mini.checked_write_of_file_hash(
                    &ino, change_iteration, new_full_hash.clone()).await;
                if let Some(r) = mini.finish(r1).await { break r?; }
            };

            if let Some(new_ino_size) = result {
                ino_size_arc = new_ino_size;
            }
        }

        Ok(ino_size_arc.data_hash.to_owned().unwrap())
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
            let key = self.key_builder().hashed_block(*hash);
            keys.push(Key::from(key));
        }
        let rcv_data_list = self.f_txn.batch_get(keys).await?;
        let mut hashed_block_data = HashMap::with_capacity(rcv_data_list.len());
        for KvPair(k, v) in rcv_data_list {
            let key_data = Vec::from(k);
            let mut i = key_data.iter();
            let hash = self.key_parser(&mut i)?.parse_key_hashed_block()?;
            let value = Arc::new(v);
            hashed_block_data.insert(hash.clone(), value.clone());
        }
        Ok(hashed_block_data)
    }

    async fn hb_increment_reference_count(&self, hash: &TiFsHash, cnt: u64) -> HashFsResult<BigUint> {

        let block_ref_cnt_key = self.key_builder().named_hashed_block_x(
            hash, Some(super::key::HashedBlockMeta::CCountedNamedUsages), None);

        // get and increment block reference counter (with automatic retry)
        let mut spin = self.f_txn.spinning_mini_txn().await?;
        let prev_cnt: BigUint = loop {
            let mut started = spin.start().await?;
            let r1 = started.hb_increment_blocks_reference_count(
                block_ref_cnt_key.clone(), cnt).await;
            if let Some(result) = started.finish(r1).await
            { break result?; };
        };
        Ok(prev_cnt)
    }

    async fn hb_upload_new_block(&self, block_hash: TiFsHash, data: Vec<u8>) -> HashFsResult<()> {
        let key = self.key_builder().hashed_block(&block_hash);
        self.f_txn.put(key, data).await?;
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
