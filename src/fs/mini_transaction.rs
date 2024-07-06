use std::{any::type_name, collections::{BTreeMap, HashMap, HashSet}, fmt::Debug, ops::{Deref, DerefMut, Range}, sync::Arc, time::SystemTime};

use bytestring::ByteString;
use num_bigint::BigUint;
use num_traits::{FromPrimitive, Zero};
use serde::{Deserialize, Serialize};
use tikv_client::{transaction::Mutation, Key, KvPair, Transaction};
use tokio::time::sleep;
use uuid::Uuid;

use crate::fs::{inode::ParentStorageIno, meta::MetaMutable};

use super::{dir::StorageDirectory, error::{FsError, TiFsResult}, inode::{DirectoryItem, InoChangeIterationId, InoFullHash, InoInlineData, TiFsHash}};
use super::utils::txn_data_cache::{TxnDeleteMut, TxnFetchMut, TxnPutMut};
use super::key::{BlockAddress, HashedBlockMeta, OPENED_INODE_PARENT_INODE};
use super::hash_fs_interface::{BlockIndex, GotOrMade};
use super::fuse_to_hashfs::MAX_TIKV_SCAN_LIMIT;
use super::index::{deserialize_json, serialize_json};
use super::fs_config::TiFsConfig;
use super::flexible_transaction::{FlexibleTransaction, SpinningTxn, TransactionError, TransactionResult};
use super::meta::MetaStatic;
use super::key::{KeyGenerator, ScopedKeyBuilder};
use super::inode::{InoDescription, InoSize, StorageDirItem, StorageDirItemKind, InoStorageFileAttr, StorageFilePermission, StorageIno};

#[derive(PartialEq, Eq, Debug)]
pub enum DeletionCheckResult {
    StillUsed,
    DeletedInoDesc,
}

pub struct MiniTransaction {
    pub txn_client_mux: Arc<super::transaction_client_mux::TransactionClientMux>,
    fs_config: TiFsConfig,
    spin: SpinningTxn,
}

pub struct StartedMiniTransaction<'mmt_l> {
    parent: &'mmt_l mut MiniTransaction,
    txn: TransactionWithFsConfig,
}

pub struct TransactionWithFsConfig {
    pub fs_config: TiFsConfig,
    pub mini: Transaction,
}

impl MiniTransaction {
    pub async fn new(
        txn_client_mux: Arc<super::transaction_client_mux::TransactionClientMux>,
        fs_config: TiFsConfig,
    ) -> TiFsResult<Self> {
        Ok(Self{
            txn_client_mux,
            fs_config,
            spin: SpinningTxn::default(),
        })
    }

    pub fn fs_config(&self) -> &TiFsConfig {
        &self.fs_config
    }

    pub async fn start_test<'fl, 'mol>(&'mol mut self) -> &'fl i32 {
        tracing::trace!("hello!");
        &1
    }

    pub async fn start<'mol>(&'mol mut self) -> TiFsResult<StartedMiniTransaction<'mol>>
    {
        let mini_res = loop {
            let r = self.txn_client_mux.clone().single_action_txn_raw().await;
            match r {
                Ok(mini) => break mini,
                Err(e) => {
                    if let Some(delay) = self.spin.backoff.next_delay_duration() {
                        sleep(delay).await;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        };

        let txn = TransactionWithFsConfig{
            fs_config: self.fs_config().clone(),
            mini: mini_res,
        };

        Ok(StartedMiniTransaction::<'mol>{
            parent: self,
            txn,
        })
    }

    pub async fn start_snapshot_read_only(
        &mut self
    ) -> TiFsResult<TransactionWithFsConfig>
    {
        let mini_res = loop {
            let r = FlexibleTransaction::get_snapshot_read_transaction(
                self.txn_client_mux.clone()).await;
            match r {
                Ok(mini) => break mini,
                Err(e) => {
                    if let Some(delay) = self.spin.backoff.next_delay_duration() {
                        sleep(delay).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        Ok(TransactionWithFsConfig{
            fs_config: self.fs_config().clone(),
            mini: mini_res,
        })
    }
}

impl<'pl> StartedMiniTransaction<'pl> {

    pub async fn finish<'fl, R>(
        mut self,
        result: std::result::Result<R, impl Into<TransactionError>>
    ) -> Option<TiFsResult<R>> {
        match result {
            Ok(val) => {
                match self.mini.commit().await {
                    Ok(_t) => return Some(Ok(val)),
                    Err(error) => {
                        if let Some(delay) = self.parent.spin.backoff.next_delay_duration() {
                            sleep(delay).await;
                            tracing::info!("retry commit failed transaction. Type: {}, Reason: {error:?}", std::any::type_name::<R>());
                            return None;
                        } else {
                            tracing::warn!("transaction failed. Type: {}, Reason: {error:?}", std::any::type_name::<R>());
                            return Some(Err(error.into()));
                        }
                    }
                }
            }
            Err(result_err) => {
                if let Err(error) = self.mini.rollback().await {
                    tracing::error!("failed to rollback mini transaction. Type: {}, Reason: {error:?}", std::any::type_name::<R>());
                }
                match result_err.into() {
                    TransactionError::PersistentIssue(err) => {
                        tracing::error!("cancelling transaction retry due to persistent error. Type: {}, Reason: {err:?}", std::any::type_name::<R>());
                        return Some(Err(err));
                    }
                    TransactionError::TemporaryIssue(err) => {
                        if let Some(delay) = self.parent.spin.backoff.next_delay_duration() {
                            sleep(delay).await;
                            tracing::info!("retry rolled back transaction. Type: {}, Reason: {err:?}", std::any::type_name::<R>());
                            return None;
                        } else {
                            tracing::warn!("transaction failed. Type: {}, Reason: {err:?}", std::any::type_name::<R>());
                            return Some(Err(err));
                        }
                    }
                }
            }
        }
    }

    pub fn fs_config(&self) -> &TiFsConfig {
        &self.parent.fs_config()
    }
}

impl<'mmtl> Deref for StartedMiniTransaction<'mmtl> {
    type Target = TransactionWithFsConfig;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl<'mmtl> DerefMut for StartedMiniTransaction<'mmtl> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

impl TransactionWithFsConfig {

    pub fn fs_config(&self) -> &TiFsConfig {
        &self.fs_config
    }

    pub async fn meta_static_mutable_init(&mut self) -> TiFsResult<()> {
        let meta_static = Arc::new(MetaStatic {
            block_size: self.fs_config().block_size as u64,
            hashed_blocks: self.fs_config().hashed_blocks,
            hash_algorithm: self.fs_config().hash_algorithm.to_string(),
        });
        self.put(&(), meta_static).await?;
        let initial_mutable_meta = MetaMutable {
            inode_next: crate::fs::key::FIRST_DATA_INODE.0,
            last_stat: None,
        };
        self.put(&(), Arc::new(initial_mutable_meta)).await
    }

    pub async fn meta_mutable_reserve_new_ino(&mut self) -> TiFsResult<StorageIno> {
        self.meta_mutable_reserve_new_inos(1).await
    }

    pub async fn meta_mutable_reserve_new_inos(&mut self, count: u64) -> TiFsResult<StorageIno> {
        let read_meta: Option<MetaMutable> = self.fetch_try(&()).await?;

        let Some(mut dyn_meta) = read_meta else {
            tracing::info!("dyn meta data is missing -> assume fresh FS. Initialize on the fly.");
            self.meta_static_mutable_init().await?;

            // the first call to this function for a fresh filesystem is about
            // creating the root inode data. Thus we return the pre-defined ROOT_INODE (NR 1).
            return Ok( crate::fs::key::ROOT_INODE.0 );
        };

        let ino = dyn_meta.inode_next;
        dyn_meta.inode_next += count;

        self.put(&(), Arc::new(dyn_meta)).await?;
        tracing::info!("reserved new ino: {}", ino);
        Ok(StorageIno(ino))
    }

    pub async fn directory_get_child(&mut self, parent: ParentStorageIno, name: ByteString
    ) -> TiFsResult<Option<StorageDirItem>> {
        tracing::trace!("get dir item. parent {:?}, name {:?}", parent, name);
        self.fetch_try(&(parent, name.as_bytes().deref())).await
    }

    pub async fn directory_scan_for_children(&mut self, dir_ino: StorageIno, limit: u32
    ) -> TiFsResult<Vec<DirectoryItem>> {
        let range = self.fs_config().key_builder().directory_child_range(dir_ino);
        let data = self.mini.scan(range, limit).await?.collect::<Vec<_>>();
        let mut result = StorageDirectory::with_capacity(data.len());
//        data.iter().enumerate().map(|(i, KvPair(k,v))|{
//            trace!("scan result key #{i}: {:?}, data-len: {}", k, v.len());
//        }).fold((), |_,_|{});
        for (_i, KvPair(k,v)) in data.into_iter().enumerate() {
            let (_p_ino, name) = self.fs_config().key_parser_b(k)?
                .parse_directory_child()?;
            let item = deserialize_json::<StorageDirItem>(&v)?;
            result.push(DirectoryItem {
                ino: item.ino,
                name: String::from_utf8_lossy(&name).to_string(),
                typ: item.typ,
            });
        }
        //trace!("read dir data: len: {}", result.len());
        Ok(result)
    }

    pub async fn directory_add_child_link_unchecked(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        item: Arc<StorageDirItem>,
    ) -> TiFsResult<()> {
        self.put(&(item.ino, parent, name.as_bytes().deref()), Arc::new(())).await?;
        self.put(&(parent, name.as_bytes().deref()), item).await?;
        Ok(())
    }

    pub async fn directory_add_child_checked_existing_inode(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        ino: StorageIno
    ) -> TiFsResult<GotOrMade<StorageDirItem>> {

        let existing = self.directory_get_child(parent, name.clone()).await?;
        if let Some(existing) = existing {
            return Ok(GotOrMade::ExistedAlready(existing));
        }

        let ino_desc: Arc<InoDescription> = self.fetch(&ino).await?;
        let item = Arc::new(StorageDirItem { ino, typ: ino_desc.typ });
        self.directory_add_child_link_unchecked(parent, name, item).await?;
        Ok(GotOrMade::NewlyCreated(StorageDirItem{
            ino,
            typ: ino_desc.typ,
        }))
    }

    pub async fn directory_add_child_checked_new_inode(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        typ: StorageDirItemKind,
        perm: StorageFilePermission,
        gid: u32,
        uid: u32,
        rdev: u32,
        inline_data: Option<Vec<u8>>,
        new_ino: StorageIno,
    ) -> TransactionResult<GotOrMade<StorageDirItem>> {

        let existing = self.directory_get_child(parent, name.clone()).await?;
        if let Some(existing) = existing {
            return Ok(GotOrMade::ExistedAlready(existing));
        }

        let ino_desc = Arc::new(InoDescription{
            ino: new_ino,
            creation_time: SystemTime::now(),
            typ,
        });
        let i_size = InoSize::new();
        let ino_size = Arc::new(i_size);
        let ino_attr = Arc::new(InoStorageFileAttr{
            perm,
            uid,
            gid,
            rdev,
            flags: 0,
            last_change: SystemTime::now(),
        });

        self.put(&new_ino, ino_desc.clone()).await?;
        self.put(&new_ino, ino_attr.clone()).await?;
        self.put(&new_ino, ino_size.clone()).await?;

        let item = Arc::new(StorageDirItem { ino: new_ino, typ });
        self.put(&(parent, name.as_bytes().deref()), item.clone()).await?;
        self.put(&(new_ino, parent, name.as_bytes().deref()), Arc::new(())).await?;

        if let Some(data) = inline_data {
            self.put(&new_ino, Arc::new(InoInlineData{
                inlined: data,
                last_change: SystemTime::now(),
            })).await?;
        }

        Ok(GotOrMade::NewlyCreated(item.deref().clone()))
    }

    pub async fn directory_remove_child_links_unchecked(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        child_ino: StorageIno,
    ) -> TiFsResult<()> {
        let key_parent_to_child = Key::from(self.fs_config().key_builder().generate_key(
            &(parent, name.as_bytes().deref())));
        let key_child_to_parent = Key::from(self.fs_config().key_builder().generate_key(
            &(child_ino, parent, name.as_bytes().deref())));
        let mut mutations = Vec::with_capacity(2);
        mutations.push(Mutation::Delete(key_parent_to_child));
        mutations.push(Mutation::Delete(key_child_to_parent));
        Ok(self.mini.batch_mutate(mutations).await?)
    }

    pub async fn directory_remove_child(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        allowed_types: &HashSet<StorageDirItemKind>,
    ) -> TiFsResult<StorageDirItem> {
        let Some(child_item) = self.directory_get_child(parent, name.clone()).await? else {
            return Err(FsError::FileNotFound { file: name.to_string() });
        };

        if !allowed_types.contains(&child_item.typ) {
            return Err(FsError::WrongFileType);
        }

        self.directory_remove_child_links_unchecked(parent, name, child_item.ino).await?;

        Ok(child_item)
    }

    pub async fn directory_remove_child_empty_directory(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
    ) -> TiFsResult<StorageDirItem> {
        let Some(child_item) = self.directory_get_child(parent, name.clone()).await? else {
            return Err(FsError::FileNotFound { file: name.to_string() });
        };

        if child_item.typ != StorageDirItemKind::Directory {
            return Err(FsError::WrongFileType);
        }

        let children = self.directory_scan_for_children(
            child_item.ino, 1 /* just check if its empty */).await?;
        if children.len() > 0 {
            return Err(FsError::DirNotEmpty { dir: name.to_string() });
        }

        self.directory_remove_child_links_unchecked(parent, name, child_item.ino).await?;

        Ok(child_item)
    }

    pub async fn directory_rename_child(
        &mut self,
        parent: ParentStorageIno,
        name: ByteString,
        new_parent: ParentStorageIno,
        new_name: ByteString,
    ) -> TiFsResult<()> {
        let dir_item = self.directory_get_child(parent, name.clone()).await?.ok_or(
            FsError::FileNotFound { file: name.clone().into() }
        )?;
        self.directory_add_child_checked_existing_inode(
            new_parent, new_name, dir_item.ino).await?;
        self.directory_remove_child(
            parent,
            name,
            &HashSet::from([dir_item.typ])
        ).await?;
        Ok(())
    }

    pub async fn inode_check_for_delete_and_delete_atomically_description(
        &mut self,
        ino: StorageIno
    ) -> TiFsResult<DeletionCheckResult> {
        // check for remaining usages:
        let key_usages = self.fs_config().key_builder().parent_link_scan(ino);
        let usage_count = self.mini.scan_keys(key_usages, MAX_TIKV_SCAN_LIMIT).await?.count();
        if usage_count == 0 {
            TxnDeleteMut::<StorageIno, InoDescription>::delete(self, &ino).await?;
            Ok(DeletionCheckResult::DeletedInoDesc)
        } else {
            Ok(DeletionCheckResult::StillUsed)
        }
    }

    pub async fn inode_open(
        &mut self,
        ino: StorageIno,
        use_id: Uuid,
    ) -> TiFsResult<()> {
        // check for existence
        let ino_desc: Arc<InoDescription> = self.fetch(&ino).await?;
        // publish opened state
        let used_id_str = use_id.to_string();
        let result = self.directory_add_child_checked_existing_inode(
            OPENED_INODE_PARENT_INODE,
            ByteString::from(used_id_str.clone()),
            ino_desc.ino
        ).await?;
        if result.existed_before() {
            return Err(FsError::FileExist { file: used_id_str });
        }
        Ok(())
    }

    pub async fn inode_close(
        &mut self,
        ino: StorageIno,
        use_id: Uuid,
    ) -> TiFsResult<()> {
        // remove published opened state
        let used_id_str = use_id.to_string();
        let result = self.directory_remove_child(
            OPENED_INODE_PARENT_INODE,
            ByteString::from(used_id_str.clone()),
            &HashSet::from([
                StorageDirItemKind::Directory,
                StorageDirItemKind::File,
                StorageDirItemKind::Symlink
            ]),
        ).await?;
        if result.ino != ino {
            // return err -> force rollback (un-delete)
            return Err(FsError::CloseFailedDueToWrongInoOrUseId);
        }
        Ok(())
    }

    pub async fn checked_write_of_file_hash(
        &mut self,
        ino: &StorageIno,
        change_iteration: &InoChangeIterationId,
        new_full_hash: TiFsHash,
    ) -> TiFsResult<bool> {
        let ino_change_id: Arc<InoChangeIterationId> = self.fetch(ino).await?;
        // was data changed in the meantime?
        if ino_change_id.as_ref() == change_iteration {
            self.put(ino, Arc::new(InoFullHash(new_full_hash))).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn inode_allocate_size(
        &mut self,
        ino: StorageIno,
        offset: i64,
        length: i64,
    ) -> TiFsResult<()> {
        let mut ino_size = TxnFetchMut::<StorageIno, InoSize>::fetch(
            self, &ino).await?.deref().clone();
        let target_size = (offset + length) as u64;
        if target_size <= ino_size.size() {
            return Ok(());
        }

        ino_size.set_size(target_size, self.fs_config().block_size);
        ino_size.last_change = SystemTime::now();
        self.put(&ino, Arc::new(ino_size)).await?;
        Ok(())
    }

    pub async fn hb_increment_blocks_reference_count(
        &mut self,
        blocks: &[(&TiFsHash, u64)],
    ) -> TiFsResult<HashMap<TiFsHash, BigUint>> {

        let keys = blocks.iter().map(|(h,_c)| {
            self.fs_config().key_builder().named_hashed_block_x(
                h,
                Some(super::key::HashedBlockMeta::CCountedNamedUsages),
                None).into()
        }).collect::<Vec<Key>>();

        let prev_counter_values = self.mini.batch_get_for_update(keys).await?
            .into_iter().filter_map(|KvPair(k,v)|{
                let kp = self.fs_config().key_parser_b(k).ok()?;
                let hash = kp.parse_hash().ok()?.1;
                let cnt = BigUint::from_bytes_be(&v);
                Some((hash, cnt))
            }).collect::<HashMap<_,_>>();

        let new_counter_values = blocks.iter().map(|(h,c)|{
            let new_cnt = c + prev_counter_values
                .get(*h).cloned().unwrap_or(BigUint::zero());
            let key: Key = self.fs_config().key_builder().named_hashed_block_x(
                h,
                Some(super::key::HashedBlockMeta::CCountedNamedUsages),
                None).into();
            Mutation::Put(key, new_cnt.to_bytes_be())
        }).collect::<Vec<_>>();

        self.mini.batch_mutate(new_counter_values).await?;
        Ok(prev_counter_values)
    }

    pub async fn hb_replace_block_hash_for_address_no_size_update(
        &mut self,
        addresses: &[(BlockAddress, Option<&TiFsHash>)],
    ) -> TiFsResult<HashMap<TiFsHash, u64>> {
        if addresses.len() == 0 {
            return Ok(HashMap::new());
        }

        let mut mutations = Vec::new();
        let keys = addresses.iter().map(|(addr, new_hash)|{
            let key = Key::from(self.fs_config().key_builder().block_hash(*addr));
            if let Some(hash) = new_hash {
                mutations.push(Mutation::Put(key.clone(), hash.to_vec()));
            } else {
                mutations.push(Mutation::Delete(key.clone()));
            }
            key
        }).collect::<Vec<_>>();
        let prev_block_hash = self.mini.batch_get(keys).await?;
        self.mini.batch_mutate(mutations).await?;

        let mut decrements = HashMap::new();
        for KvPair(_k,hash) in prev_block_hash {
            if hash.len() != self.fs_config().hash_len {
                return Err(FsError::Serialize { target: "hash", typ: "BIN", msg: format!("hash length mismatch.")});
            }
            let mut dec: u64 = decrements.get(&hash).cloned().unwrap_or(0u64);
            dec += 1;
            decrements.insert(hash, dec);
        }

        Ok(decrements)
    }

    pub async fn hb_replace_block_hash_for_address_only_size_update(
        &mut self,
        addr: &BlockAddress,
        new_blocks_actual_size: u64
    ) -> TiFsResult<()> {
        let ino_size_arc: Arc<InoSize> = self.fetch(&addr.ino).await?;
        let mut ino_size = ino_size_arc.deref().clone();
        let was_changed = ino_size.block_write_size_update(
            addr, self.fs_config().block_size, new_blocks_actual_size);
        if was_changed {
            self.put(&addr.ino, Arc::new(ino_size)).await?;
        }
        Ok(())
    }

    pub async fn hb_replace_block_hash_for_address_only_size_update_b(
        &mut self,
        ino: StorageIno,
        new_target_size: u64
    ) -> TiFsResult<()> {
        let ino_size_arc: Arc<InoSize> = self.fetch(&ino).await?;
        let mut ino_size = ino_size_arc.deref().clone();
        if new_target_size > ino_size.size {
            ino_size.set_size(new_target_size, self.fs_config().block_size);
            self.put(&ino, Arc::new(ino_size)).await?;
        }
        Ok(())
    }

    pub async fn hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
        &mut self,
        decrements: &HashMap<&TiFsHash, u64>,
    ) -> TiFsResult<()> {
        let block_ref_cnt_keys = decrements.iter().map(|(h, _dec)|{
            let key = Key::from(self.fs_config().key_builder().named_hashed_block_x(
                h, Some(HashedBlockMeta::CCountedNamedUsages), None));
            key
        }).collect::<Vec<_>>();

        let prev_counter_values = self.mini.batch_get(block_ref_cnt_keys.clone()).await?
            .filter_map(|KvPair(key, value)|{
                let cnt = BigUint::from_bytes_be(&value);
                let hash = self.fs_config().key_parser_b(key).ok()?.parse_hash().ok()?.1;
                Some((hash, cnt))
            }).collect::<HashMap<_,_>>();

        let mut mutations = Vec::new();
        for (h, dec) in decrements {
            let mut actual_dec = BigUint::from_u64(*dec).unwrap();
            let prev_counter_value = prev_counter_values.get(*h).cloned().unwrap_or(BigUint::ZERO);
            if prev_counter_value < actual_dec {
                tracing::error!("full decrement by {actual_dec} of block reference counter not possible with value {prev_counter_value}.");
                actual_dec = prev_counter_value.clone();
            }
            let new_counter_value = prev_counter_value.clone() - actual_dec;

            let key = Key::from(self.fs_config().key_builder().named_hashed_block_x(
                h, Some(HashedBlockMeta::CCountedNamedUsages), None));

            if new_counter_value == BigUint::from_u8(0).unwrap() {
                let block_key = self.fs_config().key_builder().hashed_block(h);
                mutations.push(Mutation::Delete(Key::from(key)));
                mutations.push(Mutation::Delete(Key::from(block_key)));
                tracing::warn!("deleting block with hash: {h:?}");
            } else {
                mutations.push(Mutation::Put(key.clone(), new_counter_value.to_bytes_be()));
            }
        }
        self.mini.batch_mutate(mutations).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn hb_get_block_hash_list_by_block_range(
        &mut self,
        ino: StorageIno,
        block_range: Range<BlockIndex>,
    ) -> TiFsResult<BTreeMap<BlockIndex, TiFsHash>> {
        let range = self.fs_config().key_builder().block_hash_range(ino, block_range.clone());
        let iter = self.mini.scan(
                range,
                block_range.count() as u32,
            )
            .await?;
        let mut result = BTreeMap::<BlockIndex, TiFsHash>::new();
        for KvPair(k, hash) in iter.into_iter() {
            let key = self.fs_config().key_parser_b(k)?.parse_key_block_address()?;
            if hash.len() != self.fs_config().hash_len {
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
        &mut self,
        ino: StorageIno,
        block_range: Range<BlockIndex>,
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
}



impl<K, V> TxnFetchMut<K, V> for TransactionWithFsConfig
where
    V: for<'dl> Deserialize<'dl>,
    ScopedKeyBuilder: KeyGenerator<K, V>,
    K: Debug,
{
    async fn fetch(&mut self, key: &K) -> TiFsResult<Arc<V>> {
        let opt = self.fetch_try(key).await?;
        if let Some(data) = opt {
            Ok(Arc::new(data))
        } else {
            Err(FsError::KeyNotFound(
                Some(format!("kv-type: {}->{}, key: {:?}", type_name::<K>(), type_name::<V>(), key))))
        }
    }

    async fn fetch_try(&mut self, key: &K) -> TiFsResult<Option<V>> {
        let t = self.fs_config().key_builder();
        let key_raw = t.generate_key(key);
        let result = self.mini.get(key_raw).await?;
        let Some(data) = result else {
            return Ok(None);
        };
        Ok(Some(deserialize_json::<V>(&data)
            .map_err(|err|FsError::Serialize{
                target: std::any::type_name::<V>(),
                typ: "JSON",
                msg: format!("deserialize failed: {err}"),
            })?))
    }
}


impl<K, V> TxnPutMut<K, V> for TransactionWithFsConfig
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn put(&mut self, key: &K, value: Arc<V>) -> TiFsResult<()> {
        let t = self.fs_config().key_builder();
        let key_raw = t.generate_key(key);
        let data = serialize_json(value.deref())
            .map_err(|err|FsError::Serialize{
                target: std::any::type_name::<V>(),
                typ: "JSON",
                msg: format!("serialization failed: {err}")
            })?;
        self.mini.put(key_raw, data).await?;
        Ok(())
    }
}

impl<K, V> TxnDeleteMut<K, V> for TransactionWithFsConfig
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn delete(&mut self, key: &K) -> TiFsResult<()> {
        let t = self.fs_config().key_builder();
        let key_raw = t.generate_key(key);
        self.mini.delete(key_raw).await?;
        Ok(())
    }
}
