use std::{collections::{HashMap, HashSet}, ops::Deref, sync::Arc, time::SystemTime};

use bytestring::ByteString;
use num_bigint::BigUint;
use num_traits::{FromPrimitive, Zero};
use serde::{Deserialize, Serialize};
use tikv_client::{transaction::Mutation, Key, KvPair, Transaction};
use tokio::time::sleep;
use uuid::Uuid;

use crate::fs::{inode::ParentStorageIno, meta::MetaMutable};

use super::{dir::StorageDirectory, error::{FsError, TiFsResult}, hash_fs_interface::GotOrMade, inode::{DirectoryItem, InoChangeIterationId, InoFullHash, InoInlineData, TiFsHash}, key::{BlockAddress, HashedBlockMeta, OPENED_INODE_PARENT_INODE}, fuse_to_hashfs::MAX_TIKV_SCAN_LIMIT, utils::txn_data_cache::{TxnDeleteMut, TxnFetchMut, TxnPutMut}};
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

pub struct MiniTransaction<'ft_l> {
    txn: &'ft_l FlexibleTransaction,
    fs_config: &'ft_l TiFsConfig,
    spin: SpinningTxn,
}

pub struct StartedMiniTransaction<'mmt_l, 'ft_l> {
    parent: &'mmt_l mut MiniTransaction<'ft_l>,
    pub mini: Transaction,
}

impl<'ft_l> MiniTransaction<'ft_l> {
    pub async fn new(txn: &'ft_l FlexibleTransaction) -> TiFsResult<Self> {
        Ok(Self{
            txn,
            fs_config: txn.fs_config(),
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

    pub async fn start<'mol>(&'mol mut self) -> TiFsResult<StartedMiniTransaction<'mol, 'ft_l>>
    {
        let mini_res = loop {
            let r = self.txn.single_action_txn_raw().await;
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
        Ok(StartedMiniTransaction::<'mol, 'ft_l>{
            parent: self,
            mini: mini_res
        })
    }
}

impl<'ol, 'pl> StartedMiniTransaction<'ol, 'pl> {

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
        let read_meta: Result<Arc<MetaMutable>, FsError> = self.fetch(&()).await;

        let Ok(mut dyn_meta) = read_meta.as_deref().cloned() else {
            let err = read_meta.unwrap_err();
            if err != FsError::KeyNotFound {
                return Err(err);
            }

            tracing::info!("dyn meta data is missing -> assume fresh FS. Initialize on the fly.");
            self.meta_static_mutable_init().await?;

            // the first call to this function for a fresh filesystem is about
            // creating the root inode data. Thus we return the pre-defined ROOT_INODE (NR 1).
            return Ok( crate::fs::key::ROOT_INODE.0 );
        };

        let ino = dyn_meta.inode_next;
        dyn_meta.inode_next += 1;

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
        addr: &BlockAddress,
        new_hash: Option<&TiFsHash>,
    ) -> TiFsResult<Option<TiFsHash>> {
        let key = Key::from(self.fs_config().key_builder().block_hash(addr.clone()));
        let prev_block_hash = self.mini.get(key.clone()).await?;
        if let Some(new_hash) = new_hash {
            self.mini.put(key.clone(), new_hash.clone()).await?;
        } else {
            self.mini.delete(key.clone()).await?;
        }
        Ok(prev_block_hash)
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

    pub async fn hb_decrement_blocks_reference_count_and_delete_if_zero_reached(
        &mut self,
        hash: &Vec<u8>,
        cnt: u64,
    ) -> TiFsResult<()> {
        let block_ref_cnt_key = self.fs_config().key_builder().named_hashed_block_x(
            hash, Some(HashedBlockMeta::CCountedNamedUsages), None);

        let prev_counter_value = self.mini.get(block_ref_cnt_key.clone()).await?
            .map(|vec|BigUint::from_bytes_be(&vec))
            .unwrap_or(BigUint::from_u8(0u8).unwrap());
        let mut actual_dec = BigUint::from_u64(cnt).unwrap();
        if prev_counter_value < actual_dec {
            tracing::error!("full decrement by {cnt} of block reference counter not possible with value {prev_counter_value}.");
            actual_dec = prev_counter_value.clone();
        }
        let new_counter_value = prev_counter_value.clone() - actual_dec;
        if new_counter_value == BigUint::from_u8(0).unwrap() {
            let block_key = self.fs_config().key_builder().hashed_block(hash);
            let mut deletes = Vec::with_capacity(2);
            deletes.push(Mutation::Delete(Key::from(block_ref_cnt_key)));
            deletes.push(Mutation::Delete(Key::from(block_key)));
            self.mini.batch_mutate(deletes).await?;
            tracing::warn!("deleting block with hash: {hash:?}");
        } else {
            self.mini.put(block_ref_cnt_key.clone(), new_counter_value.to_bytes_be()).await?;
        }
        Ok(())
    }
}



impl<'ol, 'pl, K, V> TxnFetchMut<K, V> for StartedMiniTransaction<'ol, 'pl>
where V: for<'dl> Deserialize<'dl>, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn fetch(&mut self, key: &K) -> TiFsResult<Arc<V>> {
        let opt = self.fetch_try(key).await?;
        if let Some(data) = opt {
            Ok(Arc::new(data))
        } else {
            Err(FsError::KeyNotFound)
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


impl<'ol, 'pl, K, V> TxnPutMut<K, V> for StartedMiniTransaction<'ol, 'pl>
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

impl<'ol, 'pl, K, V> TxnDeleteMut<K, V> for StartedMiniTransaction<'ol, 'pl>
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn delete(&mut self, key: &K) -> TiFsResult<()> {
        let t = self.fs_config().key_builder();
        let key_raw = t.generate_key(key);
        self.mini.delete(key_raw).await?;
        Ok(())
    }
}
