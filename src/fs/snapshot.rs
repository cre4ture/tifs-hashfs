use std::{collections::{HashMap, VecDeque}, sync::Arc};

use bytestring::ByteString;
use counter::Counter;
use num_bigint::BigUint;

use super::{error::TiFsResult, flexible_transaction::FlexibleTransaction, fs_config::TiFsConfig, inode::InoDescription};
use super::hash_fs_interface::{BlockIndex, GotOrMade};
use super::transaction_client_mux::TransactionClientMux;
use super::kv_parser::KvPairParser;
use super::utils::txn_data_cache::{TxnFetchMut, TxnPutMut};
use super::key::{BlockAddress, KeyGenerator, ROOT_INODE, SNAPSHOT_PARENT_INODE};
use super::inode::{DirectoryItem, InoAccessTime, InoInlineData, InoModificationTime, InoSize, InoStorageFileAttr, ParentStorageIno, StorageDirItem, StorageDirItemKind, StorageIno};


pub struct CreateSnapshot {
    src_txn: super::mini_transaction::TransactionWithFsConfig,
    txn_client_mux: Arc<TransactionClientMux>,
    single_action: FlexibleTransaction,
}

impl CreateSnapshot {

    pub fn new(
        src_txn: super::mini_transaction::TransactionWithFsConfig,
        txn_client_mux: Arc<TransactionClientMux>,
    ) -> Self {
        let single_action = FlexibleTransaction{
            fs_config: src_txn.fs_config().clone(),
            kind: super::flexible_transaction::FlexibleTransactionKind::SpawnMiniTransactions(
                txn_client_mux.clone(),
            )
        };
        Self {
            src_txn,
            txn_client_mux,
            single_action,
        }
    }

    pub async fn spinning_mini_txn(
        &self
    ) -> TiFsResult<super::mini_transaction::MiniTransaction> {
        super::mini_transaction::MiniTransaction::new(
            self.txn_client_mux.clone(), self.fs_config().clone()).await
    }

    pub async fn create_snapshot(&mut self, name: ByteString) -> TiFsResult<GotOrMade<StorageDirItem>> {

        let root_attr: Arc<InoStorageFileAttr> = self.src_txn.fetch(&ROOT_INODE.0).await?;

        let new_ino = self.reserve_new_inos(1).await?;

        let mut spin = self.spinning_mini_txn().await?;
        let got_or_made = loop {
            let mut started = spin.start().await?;
            let r1 = started.directory_add_child_checked_new_inode(
                SNAPSHOT_PARENT_INODE,
                name.clone(),
                StorageDirItemKind::Directory,
                root_attr.perm,
                root_attr.gid,
                root_attr.uid,
                root_attr.rdev,
                None,
                new_ino
            ).await;
            if let Some(r) = started.finish(r1).await { break r?; }
        };

        let GotOrMade::NewlyCreated(snapshot_dir) = got_or_made else {
            return Ok(got_or_made);
        };

        self.directory_copy_recursive(ROOT_INODE, ParentStorageIno(snapshot_dir.ino)).await?;

        Ok(GotOrMade::NewlyCreated(snapshot_dir))
    }

    pub fn fs_config(&self) -> &TiFsConfig {
        self.src_txn.fs_config()
    }

    async fn reserve_new_inos(&mut self, count: u64) -> TiFsResult<StorageIno> {
        let mut spin = self.spinning_mini_txn().await?;
        let new_ino = loop {
            let mut started = spin.start().await?;
            let r1 = started.meta_mutable_reserve_new_inos(count).await;
            if let Some(r) = started.finish(r1).await { break r?; }
        };
        Ok(new_ino)
    }

    async fn inode_clone_attrs(
        &mut self,
        src_dst_mapping: &[(DirectoryItem, StorageIno)],
    ) -> TiFsResult<HashMap<StorageIno, InoSize>> {

        let mut keys = Vec::with_capacity(src_dst_mapping.len()*5);
        for (src, _dst) in src_dst_mapping {
            keys.push(KeyGenerator::<StorageIno, InoDescription>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
            keys.push(KeyGenerator::<StorageIno, InoStorageFileAttr>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
            keys.push(KeyGenerator::<StorageIno, InoAccessTime>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
            keys.push(KeyGenerator::<StorageIno, InoModificationTime>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
            keys.push(KeyGenerator::<StorageIno, InoInlineData>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
            keys.push(KeyGenerator::<StorageIno, InoSize>::generate_key(
                self.src_txn.fs_config().key_builder(),
                &src.ino));
        }

        let attr_data = self.src_txn.mini.batch_get(keys).await?.collect::<Vec<_>>();

        let kv_parser = KvPairParser{fs_config: self.src_txn.fs_config().clone()};
        let mut maps = kv_parser.parse_inode_attrs_kv_pairs(attr_data)?;

        let mut spin = self.spinning_mini_txn().await?;
        loop {
            let mut started = spin.start().await?;
            let mut r1 = Ok(());
            for (src, new_ino) in src_dst_mapping {
                if let Some(mut value) = maps.descriptions.remove(&src.ino) {
                    value.ino = *new_ino;
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
                if let Some(value) = maps.attrs.remove(&src.ino) {
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
                if let Some(value) = maps.access_times.remove(&src.ino) {
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
                if let Some(value) = maps.modification_times.remove(&src.ino) {
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
                if let Some(value) = maps.inline_data.remove(&src.ino) {
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
                if let Some(value) = maps.size.get(&src.ino).cloned() {
                    r1 = started.put(new_ino, Arc::new(value)).await;
                    if r1.is_err() { break; }
                }
            }
            if let Some(r) = started.finish(r1).await { break r?; }
        }
        Ok(maps.size)
    }

    async fn inode_clone_data(
        &mut self,
        src: &DirectoryItem,
        src_size: &InoSize,
        dst: StorageIno,
    ) -> TiFsResult<()> {
        let range = std::ops::Range::<BlockIndex>{
            start: BlockIndex(0),
            end: BlockIndex(1 + src_size.size.div_ceil(self.fs_config().block_size)),
        };
        let hashes = self.src_txn.hb_get_block_hash_list_by_block_range_chunked(
            src.ino, range).await?;

        let block_incs = hashes.iter()
            .map(|e|e.1).collect::<Counter<_>>()
            .into_iter().map(|(a,c)|(a,c as u64)).collect::<Vec<_>>();

        let mut spin = self.spinning_mini_txn().await?;
        let pre_cnts = loop {
            let mut started = spin.start().await?;
            let r1 = started.hb_increment_blocks_reference_count(
                &block_incs).await;
            if let Some(r) = started.finish(r1).await { break r?; }
        };

        let _ = pre_cnts.into_iter().map(|(_,cnt)|{
            if cnt == BigUint::ZERO {
                tracing::error!("hashed block data is not available for snapshot. Internal Error!");
            }
        });

        let mutations = hashes.into_iter().map(|(idx, hash)|{
            let key = self.fs_config().key_builder().block_hash(BlockAddress{
                ino: dst, index: idx
            });
            tikv_client::transaction::Mutation::Put(tikv_client::Key::from(key), hash)
        });

        self.single_action.batch_mutate(mutations).await?;
        Ok(())
    }

    async fn directory_copy_recursive(
        &mut self,
        start_origin_dir_ino: ParentStorageIno,
        start_destination_dir_ino: ParentStorageIno
    ) -> TiFsResult<()> {
        let mut copy_jobs =
            VecDeque::<(ParentStorageIno, ParentStorageIno)>::new();

        copy_jobs.push_back((start_origin_dir_ino, start_destination_dir_ino));

        loop {
            let Some((origin_dir_ino, destination_dir_ino))
                = copy_jobs.pop_front() else {
                break;
            };

            let src_list = self.src_txn.directory_scan_for_children(
                origin_dir_ino.0, super::fuse_to_hashfs::MAX_TIKV_SCAN_LIMIT).await?;

            if src_list.len() == 0 {
                continue;
            }

            let new_inos = self.reserve_new_inos(src_list.len() as u64).await?;

            let src_dst_mapping = src_list
                .into_iter().enumerate().map(|(i, src)|{
                    let new_ino = StorageIno(new_inos.0 + i as u64);
                    (src, new_ino)
                }).collect::<Vec<_>>();

            let mut spin = self.spinning_mini_txn().await?;
            loop {
                let mut started = spin.start().await?;
                let mut r1 = Ok(());
                for (src_child, new_ino) in &src_dst_mapping {
                    r1 = started.directory_add_child_link_unchecked(
                        destination_dir_ino,
                        src_child.name.clone().into(),
                        Arc::new(StorageDirItem{
                        ino: *new_ino,
                        typ: src_child.typ,
                        })).await;

                    if r1.is_err() {
                        break;
                    }
                }
                if let Some(r) = started.finish(r1).await { break r?; }
            }

            let size_map = self.inode_clone_attrs(&src_dst_mapping).await?;

            for (src_child, new_ino) in &src_dst_mapping {
                match src_child.typ {
                    StorageDirItemKind::File => {
                        if let Some(src_size) = size_map.get(&src_child.ino) {
                            self.inode_clone_data(src_child, src_size, *new_ino).await?;
                        }
                    }
                    StorageDirItemKind::Directory => {
                        copy_jobs.push_back((ParentStorageIno(src_child.ino), ParentStorageIno(*new_ino)));
                    }
                    StorageDirItemKind::Symlink => {}
                }
            }
        }

        Ok(())
    }

}
