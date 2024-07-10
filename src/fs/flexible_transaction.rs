use std::any::type_name;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tikv_client::{Backoff, BoundRange, Key, KvPair, Result, RetryOptions, Timestamp, Transaction, TransactionOptions, Value};
use tikv_client::transaction::Mutation;
use tokio::sync::RwLock;
use tokio::time::sleep;

use super::error::FsError;
use super::fs_config::TiFsConfig;
use super::index::{deserialize_json, serialize_json};
use super::key::{KeyGenerator, ScopedKeyBuilder};
use super::utils::txn_data_cache::{TxnDelete, TxnFetch, TxnPut};
use super::{error::TiFsResult, fuse_to_hashfs::{DEFAULT_REGION_BACKOFF, OPTIMISTIC_BACKOFF}, transaction_client_mux::TransactionClientMux};

use tikv_client::Result as TiKvResult;

pub enum SpinningIterResult<R> {
    Done(R),
    TryAgain,
    Failed(FsError),
}

pub struct SpinningTxn {
    pub backoff: Backoff,
}

impl Default for SpinningTxn {
    fn default() -> Self {
        Self {
            backoff: Backoff::decorrelated_jitter_backoff(10, 1000, 200),
        }
    }
}

impl SpinningTxn {
/*
    pub async fn run_txn_default<Fun, Fut, R>(
        f_txn: &FlexibleTransaction,
        f: Fun,
    ) -> TiFsResult<R>
    where
        for<'tl> Fut: 'tl + Future<Output = TiKvResult<R>>,
        for<'tl> Fun: Fn(Arc<Transaction>) -> Fut,
        for<'rl> R: 'rl + std::fmt::Debug,
    {
        let mut spin = SpinningTxn::default();
        spin.run_txn(f_txn, f).await
    }
*/
/*
    pub async fn run_txn<Fun, Fut, R>(
        &mut self,
        f_txn: &FlexibleTransaction,
        f: Fun,
    ) -> TiFsResult<R>
    where
    for <'tl> R: 'tl + std::fmt::Debug,
    for <'tl> Fut: 'tl + Future<Output = TiKvResult<R>>,
    for <'tl> Fun: Fn(&'tl Transaction) -> Fut,
    {
        let mut spin = SpinningTxn::default();
        loop {
            let mini = f_txn.mini_txn().await?;
            match spin.end_iter(f(&mini).await, mini).await {
                SpinningIterResult::Done(r) => break Ok(r),
                SpinningIterResult::TryAgain => {},
                SpinningIterResult::Failed(err) => return Err(err.into()),
            }
        }
    }
    */

//    pub async fn end_iter<R: std::fmt::Debug>(
//        &mut self,
//        result: TiKvResult<R>,
//        mini: Transaction
//    ) -> SpinningIterResult<R> {
//        match self.end_iter_b(result, mini).await {
//            None => return SpinningIterResult::TryAgain,
//            Some(Ok(value)) => return SpinningIterResult::Done(value),
//            Some(Err(err)) => return SpinningIterResult::Failed(err),
//        }
//    }

    pub async fn end_iter_b<R>(
        &mut self, result: TransactionResult<R>,
        mut mini: Transaction,
        log_msg: &str,
    ) -> Option<TiFsResult<R>> {
        match result {
            Ok(val) => {
                match mini.commit().await {
                    Ok(_t) => return Some(Ok(val)),
                    Err(error) => {
                        if let Some(delay) = self.backoff.next_delay_duration() {
                            sleep(delay).await;
                            tracing::info!("retry commit failed transaction. Type: {}, Msg: {log_msg}, Reason: {error:?}", std::any::type_name::<R>());
                            return None;
                        } else {
                            tracing::warn!("transaction failed. Type: {}, Msg: {log_msg}, Reason: {error:?}", std::any::type_name::<R>());
                            return Some(Err(error.into()));
                        }
                    }
                }
            }
            Err(result_err) => {
                if let Err(error) = mini.rollback().await {
                    tracing::error!("failed to rollback mini transaction. Err: {error:?}. Type: {}, Msg: {log_msg}", std::any::type_name::<R>());
                }
                match result_err {
                    TransactionError::PersistentIssue(err) => {
                        tracing::error!("cancelling transaction retry due to persistent error. Err: {err:?}. Type: {}, Msg: {log_msg}", std::any::type_name::<R>());
                        return Some(Err(err));
                    }
                    TransactionError::TemporaryIssue(err) => {
                        if let Some(delay) = self.backoff.next_delay_duration() {
                            sleep(delay).await;
                            tracing::info!("retry rolled back transaction. Type: {}, Msg: {log_msg}, Reason: {err:?}", std::any::type_name::<R>());
                            return None;
                        } else {
                            tracing::warn!("transaction failed. Type: {}, Msg: {log_msg}, Reason: {err:?}", std::any::type_name::<R>());
                            return Some(Err(err));
                        }
                    }
                }
            }
        }
    }
}

pub enum FlexibleTransactionKind {
    UseExistingTxn(RwLock<Transaction>),
    UseRawClient(Arc<tikv_client::RawClient>),
    SpawnMiniTransactions(Arc<TransactionClientMux>),
}

pub struct FlexibleTransaction {
    pub kind: FlexibleTransactionKind,
    pub fs_config: TiFsConfig,
}

impl FlexibleTransaction {
    pub fn new_pure_raw(
        raw: Arc<tikv_client::RawClient>,
        fs_config: TiFsConfig
    ) -> Self {
        Self { kind: FlexibleTransactionKind::UseRawClient(raw), fs_config }
    }

    pub fn new_with_existing_txn(
        txn: Transaction,
        fs_config: TiFsConfig,
    ) -> Self {
        Self { kind: FlexibleTransactionKind::UseExistingTxn(RwLock::new(txn)), fs_config }
    }

    pub async fn begin_optimistic_small(
        client_mux: Arc<TransactionClientMux>,
    ) -> Result<Transaction> {
        let options = TransactionOptions::new_optimistic()
            .use_async_commit();
        let options = options.retry_options(RetryOptions {
                region_backoff: DEFAULT_REGION_BACKOFF,
                lock_backoff: OPTIMISTIC_BACKOFF,
            })
            .try_one_pc();
        Ok(client_mux.give_one_transaction(&options).await?)
    }

    pub async fn get_snapshot_read_transaction(
        client_mux: Arc<TransactionClientMux>,
    ) -> TiFsResult<Transaction> {
        let options = TransactionOptions::new_optimistic()
            .retry_options(RetryOptions {
                region_backoff: DEFAULT_REGION_BACKOFF,
                lock_backoff: OPTIMISTIC_BACKOFF,
            })
            .read_only();
        Ok(client_mux.give_one_transaction(&options).await?)
    }

    pub fn fs_config(&self) -> &TiFsConfig {
        &self.fs_config
    }

    pub async fn single_action_txn_raw(txn_client_mux: Arc<TransactionClientMux>) -> TiFsResult<Transaction> {
        let transaction = Self::begin_optimistic_small(
            txn_client_mux.clone()).await.map_err(|err|{
                tracing::warn!("mini_txn failed. Err: {err:?}");
                err
            })?;
        Ok(transaction)
    }

    pub async fn finish_txn<R>(result: TiKvResult<R>, mut mini: Transaction) -> TiFsResult<R> {
        if result.is_ok() {
            mini.commit().await?;
        } else {
            mini.rollback().await?;
        }
        Ok(result?)
    }

    pub async fn get(&self, key: impl Into<Key>) -> TiFsResult<Option<Value>> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.get(key).await?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let key: Key = key.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.get(key.clone()).await.map_err(|e|e.into()),
                        mini, "get").await { break Ok(r?); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.get(key).await?)
            }
        }
    }

    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> TiFsResult<Vec<KvPair>> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.batch_get(keys).await
                    .map(|iter|iter.collect::<Vec<_>>())?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let keys = keys.into_iter().map(|k|Key::from(k.into())).collect::<Vec<_>>();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_get(keys.clone()).await.map_err(|e|e.into()),
                        mini, "batch_get").await { break Ok(r?.into_iter().collect::<Vec<_>>()); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.batch_get(keys).await?)
            }
        }
    }

    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> TiFsResult<()> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.put(key, value).await?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let key: Key = key.into();
                let value: Value = value.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.put(key.clone(), value.clone()).await.map_err(|e|e.into()),
                        mini, "put").await { break Ok(r?); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.put(key, value).await?)
            }
        }
    }

    pub async fn batch_put(&self, pairs: Vec<KvPair>) -> TiFsResult<()> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                let mutations = pairs.into_iter().map(|KvPair(k,v)| Mutation::Put(k, v));
                Ok(txn.write().await.batch_mutate(mutations).await?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let mutations = pairs.into_iter().map(|KvPair(k,v)| Mutation::Put(k, v));
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_mutate(mutations.clone()).await.map_err(|e|e.into()),
                        mini, "batch_put").await { break Ok(r?); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.batch_put(pairs).await?)
            }
        }
    }

    pub async fn delete(&self, key: impl Into<Key>) -> TiFsResult<()> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.delete(key).await?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let key: Key = key.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.delete(key.clone()).await.map_err(|e|e.into()),
                        mini, "delete").await { break Ok(r?); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.delete(key).await?)
            }
        }
    }

    pub async fn batch_mutate(&self, mutations: impl IntoIterator<Item = Mutation>) -> TiFsResult<()> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.batch_mutate(mutations).await?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let mutations: Vec<Mutation> = mutations.into_iter().collect();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_mutate(mutations.clone()).await.map_err(|e|e.into()),
                        mini, "batch_mutate").await { break Ok(r?); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                let mut deletes = Vec::new();
                let mut put_pairs = Vec::new();
                for entry in mutations.into_iter() {
                    match entry {
                        Mutation::Delete(key) => deletes.push(key),
                        Mutation::Put(key, value) => {
                            put_pairs.push(KvPair(key, value));
                        }
                    }
                };
                if put_pairs.len() > 0 {
                    raw.batch_put(put_pairs).await?;
                }
                if deletes.len() > 0 {
                    raw.batch_delete(deletes).await?;
                }
                Ok(())
            }
        }
    }

    pub async fn scan(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> TiFsResult<Vec<KvPair>> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.scan(range, limit).await
                    .map(|iter|iter.collect::<Vec<KvPair>>())?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let range: BoundRange = range.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.scan(range.clone(), limit).await.map_err(|e|e.into()),
                        mini, "scan").await { break Ok(r?.collect::<Vec<KvPair>>()); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.scan(range, limit).await?)
            }
        }
    }

    pub async fn scan_keys(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> TiFsResult<Vec<Key>> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                Ok(txn.write().await.scan_keys(range, limit).await
                .map(|iter|iter.collect::<Vec<_>>())?)
            },
            FlexibleTransactionKind::SpawnMiniTransactions(txn_mux) => {
                let range: BoundRange = range.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = txn_mux.clone().single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.scan_keys(range.clone(), limit).await.map_err(|e|e.into()),
                        mini, "scan_keys").await { break Ok(r?.collect::<Vec<Key>>()); }
                }
            },
            FlexibleTransactionKind::UseRawClient(raw) => {
                Ok(raw.scan_keys(range, limit).await?)
            }
        }
    }

    pub async fn commit(&self) -> TiKvResult<Option<Timestamp>> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                txn.write().await.commit().await
            },
            _ => Ok(Some(Timestamp::default())),
        }
    }

    pub async fn rollback(&self) -> TiKvResult<()> {
        match &self.kind {
            FlexibleTransactionKind::UseExistingTxn(txn) => {
                txn.write().await.rollback().await
            },
            _ => Ok(()),
        }
    }
}



impl<K, V> TxnFetch<K, V> for FlexibleTransaction
where
    V: for<'dl> Deserialize<'dl>,
    ScopedKeyBuilder: KeyGenerator<K, V>,
    K: Debug,
{
    async fn fetch(&self, key: &K) -> TiFsResult<Arc<V>> {
        let t = self.fs_config.key_builder();
        let key_raw = t.generate_key(key);
        let result = self.get(key_raw).await?;
        let Some(data) = result else {
            return Err(FsError::KeyNotFound(
                Some(format!("kv-type: {}->{}, key: {:?}", type_name::<K>(), type_name::<V>(), key))));
        };
        Ok(Arc::new(deserialize_json::<V>(&data)
            .map_err(|err|FsError::UnknownError(format!("deserialize failed: {err}")))?))
    }
}


impl<K, V> TxnPut<K, V> for FlexibleTransaction
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn put_json(&self, key: &K, value: Arc<V>) -> TiFsResult<()> {
        let t = self.fs_config.key_builder();
        let key = t.generate_key(key);
        let data = serialize_json(value.deref())
            .map_err(|err|FsError::UnknownError(format!("serialization failed: {err}")))?;
        self.put(key, data).await?;
        Ok(())
    }
}

impl<K, V> TxnDelete<K, V> for FlexibleTransaction
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn delete(&self, key: &K) -> TiFsResult<()> {
        let t = self.fs_config.key_builder();
        let key = t.generate_key(key);
        self.delete(key).await?;
        Ok(())
    }
}

pub enum TransactionError {
    TemporaryIssue(FsError),
    PersistentIssue(FsError),
}

pub type TransactionResult<V> = std::result::Result<V, TransactionError>;

impl From<FsError> for TransactionError {
    fn from(value: FsError) -> Self {
        match &value {
            FsError::KeyError(_) => TransactionError::TemporaryIssue(value),
            FsError::TryLockFailed => TransactionError::TemporaryIssue(value),
            FsError::UnknownError(_) => {
                tracing::warn!("considering unknown error as temporary: {value:?}");
                TransactionError::TemporaryIssue(value)
            }
            _ => TransactionError::PersistentIssue(value),
        }
    }
}

impl From<tikv_client::Error> for TransactionError {
    fn from(value: tikv_client::Error) -> Self {
        TransactionError::from(FsError::from(value))
    }
}

//impl From<TransactionError> for FsError {
//    fn from(value: TransactionError) -> Self {
//        match &value {
//            TransactionError::TemporaryIssue(value) => value,
//            TransactionError::PersistentIssue(value) => value,
//        }
//    }
//}
