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
use super::key::{KeyGenerator, KeyParser, ScopedKeyBuilder};
use super::mini_transaction::MiniTransaction;
use super::utils::txn_data_cache::{TxnDelete, TxnFetch, TxnPut};
use super::{error::TiFsResult, transaction::{DEFAULT_REGION_BACKOFF, OPTIMISTIC_BACKOFF}, transaction_client_mux::TransactionClientMux};

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

pub struct FlexibleTransaction {
    txn_client_mux: Arc<TransactionClientMux>,
    txn: Option<RwLock<Transaction>>,
    raw: Arc<tikv_client::RawClient>,
    fs_config: TiFsConfig,
}

impl FlexibleTransaction {
    pub fn new_pure_raw(
        txn_client_mux: Arc<TransactionClientMux>,
        raw: Arc<tikv_client::RawClient>,
        fs_config: TiFsConfig
    ) -> Self {
        Self { txn_client_mux, txn: None, raw, fs_config }
    }

    pub fn new_txn(
        txn_client_mux: Arc<TransactionClientMux>,
        txn: Option<RwLock<Transaction>>,
        raw: Arc<tikv_client::RawClient>,
        fs_config: TiFsConfig,
    ) -> Self {
        Self { txn_client_mux, txn, raw, fs_config }
    }

    async fn begin_optimistic_small(
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

    pub fn get_endpoints(&self) -> Vec<String> {
        self.txn_client_mux.get_endpoints()
    }

    pub fn fs_config(&self) -> &TiFsConfig {
        &self.fs_config
    }

    pub async fn single_action_txn_raw(&self) -> TiFsResult<Transaction> {
        let transaction = Self::begin_optimistic_small(
            self.txn_client_mux.clone()).await.map_err(|err|{
                tracing::warn!("mini_txn failed. Err: {err:?}");
                err
            })?;
        Ok(transaction)
    }

    pub async fn spinning_mini_txn(&self) -> TiFsResult<MiniTransaction> {
        MiniTransaction::new(self).await
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
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.get(key).await?)
        } else {
            if self.fs_config.small_transactions {
                let key: Key = key.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.get(key.clone()).await.map_err(|e|e.into()),
                        mini, "get").await { break Ok(r?); }
                }
            } else {
                Ok(self.raw.get(key).await?)
            }
        }
    }

    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> TiFsResult<Vec<KvPair>> {
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.batch_get(keys).await
                .map(|iter|iter.collect::<Vec<_>>())?)
        } else {
            if self.fs_config.small_transactions {
                let keys = keys.into_iter().map(|k|Key::from(k.into())).collect::<Vec<_>>();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_get(keys.clone()).await.map_err(|e|e.into()),
                        mini, "batch_get").await { break Ok(r?.into_iter().collect::<Vec<_>>()); }
                }
            } else {
                Ok(self.raw.batch_get(keys).await?)
            }
        }
    }

    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> TiFsResult<()> {
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.put(key, value).await?)
        } else {
            if self.fs_config.small_transactions {
                let key: Key = key.into();
                let value: Value = value.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.put(key.clone(), value.clone()).await.map_err(|e|e.into()),
                        mini, "put").await { break Ok(r?); }
                }
            } else {
                Ok(self.raw.put(key, value).await?)
            }
        }
    }

    pub async fn batch_put(&self, pairs: Vec<KvPair>) -> TiFsResult<()> {
        if let Some(txn) = &self.txn {
            let mutations = pairs.into_iter().map(|KvPair(k,v)| Mutation::Put(k, v));
            Ok(txn.write().await.batch_mutate(mutations).await?)
        } else {
            if self.fs_config.small_transactions {
                let mutations = pairs.into_iter().map(|KvPair(k,v)| Mutation::Put(k, v));
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_mutate(mutations.clone()).await.map_err(|e|e.into()),
                        mini, "batch_put").await { break Ok(r?); }
                }
            } else {
                Ok(self.raw.batch_put(pairs).await?)
            }
        }
    }

    pub async fn delete(&self, key: impl Into<Key>) -> TiFsResult<()> {
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.delete(key).await?)
        } else {
            if self.fs_config.small_transactions {
                let key: Key = key.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.delete(key.clone()).await.map_err(|e|e.into()),
                        mini, "delete").await { break Ok(r?); }
                }
            } else {
                Ok(self.raw.delete(key).await?)
            }
        }
    }

    pub async fn batch_mutate(&self, mutations: impl IntoIterator<Item = Mutation>) -> TiFsResult<()> {
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.batch_mutate(mutations).await?)
        } else {
            if self.fs_config.small_transactions {
                let mutations: Vec<Mutation> = mutations.into_iter().collect();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.batch_mutate(mutations.clone()).await.map_err(|e|e.into()),
                        mini, "batch_mutate").await { break Ok(r?); }
                }
            } else {
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
                    self.raw.batch_put(put_pairs).await?;
                }
                if deletes.len() > 0 {
                    self.raw.batch_delete(deletes).await?;
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
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.scan(range, limit).await
                .map(|iter|iter.collect::<Vec<KvPair>>())?)
        } else {
            if self.fs_config.small_transactions {
                let range: BoundRange = range.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.scan(range.clone(), limit).await.map_err(|e|e.into()),
                        mini, "scan").await { break Ok(r?.collect::<Vec<KvPair>>()); }
                }
            } else {
                Ok(self.raw.scan(range, limit).await?)
            }
        }
    }

    pub async fn scan_keys(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> TiFsResult<Vec<Key>> {
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.scan_keys(range, limit).await
                .map(|iter|iter.collect::<Vec<_>>())?)
        } else {
            if self.fs_config.small_transactions {
                let range: BoundRange = range.into();
                let mut spin = SpinningTxn::default();
                loop {
                    let mut mini = self.single_action_txn_raw().await?;
                    if let Some(r) = spin.end_iter_b(
                        mini.scan_keys(range.clone(), limit).await.map_err(|e|e.into()),
                        mini, "scan_keys").await { break Ok(r?.collect::<Vec<Key>>()); }
                }
            } else {
                Ok(self.raw.scan_keys(range, limit).await?)
            }
        }
    }

    pub async fn commit(&self) -> TiKvResult<Option<Timestamp>> {
        if let Some(txn) = &self.txn {
            txn.write().await.commit().await
        } else {
            Ok(Some(Timestamp::default()))
        }
    }

    pub async fn rollback(&self) -> TiKvResult<()> {
        if let Some(txn) = &self.txn {
            txn.write().await.rollback().await
        } else {
            Ok(())
        }
    }

    pub fn key_builder(&self) -> ScopedKeyBuilder {
        ScopedKeyBuilder::new(&self.fs_config.key_prefix)
    }

    pub fn key_parser<'fl>(&'fl self, i: &'fl mut std::slice::Iter<'fl, u8>) -> TiFsResult<KeyParser<'fl>> {
        KeyParser::start(i, &self.fs_config.key_prefix, self.fs_config.hash_len)
    }
}



impl<K, V> TxnFetch<K, V> for FlexibleTransaction
where V: for<'dl> Deserialize<'dl>, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn fetch(&self, key: &K) -> TiFsResult<Arc<V>> {
        let t = self.key_builder();
        let key_raw = t.generate_key(key);
        let result = self.get(key_raw).await?;
        let Some(data) = result else {
            return Err(FsError::KeyNotFound);
        };
        Ok(Arc::new(deserialize_json::<V>(&data)
            .map_err(|err|FsError::UnknownError(format!("deserialize failed: {err}")))?))
    }
}


impl<K, V> TxnPut<K, V> for FlexibleTransaction
where V: Serialize, ScopedKeyBuilder: KeyGenerator<K, V>
{
    async fn put_json(&self, key: &K, value: Arc<V>) -> TiFsResult<()> {
        let t = self.key_builder();
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
        let t = self.key_builder();
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
