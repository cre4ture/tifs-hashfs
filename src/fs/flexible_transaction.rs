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
use super::utils::txn_data_cache::{TxnDelete, TxnFetch, TxnPut};
use super::{error::TiFsResult, transaction::{DEFAULT_REGION_BACKOFF, OPTIMISTIC_BACKOFF}, transaction_client_mux::TransactionClientMux};

use tikv_client::Result as TiKvResult;

pub enum SpinningIterResult<R> {
    Done(R),
    TryAgain,
    Failed(tikv_client::Error),
}

pub struct SpinningTxn {
    pub backoff: Backoff,
}

impl SpinningTxn {
    pub async fn end_iter<R: std::fmt::Debug>(&mut self, result: TiKvResult<R>, mini: &mut Transaction
    ) -> SpinningIterResult<R> {
        if let Ok(val) = result {
            match mini.commit().await {
                Ok(_t) => return SpinningIterResult::Done(val),
                Err(error) => {
                    if let Some(delay) = self.backoff.next_delay_duration() {
                        sleep(delay).await;
                        return SpinningIterResult::TryAgain;
                    } else {
                        return SpinningIterResult::Failed(error);
                    }
                }
            }
        } else {
            if let Err(error) = mini.rollback().await {
                tracing::error!("failed to rollback mini transaction. Err: {error:?}");
            }
            return SpinningIterResult::Failed(result.unwrap_err());
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
        client: Arc<TransactionClientMux>,
    ) -> Result<Transaction> {
        let options = TransactionOptions::new_optimistic().use_async_commit();
        let options = options.retry_options(RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        });
        let txn = client.give_one().begin_with_options(options).await?;
        Ok(txn)
    }

    pub async fn mini_txn(&self) -> TiFsResult<Transaction> {
        let transaction = Self::begin_optimistic_small(
            self.txn_client_mux.clone()).await?;
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
        if let Some(txn) = &self.txn {
            Ok(txn.write().await.get(key).await?)
        } else {
            if self.fs_config.small_transactions {
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.get(key).await, mini).await
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
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.batch_get(keys).await
                    .map(|iter|iter.collect::<Vec<_>>())
                    , mini
                ).await
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
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.put(key, value).await, mini).await
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
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.batch_mutate(mutations).await, mini).await
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
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.delete(key).await, mini).await
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
                let mut mini = self.mini_txn().await?;
                Self::finish_txn(mini.batch_mutate(mutations).await, mini).await
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
                let mut mini = self.mini_txn().await?;
                Ok(Self::finish_txn(mini.scan(range, limit).await, mini).await?
                    .collect::<Vec<KvPair>>())
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
                let mut mini = self.mini_txn().await?;
                Ok(Self::finish_txn(mini.scan_keys(range, limit).await, mini).await?
                    .collect::<Vec<Key>>())
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
    async fn put(&self, key: &K, value: Arc<V>) -> TiFsResult<()> {
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
