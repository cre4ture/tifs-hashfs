use std::{ops::Deref, sync::Arc, time::{Duration, Instant}};

use tikv_client::{Key, Transaction};

use crate::fs::error::TiFsResult;



pub trait TxnFetch<K, V> {
    #[allow(async_fn_in_trait)]
    async fn fetch(&self, key: &K) -> TiFsResult<Arc<V>>;
}

pub trait TxnPut<K, V> {
    #[allow(async_fn_in_trait)]
    async fn put(&self, key: &K, value: Arc<V>) -> TiFsResult<()>;
}

pub trait TxnDelete<K, V> {
    #[allow(async_fn_in_trait)]
    async fn delete(&self, key: &K) -> TiFsResult<()>;
}

#[derive(Clone)]
pub struct TxnDataCache<K, V> {
    cache: moka::future::Cache<K, (Instant, Arc<V>)>,
    time_limit: Duration,
}

impl<K, V> TxnDataCache<K, V>
where
    K: Clone + 'static,
    K: std::hash::Hash,
    K: std::cmp::Eq,
    K: std::marker::Send,
    K: std::marker::Sync,
    V: std::marker::Sync,
    V: std::marker::Send,
    V: 'static,
{
    pub fn new(capacity: u64, time_limit: Duration) -> Self {
        Self{
            cache: moka::future::Cache::new(capacity),
            time_limit,
        }
    }

    pub async fn read_uncached(&self, key: &K, txn: &impl TxnFetch<K, V>) -> TiFsResult<Arc<V>> {
        let value = txn.fetch(key).await?;
        self.cache.insert(key.clone(), (Instant::now(), value.clone())).await;
        Ok(value)
    }

    pub async fn read_cached(&self, key: &K, txn: &impl TxnFetch<K, V>) -> TiFsResult<Arc<V>> {
        if let Some((time, value)) = self.cache.get(key).await {
            if time.elapsed() < self.time_limit {
                return Ok(value);
            } else {
                self.cache.remove(key).await;
            }
        }
        self.read_uncached(key, txn).await
    }

    pub async fn write_cached(&self, key: K, value: Arc<V>, txn: &impl TxnPut<K, V>) -> TiFsResult<()> {
        self.cache.insert(key.clone(), (Instant::now(), value.clone())).await;
        Ok(txn.put(&key, value).await?)
    }

    pub async fn delete_with_cache(&self, key: &K, txn: &impl TxnDelete<K, V>) -> TiFsResult<()> {
        self.cache.remove(key).await;
        txn.delete(key).await
    }
}


pub struct GenericDataCache {
    cache: moka::future::Cache<Key, (Instant, Option<Arc<Vec<u8>>>)>,
    time_limit: Duration,
}

impl GenericDataCache {
    pub fn new(capacity: u64, time_limit: Duration) -> Self {
        Self{
            cache: moka::future::Cache::new(capacity),
            time_limit,
        }
    }

    pub async fn read_uncached(
        &self,
        key: Key,
        txn: &mut Transaction
    ) -> TiFsResult<Option<Arc<Vec<u8>>>> {
        let value = txn.get(key.clone()).await?.map(|v| Arc::new(v));
        self.cache.insert(key, (Instant::now(), value.clone())).await;
        Ok(value)
    }

    pub async fn read_cached(
        &self,
        key: &Key,
        txn: &mut Transaction
    ) -> TiFsResult<Option<Arc<Vec<u8>>>> {
        if let Some((time, value)) = self.cache.get(key).await {
            if time.elapsed() < self.time_limit {
                return Ok(value);
            } else {
                self.cache.remove(key).await;
            }
        }
        self.read_uncached(key.clone(), txn).await
    }

    pub async fn write_cached(&self, key: Key, value: Arc<Vec<u8>>, txn: &mut Transaction) -> TiFsResult<()> {
        self.cache.insert(key.clone(), (Instant::now(), Some(value.clone()))).await;
        Ok(txn.put(key, value.deref().clone()).await?)
    }

    pub async fn delete_with_cache(&self, key: Key, txn: &mut Transaction) -> TiFsResult<()> {
        self.cache.remove(&key).await;
        Ok(txn.delete(key).await?)
    }

    /*
    pub async fn scan_range_uncached(
        &self,
        range: BoundRange,
        limit: u32,
        txn: &mut Transaction,
    ) -> TiFsResult<> {
        let values = txn.scan(range, limit).await?;
    }
    */
}
