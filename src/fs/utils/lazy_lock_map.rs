use core::fmt;
use std::{collections::HashMap, sync::{Arc, Weak}};

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};


pub struct LazyLockMap<K, V> {
    map: RwLock<HashMap<K, Weak<RwLock<V>>>>,
}

impl<K, V> LazyLockMap<K, V>
where
    K: std::cmp::Eq,
    K: std::hash::Hash,
    K: Clone,
    V: Default,
    K: fmt::Debug,
    V: fmt::Debug,
{
    pub fn new() -> Self {
        Self{
            map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_lock(&self, key: &K) -> Arc<RwLock<V>> {
        let mut locks = self.map.write().await;
        let existing_lock = locks.get(&key).and_then(|x|x.upgrade());
        if let Some(lock) = existing_lock {
            lock
        } else {
            let lock = Arc::new(RwLock::new(V::default()));
            locks.insert(key.clone(), Arc::downgrade(&lock));
            lock
        }
    }

    pub async fn lock_write(&self, key: &K) -> OwnedRwLockWriteGuard<V> {
        let lock = self.get_lock(key).await;
        let wl = lock.write_owned().await;
        wl
    }

    pub async fn lock_read(&self, key: &K) -> OwnedRwLockReadGuard<V> {
        let lock = self.get_lock(key).await;
        let rl = lock.read_owned().await;
        rl
    }

    pub fn print_statistics(&self) {
        let Some(lock) = self.map.try_read().ok() else {
            return;
        };

        tracing::trace!("locks({}): {:?}", lock.len(), lock);
    }

    pub fn cleanup(&self) {
        let Some(mut ino_locks) = self.map.try_write().ok() else {
            return;
        };

        ino_locks.retain(|_k,w|{
            w.upgrade().is_some()
        });
    }
}
