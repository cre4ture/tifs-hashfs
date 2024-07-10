use std::ops::{Deref, DerefMut};


pub struct Pool<T> {
    free: std::sync::RwLock<std::collections::VecDeque<T>>,
    counter: std::sync::RwLock<u64>,
}

pub struct HandedOutPoolElement<'pool, T> {
    pool: &'pool Pool<T>,
    element: Option<T>,
}

impl<'pool, T> HandedOutPoolElement<'pool, T> {
    pub fn new(pool: &'pool Pool<T>, element: T) -> Self {
        Self {
            pool,
            element: Some(element),
        }
    }
}

impl<'pool, T> Drop for HandedOutPoolElement<'pool, T> {
    fn drop(&mut self) {
        if let Some(element) = self.element.take() {
            self.pool.free.write().unwrap().push_back(element);
        }
    }
}

impl<'pool, T> std::ops::Deref for HandedOutPoolElement<'pool, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.element.as_ref().unwrap()
    }
}

impl<'pool, T> std::ops::DerefMut for HandedOutPoolElement<'pool, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.element.as_mut().unwrap()
    }
}

impl<T> Pool<T> {
    pub fn new() -> Self {
        Self {
            free: std::collections::VecDeque::new().into(),
            counter: std::sync::RwLock::new(0),
        }
    }

    pub fn add_new_busy_to_pool<'pool>(&'pool self, new_busy_element: T) -> HandedOutPoolElement<'pool, T> {
        let mut counter_lock = self.counter.write().unwrap();
        *counter_lock.deref_mut() += 1;
        let new_count = *counter_lock.deref();
        drop(counter_lock);
        tracing::info!("adding element to pool. new count: {}", new_count);
        return HandedOutPoolElement::<'pool, T>::new(self, new_busy_element);
    }

    pub fn get_one_free<'pool>(&'pool self) -> Option<HandedOutPoolElement<'pool, T>> {
        let one_free = self.free.write().unwrap().pop_front();
        one_free.map(|free|{
            HandedOutPoolElement::<'pool, T>::new(self, free)
        })
    }
}
