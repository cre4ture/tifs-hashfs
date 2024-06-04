use std::ops::{Deref, DerefMut};

use tikv_client::Transaction;

pub struct AutoTxn {
    pub txn: Transaction,
    pub auto_commit_when_drop: bool,
}

impl Drop for AutoTxn {
    fn drop(&mut self) {
        if self.auto_commit_when_drop {
            //let _ = self.txn.commit().
        }
    }
}

impl Deref for AutoTxn {
    type Target = Transaction;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DerefMut for AutoTxn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

impl AutoTxn {
    pub fn new(txn: Transaction) -> Self {
        Self {
            txn,
            auto_commit_when_drop: true,
        }
    }
}
