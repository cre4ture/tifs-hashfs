
use super::inode::TiFsHash;


pub struct PendingDeletes {
    pub iteration_number: u64,
    pub last_update: u64, // unix timestamp (s)
    pub pending: Vec<TiFsHash>,
}


impl PendingDeletes {
    pub fn null() -> Self {
        Self { iteration_number: 0, last_update: 0, pending: vec![] }
    }
}
