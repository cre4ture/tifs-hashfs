use std::collections::HashMap;

use bimap::BiHashMap;
use sha2::{Sha256, Sha512, Digest};
use lazy_static::lazy_static;

use crate::fs::inode::TiFsHash;

use super::common_prints::debug_print_start_and_end_bytes_of_buffer;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum HashAlgorithm {
    Blake3,
    Sha256,
    Sha512,
}

lazy_static!{
    pub static ref ALGO_NAME_MAP: BiHashMap<&'static str, HashAlgorithm> = {
        let mut m = BiHashMap::new();
        m.insert("BLAKE3", HashAlgorithm::Blake3);
        m.insert("SHA-256", HashAlgorithm::Sha256);
        m.insert("SHA-512", HashAlgorithm::Sha512);
        m
    };
    pub static ref ALGO_HASH_LEN_MAP: HashMap<HashAlgorithm, usize> = {
        let mut m = HashMap::new();
        m.insert(HashAlgorithm::Blake3, 32);
        m.insert(HashAlgorithm::Sha256, 32);
        m.insert(HashAlgorithm::Sha512, 64);
        m
    };
}

impl HashAlgorithm {
    pub fn calculate_hash(&self, input: &[u8]) -> TiFsHash {
        let hash = match self {
            HashAlgorithm::Blake3 => {
                blake3::hash(input).as_bytes().to_vec()
            }
            HashAlgorithm::Sha256 => {
                let mut hash_er = Sha256::new();
                hash_er.update(input);
                hash_er.finalize().to_vec()
            }
            HashAlgorithm::Sha512 => {
                let mut hash_er = Sha512::new();
                hash_er.update(input);
                hash_er.finalize().to_vec()
            }
        };
        tracing::trace!("calculate_hash - in: {}, hash out: {:x?}",
            debug_print_start_and_end_bytes_of_buffer(32, input),
            &hash
        );
        hash
    }
}
