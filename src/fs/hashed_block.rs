use std::{borrow::Cow, mem};

use super::inode::TiFsHash;

pub type HashBlockData<'ol> = Cow<'ol, [u8]>;

#[derive(Clone)]
pub struct HashedBlock<'ol> {
    pub hash: Option<TiFsHash>,
    pub data: HashBlockData<'ol>,
}

impl<'ol> HashedBlock<'ol> {
    pub fn new() -> Self {
        Self {
            hash: None,
            data: Cow::Borrowed(&[]),
        }
    }

    pub fn update_data_range(&mut self, start_write_pos: usize, block_write_data: &[u8]) {
        let mut mutable = mem::replace(&mut self.data, Cow::Borrowed(&[])).into_owned();
        Self::update_data_range_in_vec(start_write_pos, block_write_data, &mut mutable);
        self.data = Cow::Owned(mutable);
    }

    pub fn update_data_range_in_vec(start_write_pos: usize, block_write_data: &[u8], dest: &mut Vec<u8>) {
        let write_till_position = start_write_pos + block_write_data.len();
        if dest.len() < write_till_position {
            dest.resize(write_till_position, 0);
        }
        dest[start_write_pos..write_till_position].copy_from_slice(block_write_data);
    }
}
