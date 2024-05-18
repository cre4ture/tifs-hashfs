use std::mem::size_of;
use std::ops::Range;

use tikv_client::Key;

use super::error::{FsError, Result};

pub const ROOT_INODE: u64 = fuser::FUSE_ROOT_ID;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy)]
pub enum ScopedKeyKind<'a> {
    Meta,
    Inode(u64),
    Block { ino: u64, block: u64 },
    FileHandler { ino: u64, handler: u64 },
    FileIndex { parent: u64, name: &'a str },
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy)]
pub struct ScopedKey<'a> {
    pub prefix: &'a [u8],
    pub key_type: ScopedKeyKind<'a>,
}

pub struct ScopedKeyBuilder<'a> {
    prefix: &'a [u8],
}

impl<'a> ScopedKeyBuilder<'a> {
    pub fn new(prefix: &'a [u8]) -> Self {
        Self {
            prefix
        }
    }

    pub const fn meta(&self) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::Meta,
        }
    }

    pub const fn inode(&self, ino: u64) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::Inode(ino),
        }
    }

    pub const fn block(&self, ino: u64, block: u64) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::Block { ino, block },
        }
    }

    pub const fn root(&self) -> ScopedKey {
        self.inode(ROOT_INODE)
    }

    pub const fn handler(&self, ino: u64, handler: u64) -> ScopedKey {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::FileHandler { ino, handler }
        }
    }

    pub fn index(&self, parent: u64, name: &'a str) -> ScopedKey {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::FileIndex { parent, name }
        }
    }

    pub fn block_range(&self, ino: u64, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino);
        self.block(ino, block_range.start).into()..self.block(ino, block_range.end).into()
    }

    pub fn inode_range(&self, ino_range: Range<u64>) -> Range<Key> {
        self.inode(ino_range.start).into()..self.inode(ino_range.end).into()
    }

    pub fn parse(&self, key: &'a [u8]) -> Result<ScopedKey> {
        let invalid_key = || FsError::InvalidScopedKey(key.to_owned());
        let (prefix, key) = key.split_at(self.prefix.len());
        if prefix != self.prefix {
            return Err(FsError::UnknownError("key with invalid prefix!".into()));
        }
        let (scope, data) = key.split_first().ok_or_else(invalid_key)?;
        match *scope {
            ScopedKey::META => Ok(self.meta()),
            ScopedKey::INODE => {
                let ino = u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                Ok(self.inode(ino))
            }
            ScopedKey::BLOCK => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let block = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                Ok(self.block(ino, block))
            }
            ScopedKey::HANDLER => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let handler = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                Ok(self.handler(ino, handler))
            }
            ScopedKey::INDEX => {
                let parent =
                    u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                Ok(self.index(
                    parent,
                    std::str::from_utf8(&data[size_of::<u64>()..]).map_err(|_| invalid_key())?,
                ))
            }
            _ => Err(invalid_key()),
        }
    }
}

impl<'a> ScopedKey<'a> {
    const META: u8 = 0;
    const INODE: u8 = 1;
    const BLOCK: u8 = 2;
    const HANDLER: u8 = 3;
    const INDEX: u8 = 4;

    pub fn scope(&self) -> u8 {
        use ScopedKeyKind::*;

        match self.key_type {
            Meta => Self::META,
            Inode(_) => Self::INODE,
            Block { ino: _, block: _ } => Self::BLOCK,
            FileHandler { ino: _, handler: _ } => Self::HANDLER,
            FileIndex { parent: _, name: _ } => Self::INDEX,
        }
    }

    pub fn len(&self) -> usize {
        use ScopedKeyKind::*;

        let kind_size = match self.key_type {
            Meta => 0,
            Inode(_) => size_of::<u64>(),
            Block { ino: _, block: _ } => size_of::<u64>() * 2,
            FileHandler { ino: _, handler: _ } => size_of::<u64>() * 2,
            FileIndex { parent: _, name } => size_of::<u64>() + name.len(),
        };

        return self.prefix.len() + 1 + kind_size;
    }

    pub fn serialize(&self) -> Vec<u8> {
        use ScopedKeyKind::*;

        let mut data = Vec::with_capacity(self.len());
        data.extend(self.prefix);
        data.push(self.scope());
        match self.key_type {
            Meta => (),
            Inode(ino) => data.extend(ino.to_be_bytes().iter()),
            Block { ino, block } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(block.to_be_bytes().iter())
            }
            FileHandler { ino, handler } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(handler.to_be_bytes().iter())
            }
            FileIndex { parent, name } => {
                data.extend(parent.to_be_bytes().iter());
                data.extend(name.as_bytes().iter());
            }
        }
        data
    }
}

impl<'a> From<ScopedKey<'a>> for Key {
    fn from(key: ScopedKey<'a>) -> Self {
        key.serialize().into()
    }
}
