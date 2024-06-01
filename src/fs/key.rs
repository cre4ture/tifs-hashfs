use std::{convert::TryInto, mem::size_of};
use std::ops::Range;

use tikv_client::Key;
use uuid::Uuid;

use super::inode::{StorageIno, UInode};
use super::reply::LogicalIno;
use super::{error::{FsError, Result}, inode::{self, BlockAddress, TiFsHash}};

pub const ROOT_INODE: StorageIno = StorageIno(fuser::FUSE_ROOT_ID);
pub const ROOT_LOGICAL_INODE: LogicalIno = LogicalIno::from_raw(fuser::FUSE_ROOT_ID);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy)]
pub enum ScopedKeyKind<'a> {
    Meta,
    Inode(u64),
    Block { ino: u64, block: u64 },
    FileHandler { ino: u64, handler: u64 },
    FileIndex { parent: u64, name: &'a str },
    HashedBlock { hash: &'a[u8] },
    HashOfBlock { ino: u64, block: u64 },
    HashedBlockExists { hash: &'a[u8] },
    OpenedInode { ino: UInode, uuid: [u8; 16] },
    //HashedBlockUsedBy { hash: &'a[u8], ino: u64, block: u64 },
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

    pub const fn inode(&self, ino: StorageIno) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::Inode(ino.0),
        }
    }

    pub const fn block(&self, ino: StorageIno, block: u64) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::Block { ino: ino.0, block },
        }
    }

    pub const fn opened_inode(&self, ino: StorageIno, instance_id: Uuid) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::OpenedInode { ino: ino.0, uuid: instance_id.into_bytes() },
        }
    }

    pub const fn block_hash(&self, addr: BlockAddress) -> ScopedKey {
        ScopedKey{
            prefix: self.prefix, key_type: ScopedKeyKind::HashOfBlock { ino: addr.ino.0, block: addr.index },
        }
    }

    pub const fn hashed_block<'fl>(&self, hash: &'a inode::Hash) -> ScopedKey<'a> {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::HashedBlock { hash: hash.as_bytes() },
        }
    }

    pub const fn hashed_block_exists<'fl>(&self, hash: &'a inode::Hash) -> ScopedKey<'a> {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::HashedBlockExists { hash: hash.as_bytes() },
        }
    }

    pub const fn root(&self) -> ScopedKey {
        self.inode(ROOT_INODE)
    }

    pub const fn handler(&self, ino: StorageIno, handler: u64) -> ScopedKey {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::FileHandler { ino: ino.0, handler }
        }
    }

    pub fn index(&self, parent: StorageIno, name: &'a str) -> ScopedKey {
        ScopedKey {
            prefix: &self.prefix, key_type: ScopedKeyKind::FileIndex { parent: parent.0, name }
        }
    }

    pub fn block_range(&self, ino: StorageIno, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.block(ino, block_range.start).into()..self.block(ino, block_range.end).into()
    }

    pub fn block_hash_range(&self, ino: StorageIno, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.block_hash(BlockAddress { ino, index: block_range.start, })
            .into()..self.block_hash(BlockAddress { ino, index: block_range.end }).into()
    }

    pub fn inode_range(&self, ino_range: Range<StorageIno>) -> Range<Key> {
        self.inode(ino_range.start).into()..self.inode(ino_range.end).into()
    }

    pub fn parse_hash_from_dyn_sized_array(data: &[u8]) -> Option<TiFsHash> {
        let static_len: Option<&[u8;blake3::OUT_LEN]> = data.try_into().ok();
        static_len.and_then(|sl| Some(TiFsHash::from_bytes(*sl)))
    }

    pub fn parse_key_hashed_block(&self, key: &'a [u8]) -> Option<inode::Hash> {
        let o = self.parse(key).ok();
        if let Some(key) = o {
            match key.key_type {
                ScopedKeyKind::HashedBlock { hash } => Self::parse_hash_from_dyn_sized_array(hash),
                ScopedKeyKind::HashedBlockExists { hash } => Self::parse_hash_from_dyn_sized_array(hash),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn parse_key_block_address(&self, key: &'a [u8]) -> Option<BlockAddress> {
        let o = self.parse(key);
        if let Ok(key) = o {
            match key.key_type {
                ScopedKeyKind::Block { ino, block } => Some(BlockAddress{ino: StorageIno(ino), index: block}),
                ScopedKeyKind::HashOfBlock { ino, block } => Some(BlockAddress{ino: StorageIno(ino), index: block}),
                other => {
                    tracing::error!("parse_key_block_address(): unexpected key_type: {:?}", other);
                    None
                }
            }
        } else {
            tracing::error!("parse_key_block_address(): failed parsing key. Err: {:?}, Data: {:?}", o.unwrap_err(), key);
            None
        }
    }

    pub fn parse(&self, key: &'a [u8]) -> Result<ScopedKey> {
        let invalid_key = || FsError::InvalidScopedKey(key.to_owned());
        let (prefix, key) = key.split_at(self.prefix.len());
        if prefix != self.prefix {
            return Err(FsError::UnknownError("key with invalid prefix!".into()));
        }
        let (scope, data) = key.split_first().ok_or_else(invalid_key)?;
        let kind = match *scope {
            ScopedKey::META => ScopedKeyKind::Meta,
            ScopedKey::INODE => {
                let ino = u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                ScopedKeyKind::Inode(ino)
            }
            ScopedKey::BLOCK => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let block = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                ScopedKeyKind::Block { ino, block }
            }
            ScopedKey::HANDLER => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let handler = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                ScopedKeyKind::FileHandler { ino, handler }
            }
            ScopedKey::INDEX => {
                let parent =
                    u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                let name = std::str::from_utf8(&data[size_of::<u64>()..]).map_err(|_| invalid_key())?;
                ScopedKeyKind::FileIndex { parent, name }
            }
            ScopedKey::HASHED_BLOCK => {
                ScopedKeyKind::HashedBlock { hash: data }
            }
            ScopedKey::HASH_OF_BLOCK => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let block = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                ScopedKeyKind::HashOfBlock { ino, block }
            }
            ScopedKey::HASHED_BLOCK_EXISTS => {
                ScopedKeyKind::HashedBlockExists { hash: data }
            }
            ScopedKey::OPENED_INODE => {
                let ino = u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                let uuid = data[size_of::<u64>()..].array_chunks().next().ok_or_else(invalid_key)?;
                ScopedKeyKind::OpenedInode { ino, uuid: uuid.clone() }
            }
            _ => return Err(invalid_key()),
        };

        Ok(ScopedKey{key_type: kind, prefix})
    }
}

impl<'a> ScopedKey<'a> {
    const META: u8 = 0;
    const INODE: u8 = 1;
    const BLOCK: u8 = 2;
    const HANDLER: u8 = 3;
    const INDEX: u8 = 4;
    const HASHED_BLOCK: u8 = 5;
    const HASH_OF_BLOCK: u8 = 6;
    const HASHED_BLOCK_EXISTS: u8 = 7;
    const OPENED_INODE: u8 = 8;

    pub fn scope(&self) -> u8 {
        use ScopedKeyKind::*;

        match self.key_type {
            Meta => Self::META,
            Inode(_) => Self::INODE,
            Block { ino: _, block: _ } => Self::BLOCK,
            FileHandler { ino: _, handler: _ } => Self::HANDLER,
            FileIndex { parent: _, name: _ } => Self::INDEX,
            HashedBlock { hash: _ } => Self::HASHED_BLOCK,
            HashOfBlock { ino: _, block: _ } => Self::HASH_OF_BLOCK,
            HashedBlockExists { hash: _ } => Self::HASHED_BLOCK_EXISTS,
            OpenedInode { ino: _, uuid: _ } => Self::OPENED_INODE,
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
            HashedBlock { hash: _ } => size_of::<inode::Hash>(),
            HashOfBlock { ino: _, block: _ } => size_of::<u64>() * 2,
            HashedBlockExists { hash: _ } => size_of::<inode::Hash>(),
            OpenedInode { ino: _, uuid: _ } => size_of::<UInode>() + 16,
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
            HashedBlock { hash } => {
                data.extend(hash)
            }
            HashOfBlock { ino, block } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(block.to_be_bytes().iter())
            }
            HashedBlockExists { hash } => {
                data.extend(hash)
            }
            OpenedInode { ino, uuid } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(uuid.iter());
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
