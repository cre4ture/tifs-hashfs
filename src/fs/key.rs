use std::ops::Range;

use bimap::BiHashMap;
use lazy_static::lazy_static;
use num_traits::FromBytes;
use std::slice::Iter;
use tikv_client::Key;
use uuid::Uuid;
use strum::{EnumIter, IntoEnumIterator};

use super::error::TiFsResult;
use super::inode::{AccessTime, InoDescription, InoLockState, InoSize, ModificationTime, StorageFileAttr, StorageIno};
use super::reply::LogicalIno;
use super::tikv_fs::InoUse;
use super::{error::FsError, inode::TiFsHash};

pub const PARENT_OF_ROOT_INODE: StorageIno = StorageIno(0);
pub const ROOT_INODE: StorageIno = StorageIno(fuser::FUSE_ROOT_ID);
pub const ROOT_LOGICAL_INODE: LogicalIno = LogicalIno::from_raw(fuser::FUSE_ROOT_ID);

/// ATTENTION: Order of enums in this struct matters for serialization!
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum KeyKind {
    KeyLockStates,
    FsMetadataStatic,
    FsMetadata,
    InoMetadata,
    Block, // { ino: u64, block: u64 },
    FileIndex, // { parent: u64, name: &'static str },
    HashedBlock, // { hash: &[u8] },
    BlockHash, // { ino: u64, block: u64 },
    HashedBlockExists, // { hash: &[u8] },
    //HashedBlockUsedBy { hash: &'a[u8], ino: u64, block: u64 },
}

/// ATTENTION: Order of enums in this struct matters for serialization!
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum InoMetadata {
    Description,
    LinkCount,
    Size,
    UnixAttributes,
    LockState,
    AccessTime,
    ModificationTime,
    Opened,
}

lazy_static!{
    static ref KEY_KIND_IDS: BiHashMap<u8, KeyKind> = {
        KeyKind::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };

    static ref INO_META_KEY_IDS: BiHashMap<u8, InoMetadata> = {
        InoMetadata::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };
}

pub type KeyBuffer = Vec<u8>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BlockAddress {
    pub ino: StorageIno,
    pub index: u64,
}

pub fn read_big_endian<const N: usize, T: num_traits::FromBytes<Bytes = [u8; N]>>(i: &mut Iter<u8>) -> TiFsResult<T>
{
    let bytes = i.as_slice().array_chunks::<N>().next()
        .ok_or(FsError::UnknownError(format!("deserialization failed!")))?.clone();
    let r: T = FromBytes::from_be_bytes(&bytes);
    i.advance_by(N).unwrap();
    Ok(r)
}

pub fn write_big_endian<T>(value: u64, buf: &mut KeyBuffer) {
    buf.extend_from_slice(&value.to_be_bytes());
}

impl BlockAddress {
    pub fn deserialize_from(i: &mut Iter<u8>) -> TiFsResult<Self> {
        let ino = StorageIno(read_big_endian::<8, u64>(i)?);
        let index = read_big_endian::<8, u64>(i)?;
        Ok(Self { ino, index })
    }
    pub fn serialize_to(self, buf: &mut KeyBuffer) {
        buf.extend_from_slice(&self.ino.0.to_be_bytes());
        buf.extend_from_slice(&self.index.to_be_bytes());
    }
}


pub struct KeyParser<'ol> {
    hash_len: usize,
    i: &'ol mut Iter<'ol, u8>,
    kind: KeyKind,
}

impl<'ol> KeyParser<'ol> {
    pub fn start<'fl>(i: &'ol mut Iter<'ol, u8>, prefix: &'fl[u8], hash_len: usize) -> TiFsResult<Self> {
        let act_prefix = i.as_slice().get(0..prefix.len())
            .unwrap_or(i.as_slice());
        if act_prefix != prefix {
            return Err(FsError::UnknownError(format!("key prefix mismatch: {:?}", act_prefix)));
        }
        i.advance_by(prefix.len()).unwrap();
        let kind_id = *i.next().unwrap_or(&0xFF);
        let kind = *KEY_KIND_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown kind_i: {}", kind_id)))?;
        Ok(Self {
            hash_len,
            i,
            kind,
        })
    }

    pub fn parse_ino<'fl>(mut self) -> TiFsResult<KeyParserIno<'ol>> {
        if self.kind != KeyKind::InoMetadata {
            return Err(FsError::UnknownError(format!("expected a ino metadata key. got: {:?}", self.kind)))
        }
        let ino = StorageIno(read_big_endian::<8, u64>(&mut self.i)?);
        let kind_id = *self.i.next().unwrap_or(&0xFF);
        let kind = *INO_META_KEY_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown ino kind: {}", kind_id)))?;
        Ok(KeyParserIno {
            pre: self,
            ino,
            meta: kind,
        })
    }

    fn parse_hash(self) -> TiFsResult<(Self, TiFsHash)> {
        let hash_len = self.hash_len;
        let hash = self.i.as_slice();
        if hash.len() != hash_len {
            return Err(FsError::UnknownError(format!("Key with unexpected hash length: {}", hash.len())));
        }
        self.i.advance_by(hash_len).unwrap();
        Ok((self, hash[0..hash_len].to_vec()))
    }

    pub fn parse_key_hashed_block(self) -> TiFsResult<TiFsHash> {
        Ok(match self.kind {
            KeyKind::HashedBlock => self.parse_hash()?.1,
            KeyKind::HashedBlockExists => self.parse_hash()?.1,
            other => {
                return Err(FsError::UnknownError(format!("parse_key_hashed_block(): unexpected key_type: {:?}", other)));
            }
        })
    }

    pub fn parse_key_block_address(self) -> TiFsResult<BlockAddress> {
        Ok(match self.kind {
            KeyKind::Block => BlockAddress::deserialize_from(self.i)?,
            KeyKind::BlockHash => BlockAddress::deserialize_from(self.i)?,
            other => {
                return Err(FsError::UnknownError(format!("parse_key_block_address(): unexpected key_type: {:?}", other)));
            }
        })
    }

    pub fn parse_lock_key(mut self, key_to_lock: &Vec<u8>) -> TiFsResult<Uuid> {
        if self.kind != KeyKind::KeyLockStates {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): unexpected key_type: {:?}", self.kind)));
        }
        let key_to_lock_act = self.i.as_slice().get(0..key_to_lock.len()).unwrap_or(self.i.as_slice());
        if key_to_lock != key_to_lock_act {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): unexpected key_to_lock_act: {:?}", key_to_lock_act)));
        }
        self.i.advance_by(key_to_lock.len());
        let chunk = self.i.as_slice().array_chunks::<16>().next();
        let Some(chunk) = chunk else {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): key length too small")));
        };
        Ok(Uuid::from_bytes_ref(chunk).clone())
    }
}

pub struct KeyParserIno<'ol> {
    pub pre: KeyParser<'ol>,
    pub ino: StorageIno,
    pub meta: InoMetadata,
}

#[derive(Clone)]
pub struct ScopedKeyBuilder {
    buf: KeyBuffer,
}

impl ScopedKeyBuilder {
    pub fn new(prefix: &[u8]) -> Self {
        let mut buf = KeyBuffer::with_capacity(128);
        buf.extend_from_slice(prefix);
        Self {
            buf,
        }
    }

    // ignore, private
    fn write_key_kind(mut self, kk: KeyKind) -> Self {
        self.buf.push(*KEY_KIND_IDS.get_by_right(&kk).unwrap());
        self
    }

    // final
    pub fn key_lock_state(
        self,
        key: Key,
        uuid: Uuid) -> KeyBuffer {
        let mut buf = self.write_key_kind(KeyKind::KeyLockStates).buf;
        buf.append(&mut Vec::from(key));
        buf.extend_from_slice(uuid.as_bytes());
        buf
    }

    // final
    pub fn meta_static(self) -> KeyBuffer {
        self.write_key_kind(KeyKind::FsMetadataStatic).buf
    }

    // final
    pub fn meta(self) -> KeyBuffer {
        self.write_key_kind(KeyKind::FsMetadata).buf
    }

    // ignore, private
    fn inode_x(self, ino: StorageIno, meta: InoMetadata) -> Self {
        let mut me = self.write_key_kind(KeyKind::InoMetadata);
        write_big_endian::<u64>(ino.0, &mut me.buf);
        me.buf.push(*INO_META_KEY_IDS.get_by_right(&meta).unwrap());
        me
    }

    // final
    pub fn inode_description(self, ino: StorageIno) -> KeyBuffer {
        self.inode_x(ino, InoMetadata::Description).buf
    }

    // final
    pub fn inode_size(self, ino: StorageIno) -> KeyBuffer {
        self.inode_x(ino, InoMetadata::Size).buf
    }

    // final
    pub fn inode_lock_state(self, ino: StorageIno) -> KeyBuffer {
        self.inode_x(ino, InoMetadata::LockState).buf
    }

    // final
    pub fn inode_atime(self, ino: StorageIno) -> KeyBuffer {
        self.inode_x(ino, InoMetadata::AccessTime).buf
    }

    pub fn inode_link_count(self, ino: StorageIno) -> KeyBuffer {
        self.inode_x(ino, InoMetadata::LinkCount).buf
    }

    pub fn block(self, addr: BlockAddress) -> KeyBuffer {
        let mut me: ScopedKeyBuilder = self.write_key_kind(KeyKind::Block);
        addr.serialize_to(&mut me.buf);
        me.buf
    }

    pub fn opened_inode(self, ino: StorageIno, instance_id: Uuid) -> KeyBuffer {
        let mut me = self.inode_x(ino, InoMetadata::Opened);
        me.buf.extend_from_slice(instance_id.as_bytes());
        me.buf
    }

    pub fn block_hash(self, addr: BlockAddress) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::BlockHash);
        addr.serialize_to(&mut me.buf);
        me.buf
    }

    pub fn hashed_block<'fl>(self, hash: &'fl[u8]) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::HashedBlock);
        me.buf.extend_from_slice(hash);
        me.buf
    }

    pub fn hashed_block_exists(self, hash: &[u8]) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::HashedBlockExists);
        me.buf.extend_from_slice(hash);
        me.buf
    }

    pub fn root(self) -> KeyBuffer {
        self.inode_description(ROOT_INODE)
    }

    pub fn index(self, parent: StorageIno, name: &str) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::FileIndex);
        write_big_endian::<u64>(parent.0, &mut me.buf);
        me.buf.extend_from_slice(name.as_bytes());
        me.buf
    }

    pub fn block_range(self, ino: StorageIno, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.clone().block(BlockAddress { ino, index: block_range.start }).into()
            ..self.block(BlockAddress { ino, index: block_range.end}).into()
    }

    pub fn block_hash_range(self, ino: StorageIno, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.clone().block_hash(BlockAddress { ino, index: block_range.start, })
            .into()..self.block_hash(BlockAddress { ino, index: block_range.end }).into()
    }

    pub fn inode_range(self, ino_range: Range<StorageIno>) -> Range<Key> {
        self.clone().inode_description(ino_range.start).into()
            ..self.inode_description(ino_range.end).into()
    }
}

pub trait KeyGenerator<K, V> {
    fn generate_key(self, k: &K) -> KeyBuffer;
}

impl KeyGenerator<StorageIno, InoDescription> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_description(*k)
    }
}

impl KeyGenerator<StorageIno, InoSize> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_size(*k)
    }
}

impl KeyGenerator<StorageIno, InoLockState> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_lock_state(*k)
    }
}

impl KeyGenerator<StorageIno, AccessTime> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::AccessTime).buf
    }
}

impl KeyGenerator<StorageIno, ModificationTime> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::ModificationTime).buf
    }
}

impl KeyGenerator<StorageIno, InoUse> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::Opened).buf
    }
}

impl KeyGenerator<StorageIno, StorageFileAttr> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::UnixAttributes).buf
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::{inode::StorageIno, key::{BlockAddress, InoMetadata, KeyKind, KeyParser}, utils::hash_algorithm::HashAlgorithm};

    use super::ScopedKeyBuilder;

    const TEST_PREFIX: &[u8] = b"HelloWorld";

    #[test]
    fn serialize_deserialize_prefix_and_meta() {
        let kb = ScopedKeyBuilder::new(TEST_PREFIX);
        assert_eq!(kb.buf, TEST_PREFIX);
        let buf = kb.meta();
        let mut i = buf.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 16).unwrap();
        assert_eq!(kp.kind, KeyKind::FsMetadata);
    }

    #[test]
    fn serialize_deserialize_ino_meta_access_time() {
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).inode_atime(StorageIno(5));
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 16).unwrap();
        assert_eq!(kp.kind, KeyKind::InoMetadata);
        let kp_ino = kp.parse_ino().unwrap();
        assert_eq!(kp_ino.ino, StorageIno(5));
        assert_eq!(kp_ino.meta, InoMetadata::AccessTime);
    }

    #[test]
    fn serialize_deserialize_hashed_block() {
        let hash = HashAlgorithm::Blake3.calculate_hash(&[1,2,3]);
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).hashed_block(&hash);
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlock);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_hashed_block_exists() {
        let hash = HashAlgorithm::Blake3.calculate_hash(&[1,2,3]);
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).hashed_block_exists(&hash);
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlockExists);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_hashed_block_len_64() {
        let hash = HashAlgorithm::Sha512.calculate_hash(&[1,2,3]);
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).hashed_block(&hash);
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 64).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlock);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_regular_block() {
        let addr = BlockAddress{ino:StorageIno(7), index: 3};
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).block(addr.clone());
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::Block);
        let read_addr = kp.parse_key_block_address().unwrap();
        assert_eq!(addr, read_addr);
    }

    #[test]
    fn serialize_deserialize_block_hash() {
        let addr = BlockAddress{ino:StorageIno(7), index: 3};
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).block_hash(addr.clone());
        let mut i = kb.iter();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::BlockHash);
        let read_addr = kp.parse_key_block_address().unwrap();
        assert_eq!(addr, read_addr);
    }
}
