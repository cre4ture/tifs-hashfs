use std::ops::{Bound, Range};

use bimap::BiHashMap;
use lazy_static::lazy_static;
use num_traits::FromBytes;
use tikv_client::{BoundRange, Key};
use uuid::Uuid;
use strum::{EnumIter, IntoEnumIterator};

use super::error::TiFsResult;
use super::hash_fs_interface::BlockIndex;
use super::inode::{InoAccessTime, InoChangeIterationId, InoDescription, InoFullHash, InoInlineData, InoLockState, InoSize, InoStorageFileAttr, ModificationTime, ParentStorageIno, StorageDirItem, StorageIno};
use super::meta::{MetaMutable, MetaStatic};
use super::reply::LogicalIno;
use super::tikv_fs::InoUse;
use super::{error::FsError, inode::TiFsHash};

pub const MAX_NAME_LEN: u32 = 1 << 8;

pub const PARENT_OF_ROOT_INODE: ParentStorageIno = ParentStorageIno(StorageIno(0)); // NR: 0
pub const ROOT_INODE: ParentStorageIno = ParentStorageIno(StorageIno(fuser::FUSE_ROOT_ID)); // NR: 1
pub const ROOT_LOGICAL_INODE: LogicalIno = LogicalIno::from_raw(fuser::FUSE_ROOT_ID);

// to avoid that opened inode are deleted when they are removed from the directory,
// we add those opened inodes to a special invisible parent directory:
pub const OPENED_INODE_PARENT_INODE: ParentStorageIno = ParentStorageIno(StorageIno(ROOT_INODE.0.0+1)); // NR: 2
pub const FIRST_DATA_INODE: StorageIno = StorageIno(10); // keep some reserved inodes for later use

/// ATTENTION: Order of enums in this struct matters for serialization!
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum KeyKind {
    KeyLockStates,
    FsMetadataStatic,
    FsMetadata,
    InoMetadata,
    Block, // { ino: u64, block: u64 } => { data: Vec<u8> }
    DirectoryChild, // { parent: u64, name: &'static str } => { ino: u64 }
    ParentLink, // { ino: u64, parent_ino: u64 } => None
    HashedBlock, // { hash: &[u8] },
    InoBlockHashMapping, // { ino: u64, block: u64 } => { hash: &[u8] }
    HashedBlockExists, // { hash: &[u8] } => None
    PendingDelete,
    //HashedBlockUsedBy { hash: &'a[u8], ino: u64, block: u64 },
    NamedHashedBlock, // { hash: &[u8], meta, uuid },
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum HashedBlockMeta {
    ANamedBlock,
    BNamedUsages,
    CCountedNamedUsages,
}


#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy, EnumIter)]
pub enum PendingDeleteMeta {
    AIterationNumber, // u64
    BLastUpdate, // u64, unix timestamp
    CPendingDeletes, // list of hashes
    DActiveWriters, // list of uuids with last fetched iteration number
    EEnd,
}

pub mod key_structs {
    use struct_iterable::Iterable;

    use crate::fs::inode::StorageIno;

    #[derive(Iterable)]
    pub struct ParentLink {
        pub ino: StorageIno,
        pub parent_ino: StorageIno,
    }
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
    FullHash,
    ChangeIterationId, // uuid, not counter
    InlineData,
}

lazy_static!{
    static ref KEY_KIND_IDS: BiHashMap<u8, KeyKind> = {
        KeyKind::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };

    static ref INO_META_KEY_IDS: BiHashMap<u8, InoMetadata> = {
        InoMetadata::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };

    static ref PENDING_DELETE_META_KEY_IDS: BiHashMap<u8, PendingDeleteMeta> = {
        PendingDeleteMeta::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };

    static ref HASHED_BLOCK_META_KEY_IDS: BiHashMap<u8, HashedBlockMeta> = {
        HashedBlockMeta::iter().enumerate().map(|(a,b)|(a as u8,b)).collect()
    };
}

pub type KeyBuffer = Vec<u8>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockAddress {
    pub ino: StorageIno,
    pub index: BlockIndex,
}

pub fn read_big_endian<const N: usize, T: num_traits::FromBytes<Bytes = [u8; N]>>(
    i: &mut impl Iterator<Item = u8>) -> TiFsResult<T>
{
    let bytes = i.array_chunks::<N>().next()
        .ok_or(FsError::UnknownError(format!("deserialization failed!")))?.clone();
    let r: T = FromBytes::from_be_bytes(&bytes);
    Ok(r)
}

pub fn write_big_endian<T: num_traits::ToBytes>(value: T, buf: &mut KeyBuffer) {
    buf.extend_from_slice(value.to_be_bytes().as_ref());
}

pub trait KeyDeSer {
    fn deserialize_from(i: &mut impl Iterator<Item = u8>) -> TiFsResult<Self> where Self: Sized;
    fn deserialize_self_from(&mut self, i: &mut impl Iterator<Item = u8>) -> TiFsResult<()> where Self: Sized {
        Self::deserialize_from(i).map(|x| { *self = x; })
    }
    fn serialize_to(self, buf: &mut KeyBuffer);
}

impl KeyDeSer for StorageIno {
    fn deserialize_from(i: &mut impl Iterator<Item = u8>) -> TiFsResult<Self> {
        read_big_endian(i).map(|x| StorageIno(x) )
    }

    fn deserialize_self_from(&mut self, i: &mut impl Iterator<Item = u8>) -> TiFsResult<()> {
        read_big_endian(i).map(|x| { *self = StorageIno(x); } )
    }

    fn serialize_to(self, buf: &mut KeyBuffer) {
        write_big_endian::<u64>(self.0, buf)
    }
}


impl KeyDeSer for BlockAddress {
    fn deserialize_from(i: &mut impl Iterator<Item = u8>) -> TiFsResult<Self> {
        let ino = StorageIno(read_big_endian::<8, u64>(i)?);
        let index = BlockIndex(read_big_endian::<8, u64>(i)?);
        Ok(Self { ino, index })
    }
    fn deserialize_self_from(&mut self, i: &mut impl Iterator<Item = u8>) -> TiFsResult<()> {
        Self::deserialize_from(i).map(|x|{ *self = x; })
    }
    fn serialize_to(self, buf: &mut KeyBuffer) {
        buf.extend_from_slice(&self.ino.0.to_be_bytes());
        buf.extend_from_slice(&self.index.0.to_be_bytes());
    }
}

pub fn parse_uuid(i: &mut impl Iterator<Item = u8>) -> TiFsResult<Uuid> {
    const UUID_LEN: usize = 16;
    let chunk = i.array_chunks::<UUID_LEN>().next();
    let Some(chunk) = chunk else {
        return Err(FsError::Serialize{
            target: "Uuid",
            typ: "binary",
            msg: format!("parse_lock_key(): key length too small")
        });
    };
    Ok(Uuid::from_bytes_ref(&chunk).clone())
}

pub fn check_file_name(name: &str) -> TiFsResult<()> {
    if name.len() <= MAX_NAME_LEN as usize {
        Ok(())
    } else {
        Err(FsError::NameTooLong {
            file: name.to_string(),
        })
    }
}

pub struct KeyParser<I>
where I: Iterator<Item = u8>
{
    hash_len: usize,
    i: I,
    pub kind: KeyKind,
}

impl<I> KeyParser<I>
where I: Iterator<Item = u8>
{
    pub fn start<'fl>(mut i: I, prefix: &'fl[u8], hash_len: usize) -> TiFsResult<Self> {
        let act_prefix = i.by_ref().take(prefix.len()).collect::<Vec<_>>();
        if act_prefix != prefix {
            return Err(FsError::UnknownError(format!("key prefix mismatch: {:?}", act_prefix)));
        }
        let kind_id = i.next().unwrap_or(0xFF);
        let kind = *KEY_KIND_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown kind_i: {}", kind_id)))?;
        Ok(Self {
            hash_len,
            i,
            kind,
        })
    }
/*
    pub fn parse_t<T: struct_iterable::Iterable + Default>(mut self) -> TiFsResult<(T, Self)> {
        let mut output = T::default();
        for (field_name, field_value) in output.iter() {
            if let Some(de_ser) = field_value.downcast_mut::<dyn KeyDeSer>() {
                de_ser.deserialize_from(&mut self.i);
            }
        }
        Ok((output, self))
    }
    */

    pub fn parse_pending_deletes_x<'fl>(mut self) -> TiFsResult<KeyParserPendingDelete<I>> {
        if self.kind != KeyKind::PendingDelete {
            return Err(FsError::UnknownError(format!("expected a pending delete key. got: {:?}", self.kind)))
        }
        let kind_id = self.i.next().unwrap_or(0xFF).clone();
        let kind = *PENDING_DELETE_META_KEY_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown pending deletes kind: {}", kind_id)))?;
        Ok(KeyParserPendingDelete {
            pre: self,
            meta: kind,
        })
    }

    pub fn parse_ino<'fl>(mut self) -> TiFsResult<KeyParserIno<I>> {
        if self.kind != KeyKind::InoMetadata {
            return Err(FsError::UnknownError(format!("expected a ino metadata key. got: {:?}", self.kind)))
        }
        let ino = StorageIno(read_big_endian::<8, u64>(&mut self.i)?);
        let kind_id = self.i.next().unwrap_or(0xFF);
        let kind = *INO_META_KEY_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown ino kind: {}", kind_id)))?;
        Ok(KeyParserIno {
            pre: self,
            ino,
            meta: kind,
        })
    }

    pub fn parse_hash_block_key_x<'fl>(self) -> TiFsResult<KeyParserHashBlock<I>> {
        if self.kind != KeyKind::HashedBlock {
            return Err(FsError::UnknownError(format!("expected a HashedBlock key. got: {:?}", self.kind)))
        }
        let (mut me, hash) = self.parse_hash()?;
        let kind_id = me.i.next().unwrap_or(0xFF).clone();
        let kind = *HASHED_BLOCK_META_KEY_IDS.get_by_left(&kind_id).ok_or(
            FsError::UnknownError(format!("key with unknown hash block kind: {}", kind_id)))?;
        Ok(KeyParserHashBlock {
            pre: me,
            hash,
            meta: kind,
        })
    }

    pub fn parse_hash(mut self) -> TiFsResult<(Self, TiFsHash)> {
        let hash_len = self.hash_len;
        let hash = (&mut self.i).take(self.hash_len).collect::<Vec<_>>();
        if hash.len() != hash_len {
            return Err(FsError::UnknownError(format!("Key with unexpected hash length: {}", hash.len())));
        }
        Ok((self, hash))
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

    pub fn parse_key_block_address(mut self) -> TiFsResult<BlockAddress> {
        Ok(match self.kind {
            KeyKind::Block => BlockAddress::deserialize_from(&mut self.i)?,
            KeyKind::InoBlockHashMapping => BlockAddress::deserialize_from(&mut self.i)?,
            other => {
                return Err(FsError::UnknownError(format!("parse_key_block_address(): unexpected key_type: {:?}", other)));
            }
        })
    }

    pub fn parse_uuid(mut self) -> TiFsResult<(Self, Uuid)> {
        let uuid = parse_uuid(&mut self.i)?;
        Ok((self, uuid))
    }

    pub fn parse_lock_key(mut self, key_to_lock: &Vec<u8>) -> TiFsResult<Uuid> {
        if self.kind != KeyKind::KeyLockStates {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): unexpected key_type: {:?}", self.kind)));
        }
        let key_to_lock_act = (&mut self.i).take(key_to_lock.len()).collect::<Vec<_>>();
        if key_to_lock != &key_to_lock_act {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): unexpected key_to_lock_act: {:?}", key_to_lock_act)));
        }
        Ok(self.parse_uuid()?.1)
    }

    pub fn parse_directory_child(mut self) -> TiFsResult<(StorageIno, Vec<u8>)> {
        if self.kind != KeyKind::DirectoryChild {
            return Err(FsError::UnknownError(
                format!("parse_lock_key(): unexpected key_type: {:?}", self.kind)));
        }
        let dir_ino = StorageIno::deserialize_from(&mut self.i)?;
        let child_name = self.i.collect::<Vec<_>>();
        Ok((dir_ino, child_name))
    }
}

pub struct KeyParserIno<I>
where I: Iterator<Item = u8>
{
    pub pre: KeyParser<I>,
    pub ino: StorageIno,
    pub meta: InoMetadata,
}

pub struct KeyParserPendingDelete<I>
where I: Iterator<Item = u8>
{
    pub pre: KeyParser<I>,
    pub meta: PendingDeleteMeta,
}

pub struct KeyParserHashBlock<I>
where I: Iterator<Item = u8>
{
    pub pre: KeyParser<I>,
    pub hash: TiFsHash,
    pub meta: HashedBlockMeta,
}

#[derive(Clone)]
pub struct ScopedKeyBuilder {
    pub buf: KeyBuffer,
}

impl ScopedKeyBuilder {
    pub fn new(prefix: &[u8]) -> Self {
        let mut buf = KeyBuffer::with_capacity(128);
        buf.extend_from_slice(prefix);
        Self {
            buf,
        }
    }

    pub fn as_key(self) -> Key {
        Key::from(self.buf)
    }

    // ignore, private
    fn write_key_kind(mut self, kk: KeyKind) -> Self {
        self.buf.push(*KEY_KIND_IDS.get_by_right(&kk).unwrap());
        self
    }

    // final
    pub fn key_lock_state(
        self,
        key: &[u8],
        uuid: Uuid) -> KeyBuffer {
        let mut buf = self.write_key_kind(KeyKind::KeyLockStates).buf;
        buf.extend_from_slice(key);
        buf.extend_from_slice(uuid.as_bytes());
        buf
    }

    fn write_key_de_ser<T: KeyDeSer>(mut self, input: T) -> Self {
        input.serialize_to(&mut self.buf);
        self
    }
/*
    fn write_struct_t<T: struct_iterable::Iterable>(mut self, input: T) -> Self {
        for (field_name, field_value) in input.iter() {
            if let Some(de_ser) = field_value.downcast_ref::<dyn KeyDeSer>() {
                de_ser.serialize_to(&mut self.buf);
            }
        }
        self
    }
    */

    // final
    pub fn meta_static(self) -> KeyBuffer {
        self.write_key_kind(KeyKind::FsMetadataStatic).buf
    }

    // final
    pub fn meta(self) -> KeyBuffer {
        self.write_key_kind(KeyKind::FsMetadata).buf
    }

    pub fn pending_delete_x(self, kind: PendingDeleteMeta) -> Self {
        let mut me = self.write_key_kind(KeyKind::PendingDelete);
        me.buf.push(*PENDING_DELETE_META_KEY_IDS.get_by_right(&kind).unwrap());
        me
    }

    pub fn pending_delete_scan_list(self) -> BoundRange {
        let me = self.write_key_kind(KeyKind::PendingDelete);
        let start = me.clone().pending_delete_x(PendingDeleteMeta::AIterationNumber);
        let end = me.pending_delete_x(PendingDeleteMeta::DActiveWriters);
        BoundRange {
            from: Bound::Included(Key::from(start.buf)),
            to: Bound::Excluded(Key::from(end.buf)),
        }
    }

    pub fn pending_delete_block_hash(self, hash: TiFsHash) -> KeyBuffer {
        let mut me = self.pending_delete_x(PendingDeleteMeta::CPendingDeletes);
        me.buf.extend_from_slice(&hash);
        me.buf
    }

    pub fn pending_delete_active_writer(self, iteration_number: u64, writer_id: Uuid) -> KeyBuffer {
        let mut me = self.pending_delete_x(PendingDeleteMeta::DActiveWriters);
        write_big_endian(iteration_number, &mut me.buf);
        me.buf.extend_from_slice(writer_id.as_bytes());
        me.buf
    }

    // ignore, private
    pub fn inode_x(self, ino: StorageIno, meta: InoMetadata) -> Self {
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
        let mut me = self.write_key_kind(KeyKind::InoBlockHashMapping);
        addr.serialize_to(&mut me.buf);
        me.buf
    }

    pub fn named_hashed_block_x<'fl>(
        self,
        hash: &'fl[u8],
        meta: Option<HashedBlockMeta>,
        name: Option<Uuid>
    ) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::HashedBlock);
        me.buf.extend_from_slice(hash);
        let Some(meta) = meta else {
            return me.buf;
        };
        me.buf.push(*HASHED_BLOCK_META_KEY_IDS.get_by_right(&meta).unwrap());
        let Some(name) = name else {
            return me.buf;
        };
        me.buf.extend_from_slice(name.as_bytes());
        me.buf
    }

    pub fn sub_key_range(self) -> BoundRange {
        let mut end = self.clone();
        *end.buf.last_mut().unwrap() += 1;
        BoundRange {
            from: Bound::Included(Key::from(self.buf)),
            to: Bound::Excluded(Key::from(end.buf)),
        }
    }

    pub fn hashed_block<'fl>(self, hash: &'fl[u8]) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::HashedBlock);
        me.buf.extend_from_slice(hash);
        me.buf
    }

    pub fn named_hashed_block_range<'fl>(self, hash: &'fl[u8]) -> BoundRange {
        let mut me = self.write_key_kind(KeyKind::NamedHashedBlock);
        me.buf.extend_from_slice(hash);
        me.buf.push(*HASHED_BLOCK_META_KEY_IDS.get_by_right(&HashedBlockMeta::ANamedBlock).unwrap());
        me.sub_key_range()
    }

    pub fn named_hashed_block_all_range<'fl>(self, hash: &'fl[u8]) -> BoundRange {
        let mut me = self.write_key_kind(KeyKind::NamedHashedBlock);
        me.buf.extend_from_slice(hash);
        me.sub_key_range()
    }

    pub fn hashed_block_exists(self, hash: &[u8]) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::HashedBlockExists);
        me.buf.extend_from_slice(hash);
        me.buf
    }

    pub fn directory_child(self, parent: StorageIno, name: &[u8]) -> KeyBuffer {
        let mut me = self.write_key_kind(KeyKind::DirectoryChild);
        write_big_endian::<u64>(parent.0, &mut me.buf);
        me.buf.extend_from_slice(name);
        me.buf
    }

    pub fn parent_link(self, ino: StorageIno, parent: ParentStorageIno, name: &[u8]) -> Vec<u8> {
        let mut me = self.write_key_kind(KeyKind::ParentLink);
        ino.serialize_to(&mut me.buf);
        parent.0.serialize_to(&mut me.buf);
        me.buf.extend_from_slice(name);
        me.buf
    }

    pub fn parent_link_scan(self, ino: StorageIno) -> BoundRange {
        let mut me1 = self.write_key_kind(KeyKind::ParentLink);
        let mut me2 = me1.clone();
        ino.serialize_to(&mut me1.buf);
        StorageIno(ino.0+1).serialize_to(&mut me2.buf);
        BoundRange {
            from: Bound::Included(Key::from(me1.buf)),
            to: Bound::Excluded(Key::from(me2.buf)),
        }
    }

    pub fn directory_child_range(self, dir_ino: StorageIno) -> BoundRange {
        let start_key = self.clone().write_key_kind(KeyKind::DirectoryChild)
            .write_key_de_ser(dir_ino).buf;
        let end_key = self.write_key_kind(KeyKind::DirectoryChild)
            .write_key_de_ser(StorageIno(dir_ino.0+1)).buf;
        let range = BoundRange {
            from: std::ops::Bound::Included(Key::from(start_key)),
            to: std::ops::Bound::Excluded(Key::from(end_key)),
        };
        range
    }


    pub fn block_range(self, ino: StorageIno, block_range: Range<BlockIndex>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.clone().block(BlockAddress { ino, index: block_range.start }).into()
            ..self.block(BlockAddress { ino, index: block_range.end}).into()
    }

    pub fn block_hash_range(self, ino: StorageIno, block_range: Range<BlockIndex>) -> Range<Key> {
        debug_assert_ne!(0, ino.0);
        self.clone().block_hash(BlockAddress { ino, index: block_range.start, })
            .into()..self.block_hash(BlockAddress { ino, index: block_range.end }).into()
    }

    pub fn inode_metadata_range(self, ino: StorageIno) -> Range<Key> {
        self.clone().inode_description(ino).into()
            ..self.inode_description(StorageIno(ino.0 + 1)).into() // end not included
    }
}

pub trait KeyGenerator<K, V> {
    fn generate_key(self, k: &K) -> KeyBuffer;
}

impl KeyGenerator<(), MetaStatic> for ScopedKeyBuilder {
    fn generate_key(self, _k: &()) -> KeyBuffer {
        self.meta_static()
    }
}

impl KeyGenerator<(), MetaMutable> for ScopedKeyBuilder {
    fn generate_key(self, _k: &()) -> KeyBuffer {
        self.meta()
    }
}

impl KeyGenerator<(ParentStorageIno, &[u8]), StorageDirItem> for ScopedKeyBuilder {
    fn generate_key(self, k: &(ParentStorageIno, &[u8])) -> KeyBuffer {
        self.directory_child(k.0.0, k.1)
    }
}

impl KeyGenerator<(StorageIno, ParentStorageIno, &[u8]), ()> for ScopedKeyBuilder {
    fn generate_key(self, k: &(StorageIno, ParentStorageIno, &[u8])) -> KeyBuffer {
        self.parent_link(k.0, k.1, k.2)
    }
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

impl KeyGenerator<StorageIno, InoAccessTime> for ScopedKeyBuilder {
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

impl KeyGenerator<StorageIno, InoStorageFileAttr> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::UnixAttributes).buf
    }
}

impl KeyGenerator<StorageIno, InoChangeIterationId> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::ChangeIterationId).buf
    }
}

impl KeyGenerator<StorageIno, InoFullHash> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::FullHash).buf
    }
}

impl KeyGenerator<StorageIno, InoInlineData> for ScopedKeyBuilder {
    fn generate_key(self, k: &StorageIno) -> KeyBuffer {
        self.inode_x(*k, InoMetadata::InlineData).buf
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::{hash_fs_interface::BlockIndex, inode::StorageIno, key::{parse_uuid, read_big_endian, write_big_endian, BlockAddress, InoMetadata, KeyBuffer, KeyKind, KeyParser}, utils::hash_algorithm::HashAlgorithm};

    use super::ScopedKeyBuilder;

    const TEST_PREFIX: &[u8] = b"HelloWorld";

    #[test]
    fn serialize_deserialize_prefix_and_meta() {
        let kb = ScopedKeyBuilder::new(TEST_PREFIX);
        assert_eq!(kb.buf, TEST_PREFIX);
        let buf = kb.meta();
        let mut i = buf.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 16).unwrap();
        assert_eq!(kp.kind, KeyKind::FsMetadata);
    }

    #[test]
    fn serialize_deserialize_ino_meta_access_time() {
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).inode_atime(StorageIno(5));
        let mut i = kb.iter().cloned();
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
        let mut i = kb.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlock);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_hashed_block_exists() {
        let hash = HashAlgorithm::Blake3.calculate_hash(&[1,2,3]);
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).hashed_block_exists(&hash);
        let mut i = kb.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlockExists);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_hashed_block_len_64() {
        let hash = HashAlgorithm::Sha512.calculate_hash(&[1,2,3]);
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).hashed_block(&hash);
        let mut i = kb.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 64).unwrap();
        assert_eq!(kp.kind, KeyKind::HashedBlock);
        let read_hash = kp.parse_key_hashed_block().unwrap();
        assert_eq!(hash, read_hash);
    }

    #[test]
    fn serialize_deserialize_regular_block() {
        let addr = BlockAddress{ino:StorageIno(7), index: BlockIndex(3)};
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).block(addr.clone());
        let mut i = kb.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::Block);
        let read_addr = kp.parse_key_block_address().unwrap();
        assert_eq!(addr, read_addr);
    }

    #[test]
    fn serialize_deserialize_block_hash() {
        let addr = BlockAddress{ino:StorageIno(7), index: BlockIndex(3)};
        let kb = ScopedKeyBuilder::new(TEST_PREFIX).block_hash(addr.clone());
        let mut i = kb.iter().cloned();
        let kp = KeyParser::start(&mut i, TEST_PREFIX, 32).unwrap();
        assert_eq!(kp.kind, KeyKind::InoBlockHashMapping);
        let read_addr = kp.parse_key_block_address().unwrap();
        assert_eq!(addr, read_addr);
    }

    #[test]
    fn serialize_deserialize_big_endian_integer() {
        let mut buf = KeyBuffer::new();
        write_big_endian(1u64 << 40, &mut buf);
        assert_eq!(buf.len(), 8);
        write_big_endian(55u64 << 45, &mut buf);
        assert_eq!(buf.len(), 8*2);
        // ----------------
        let mut i = buf.into_iter();
        let val1 = read_big_endian::<8, u64>(&mut i).unwrap();
        assert_eq!(val1, 1u64 << 40);
        let val2 = read_big_endian::<8, u64>(&mut i).unwrap();
        assert_eq!(val2, 55u64 << 45);
    }

    #[test]
    fn serialize_deserialize_uuid() {
        let in_val1 = uuid::Uuid::new_v4();
        let in_val2 = uuid::Uuid::new_v4();
        let mut buf = KeyBuffer::new();
        buf.extend(in_val1.as_bytes());
        assert_eq!(buf.len(), 16);
        buf.extend(in_val2.as_bytes());
        assert_eq!(buf.len(), 16*2);
        // ----------------
        let mut i = buf.into_iter();
        let val1 = parse_uuid(&mut i).unwrap();
        assert_eq!(val1, in_val1);
        let val2 = parse_uuid(&mut i).unwrap();
        assert_eq!(val2, in_val2);
    }

}
