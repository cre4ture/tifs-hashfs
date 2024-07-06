use std::collections::HashMap;

use tikv_client::KvPair;

use super::{error::TiFsResult, fs_config::TiFsConfig, index::deserialize_json, inode::{InoAccessTime, InoChangeIterationId, InoInlineData, InoModificationTime, InoSize, InoStorageFileAttr, StorageIno, TiFsHash}, key::InoMetadata};

#[derive(Default)]
pub struct InodesAttrsHashMaps {
    pub existing_hash: HashMap<StorageIno, TiFsHash>,
    pub change_iter_id: HashMap<StorageIno, InoChangeIterationId>,
    pub size: HashMap<StorageIno, InoSize>,
    pub attrs: HashMap<StorageIno, InoStorageFileAttr>,
    pub access_times: HashMap<StorageIno, InoAccessTime>,
    pub modification_times: HashMap<StorageIno, InoModificationTime>,
    pub inline_data: HashMap<StorageIno, InoInlineData>,
}

pub struct KvPairParser {
    pub fs_config: TiFsConfig,
}

impl KvPairParser {

    pub fn parse_inode_attrs_kv_pairs(&self, pairs: Vec<KvPair>
    ) -> TiFsResult<InodesAttrsHashMaps> {
        let mut maps = InodesAttrsHashMaps::default();
        for KvPair(k, v) in pairs {
            let parsed = self.fs_config.key_parser_b(k)?.parse_ino()?;
            match parsed.meta {
                InoMetadata::FullHash => {
                    maps.existing_hash.insert(parsed.ino, v);
                }
                InoMetadata::ChangeIterationId => {
                    let _ = deserialize_json::<InoChangeIterationId>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoChangeIteration: {err:?}");
                        })
                        .map(|id| maps.change_iter_id.insert(parsed.ino, id));
                }
                InoMetadata::Size => {
                    let _ = deserialize_json::<InoSize>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoSize: {err:?}");
                        })
                        .map(|s| maps.size.insert(parsed.ino, s));
                }
                InoMetadata::UnixAttributes => {
                    let _ = deserialize_json::<InoStorageFileAttr>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoStorageFileAttr: {err:?}");
                        })
                        .map(|s| maps.attrs.insert(parsed.ino, s));
                }
                InoMetadata::AccessTime => {
                    let _ = deserialize_json::<InoAccessTime>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoAccessTime: {err:?}");
                        })
                        .map(|s| maps.access_times.insert(parsed.ino, s));
                }
                InoMetadata::ModificationTime => {
                    let _ = deserialize_json::<InoModificationTime>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoModificationTime: {err:?}");
                        })
                        .map(|s| maps.modification_times.insert(parsed.ino, s));
                }
                InoMetadata::InlineData => {
                    let _ = deserialize_json::<InoInlineData>(&v)
                        .map_err(|err| {
                            tracing::error!("failed to parse InoInlineData: {err:?}");
                        })
                        .map(|s| maps.inline_data.insert(parsed.ino, s));
                }
                _ => {}
            }
        }
        Ok(maps)
    }

}
