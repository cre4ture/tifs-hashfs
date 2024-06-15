use std::{collections::HashMap, ops::Range, sync::Arc};

use futures::future::BoxFuture;
use range_collections::{RangeSet, RangeSet2};
use tokio::sync::RwLock;

use super::{error::TiFsResult, fs_config::TiFsConfig, inode::StorageIno, open_modes::OpenMode, reply::InoKind, tikv_fs::InoUse};
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

#[derive(Debug)]
pub struct FileHandlerMutData {
    pub cursor: HashMap<InoKind, u64>,
}

pub type DataRangeSet = RangeSet<[u64; 2]>;

pub struct ReadAhead {
    cached_ranges: RangeSet2<u64>,
}

impl ReadAhead {
    pub fn get_remaining_to_be_read_size(&self, range: Range<u64>) -> DataRangeSet {
        let other_range_set: DataRangeSet = RangeSet2::from(range);
        let remaining: DataRangeSet = other_range_set.difference(&self.cached_ranges);
        remaining
    }

    pub fn update_read_size(&mut self, range: Range<u64>) {
        let other_range_set: DataRangeSet = RangeSet2::from(range);
        self.cached_ranges.union_with(&other_range_set);
    }
}

pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    pub open_mode: OpenMode,
    pub mut_data: RwLock<FileHandlerMutData>,
    pub write_cache: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<usize>>>>,
    pub read_ahead: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<()>>>>,
    pub read_ahead_map: RwLock<ReadAhead>,
}

impl FileHandler {
    pub fn new(
        ino_use: Arc<InoUse>,
        open_mode: OpenMode,
        fs_config: TiFsConfig
    ) -> Self {
        Self {
            ino_use,
            open_mode,
            mut_data: RwLock::new(FileHandlerMutData {
               cursor: HashMap::new(),
            }),
            write_cache: RwLock::new(AsyncParallelPipeStage::new(fs_config.write_in_progress_limit)),
            read_ahead: RwLock::new(AsyncParallelPipeStage::new(2)),
            read_ahead_map: RwLock::new(ReadAhead { cached_ranges: RangeSet2::empty() })
        }
    }

    pub fn ino(&self) -> StorageIno {
        self.ino_use.ino()
    }
}
