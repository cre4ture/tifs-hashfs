use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use tokio::sync::RwLock;

use super::{error::TiFsResult, inode::StorageIno, reply::InoKind, tikv_fs::InoUse};
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

#[derive(Debug)]
pub struct FileHandlerMutData {
    pub cursor: HashMap<InoKind, u64>,
}

pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    // TODO: add open flags
    pub mut_data: RwLock<FileHandlerMutData>,
    pub write_cache: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<usize>>>>,
    pub read_ahead: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<()>>>>
}

impl FileHandler {
    pub fn new(ino_use: Arc<InoUse>, write_cache_in_progress_limit: usize) -> Self {
        Self {
            ino_use,
            mut_data: RwLock::new(FileHandlerMutData {
               cursor: HashMap::new(),
            }),
            write_cache: RwLock::new(AsyncParallelPipeStage::new(write_cache_in_progress_limit)),
            read_ahead: RwLock::new(AsyncParallelPipeStage::new(2)),
        }
    }

    pub fn ino(&self) -> StorageIno {
        self.ino_use.ino()
    }
}
