use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::RwLock;

use super::{error::TiFsResult, tikv_fs::InoUse};
use crate::utils::async_parallel_pipe_stage::AsyncParallelPipeStage;

#[derive(Debug)]
pub struct FileHandlerMutData {
    pub cursor: u64,
}

pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    // TODO: add open flags
    pub mut_data: RwLock<FileHandlerMutData>,
    pub write_cache: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<usize>>>>,
}

impl FileHandler {
    pub fn new(ino_use: Arc<InoUse>, cursor: u64, write_cache_in_progress_limit: usize) -> Self {
        Self {
            ino_use,
            mut_data: RwLock::new(FileHandlerMutData {
                cursor,
            }),
            write_cache: RwLock::new(AsyncParallelPipeStage::new(write_cache_in_progress_limit)),
        }
    }

    pub fn ino(&self) -> u64 {
        self.ino_use.ino()
    }
}
