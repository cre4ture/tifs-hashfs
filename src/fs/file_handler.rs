use std::sync::Arc;

use tokio::sync::RwLock;

use super::tikv_fs::InoUse;

#[derive(Debug)]
pub struct FileHandlerMutData {
    pub cursor: u64,
}

#[derive(Debug)]
pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    // TODO: add open flags
    pub mut_data: RwLock<FileHandlerMutData>,
}

impl FileHandler {
    pub fn new(ino_use: Arc<InoUse>, cursor: u64) -> Self {
        Self {
            ino_use,
            mut_data: RwLock::new(FileHandlerMutData {
                cursor,
            }),
        }
    }

    pub fn ino(&self) -> u64 {
        self.ino_use.ino()
    }
}
