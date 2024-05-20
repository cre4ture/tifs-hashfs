use std::sync::Arc;

use super::tikv_fs::InoUse;

#[derive(Debug, Clone)]
pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    // TODO: add open flags
    pub cursor: u64,
}

impl FileHandler {
    pub const fn new(ino_use: Arc<InoUse>, cursor: u64) -> Self {
        Self { ino_use, cursor }
    }
}
