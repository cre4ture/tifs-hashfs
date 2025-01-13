use std::{collections::{HashMap, LinkedList}, ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
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

pub struct WriteTaskData {
    pub data: Bytes,
    pub start: u64,
}

#[derive(Default)]
struct AggregatedBytes {
    pub parts: Vec<Bytes>,
}

impl AggregatedBytes {
    pub fn into_bytes(mut self) -> Bytes {
        if self.parts.len() == 1 {
            return self.parts.pop().unwrap();
        }
        let mut data = BytesMut::new();
        for part in self.parts.into_iter() {
            data.extend(part);
        }
        data.freeze()
    }

    pub fn total_bytes(&self) -> usize {
        self.parts.iter().map(|j|j.len()).reduce(|a,l| { a + l }).unwrap_or(0)
    }
}

#[derive(Default)]
pub struct WriteTaskDataList(pub LinkedList<WriteTaskData>);

impl WriteTaskDataList {
    pub fn write_data_len(&self) -> u64 {
        self.0.iter()
            .map(|j|j.data.len() as u64)
            .reduce(|a,l| { a + l })
            .unwrap_or(0)
    }

    pub fn end_offset(&self) -> Option<u64> {
        if self.0.is_empty() {
            return None;
        }
        let last_task = self.0.back().unwrap();
        Some(last_task.start + last_task.data.len() as u64)
    }

    pub fn pop_from_start(&mut self, data_length: u64) -> Option<WriteTaskData> {
        if data_length == 0 {
            return None;
        }
        if self.0.is_empty() {
            return None;
        }
        let mut aggregation = AggregatedBytes::default();
        let start = self.0.front().unwrap().start;
        while aggregation.total_bytes() < data_length as usize {
            if self.0.is_empty() {
                break;
            }
            let task = self.0.pop_front().unwrap();
            let task_len = task.data.len();
            if task_len > data_length as usize {
                let left = task.data.slice(0..data_length as usize);
                let right = task.data.slice(data_length as usize..);
                aggregation.parts.push(left);
                let new_task = WriteTaskData {
                    data: Bytes::from(right),
                    start: task.start + data_length,
                };
                self.0.push_front(new_task);
            } else {
                aggregation.parts.push(task.data);
            }
        }
        Some(WriteTaskData {
            data: aggregation.into_bytes(),
            start,
        })
    }

    pub fn get_aligned_write_queue_length(&self, block_length: u64) -> AlignedWriteQueueLength {
        let mut non_aligned_start = 0;
        let mut non_aligned_end = 0;
        if self.0.len() == 0 {
            return AlignedWriteQueueLength {
                non_aligned_start,
                aligned_middle: 0,
                non_aligned_end,
                end: 0,
            }
        }
        let first_task = self.0.front().unwrap();
        let last_task = self.0.back().unwrap();
        let begin = first_task.start;
        let end = last_task.start + last_task.data.len() as u64;
        let start_block = begin / block_length;
        let start_block_offset = begin % block_length;
        let middle_start_block = if start_block_offset != 0 {
            non_aligned_start = block_length - start_block_offset;
            start_block + 1
        } else {
            start_block
        };
        let end_block = end / block_length;
        let end_block_offset = end % block_length;
        if end_block_offset != 0 {
            non_aligned_end = end_block_offset;
        }
        let aligned_middle_blocks = end_block - middle_start_block;
        AlignedWriteQueueLength {
            non_aligned_start,
            aligned_middle: aligned_middle_blocks * block_length,
            non_aligned_end,
            end,
        }
    }
}

pub struct WriteTasksCache {
    block_length: u64,
    pub task_queues: HashMap<u64 /* end of write data */, Box<WriteTaskDataList>>,
}

pub struct AlignedWriteQueueLength {
    pub non_aligned_start: u64,
    pub aligned_middle: u64,
    pub non_aligned_end: u64,
    pub end: u64,
}

impl WriteTasksCache {
    pub fn new(block_length: u64) -> Self {
        Self {
            block_length,
            task_queues: HashMap::new(),
        }
    }

    pub fn add_write_task_and_get_queue_length(&mut self, task: WriteTaskData) -> AlignedWriteQueueLength {
        let new_end = task.start + task.data.len() as u64;
        let mut queue = if let Some(queue) = self.task_queues.remove(&task.start) {
            queue
        } else {
            Box::new(WriteTaskDataList::default())
        };
        queue.0.push_back(task);
        let length = queue.get_aligned_write_queue_length(self.block_length);
        self.task_queues.insert(new_end, queue);
        return length;
    }

    pub fn get_total_queue_length(&self) -> u64 {
        self.task_queues.values()
            .map(|j|j.write_data_len())
            .reduce(|a,l| { a + l })
            .unwrap_or(0)
    }
}

pub struct FileHandler {
    pub ino_use: Arc<InoUse>,
    pub open_mode: OpenMode,
    pub mut_data: RwLock<FileHandlerMutData>,
    pub write_cache: RwLock<AsyncParallelPipeStage<BoxFuture<'static, TiFsResult<usize>>>>,
    pub write_accumulator: RwLock<WriteTasksCache>,
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
            write_accumulator: RwLock::new(WriteTasksCache::new(fs_config.block_size)),
            read_ahead: RwLock::new(AsyncParallelPipeStage::new(2)),
            read_ahead_map: RwLock::new(ReadAhead { cached_ranges: RangeSet2::empty() })
        }
    }

    pub fn ino(&self) -> StorageIno {
        self.ino_use.ino()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_task_data_list() {
        let block_length = 10;
        let mut write_task_data_list = WriteTaskDataList::default();
        let data = Bytes::from("0123456789");
        let start = 0;
        let task = WriteTaskData {
            data,
            start,
        };
        write_task_data_list.0.push(task);
        let data_length = write_task_data_list.write_data_len();
        assert_eq!(data_length, 10);
        let write_queue_length = write_task_data_list.get_aligned_write_queue_length(block_length);
        assert_eq!(write_queue_length.non_aligned_start, 0);
        assert_eq!(write_queue_length.aligned_middle, 10);
        assert_eq!(write_queue_length.non_aligned_end, 0);
    }
}
