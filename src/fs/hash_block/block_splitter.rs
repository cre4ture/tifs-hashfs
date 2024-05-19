use std::ops::Range;

#[derive(Default)]
pub struct BlockIndexAndData<'a> {
    pub block_index: u64,
    pub data: &'a[u8],
}

#[derive(Default)]
pub struct BlockSplitterWrite<'a> {
    pub block_size: u64,
    pub start: u64,
    pub data: &'a[u8],
    pub first_data_start_position: usize,
    pub first_data: BlockIndexAndData<'a>,
    pub mid_data: BlockIndexAndData<'a>,
    pub last_data: BlockIndexAndData<'a>,
}

impl<'a> BlockSplitterWrite<'a> {
    pub fn new(block_size: u64, start: u64, data: &'a[u8]) -> Self {
        let mut instance = Self::default();
        instance.block_size = block_size;
        instance.start = start;
        instance.data = data;
        instance.get_irregular_start_and_rest();
        instance
    }

    fn get_irregular_start_and_rest(&mut self) {
        let mut block_index = self.start / self.block_size;
        self.first_data.block_index = block_index;
        self.first_data_start_position = (self.start % self.block_size) as usize;
        let first_block_remaining_space = self.block_size as usize - self.first_data_start_position;
        let first_block_write_length = first_block_remaining_space.min(self.data.len());
        let rest = if first_block_write_length == self.block_size as usize{
            // no special handling of first block needed
            self.data
        } else {
            let (first_block, rest) = self.data.split_at(first_block_write_length);
            self.first_data.data = first_block;
            block_index += 1;
            rest
        };

        let remaining_after_start_block = rest.len();
        let full_blocks = remaining_after_start_block / self.block_size as usize;
        let begin_of_last_irregular_block = full_blocks * self.block_size as usize;
        let (mid, end_block) = &rest.split_at(begin_of_last_irregular_block);
        self.mid_data.block_index = block_index;
        self.mid_data.data = mid;
        self.last_data.block_index = block_index + full_blocks as u64;
        if end_block.len() > 0 {
            self.last_data.data = end_block;
        } else {
            // such that "last_data.block_index + 1" refers to block index after the last valid
            self.last_data.block_index -= 1;
        }
    }

    pub fn get_range(&self) -> Range<u64> {
        self.first_data.block_index..self.last_data.block_index+1
    }
}

pub struct BlockSplitterRead {
    pub block_size: u64,
    pub start: u64,
    pub size: u64,
    pub first_block_index: u64,
    pub first_block_read_offset: u64,
    pub bytes_to_read_first_block: u64,
    pub block_count: u64,
    pub end_block_index: u64,
}

impl BlockSplitterRead {
    pub fn new(block_size: u64, start: u64, size: u64) -> Self {
        let first_block = start / block_size;
        let first_block_read_offset = start % block_size;
        let bytes_to_write_first_block = (block_size - first_block_read_offset).min(size);
        let remaining_size = size - bytes_to_write_first_block;
        let last_block = first_block + (remaining_size / block_size);
        let remaining_bytes_last_block = remaining_size % block_size;
        let mut block_count = last_block - first_block;
        if bytes_to_write_first_block > 0 {
            block_count += 1;
        }
        if remaining_bytes_last_block > 0 {
            block_count += 1;
        }
        Self {
            block_size,
            size,
            start,
            first_block_index: first_block,
            first_block_read_offset,
            bytes_to_read_first_block: bytes_to_write_first_block,
            block_count,
            end_block_index: first_block + block_count,
        }
    }
}


#[cfg(test)]
mod test_writer {
    use super::BlockSplitterWrite;
    use lazy_static::lazy_static;

    lazy_static!{
        static ref TEST_DATA: Vec<u8> = (0u8..255).chain(0u8..255).chain(0u8..255).chain(0u8..255)
                                    .chain(0u8..255).chain(0u8..255).chain(0u8..255).collect::<Vec<_>>();
    }

    // ========================================== write first block partially+fully
    #[test]
    fn write_first_block_from_start_partially() {
        let bs = BlockSplitterWrite::new(100, 0, &TEST_DATA[0..30]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.data, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 0);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_first_block_in_middle_partially() {
        let bs = BlockSplitterWrite::new(100, 30, &TEST_DATA[0..30]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 30);
        assert_eq!(bs.data, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 30);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 0);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_first_block_at_end_partially() {
        let bs = BlockSplitterWrite::new(100, 70, &TEST_DATA[0..30]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 70);
        assert_eq!(bs.data, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 70);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 0);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_first_block_fully() {
        let bs = BlockSplitterWrite::new(100, 0, &TEST_DATA[0..100]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.data, &TEST_DATA[0..100]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 0);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..100]);
        assert_eq!(bs.last_data.block_index, 0);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    // ========================================== write second block partially+fully
    #[test]
    fn write_second_block_from_start_partially() {
        let bs = BlockSplitterWrite::new(100, 100, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 1);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 2);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_second_block_in_middle_partially() {
        let bs = BlockSplitterWrite::new(100, 130, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 30);
        assert_eq!(bs.first_data.block_index, 1);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 2);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_second_block_at_end_partially() {
        let bs = BlockSplitterWrite::new(100, 170, &TEST_DATA[0..30]);
        assert_eq!(bs.first_data_start_position, 70);
        assert_eq!(bs.first_data.block_index, 1);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 2);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_second_block_fully() {
        let bs = BlockSplitterWrite::new(100, 100, &TEST_DATA[10..110]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 1);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[10..110]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    // ========================================== write first 2 blocks partially+fully
    #[test]
    fn write_first_2_block_from_start_partially() {
        let bs = BlockSplitterWrite::new(100, 0, &TEST_DATA[0..130]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.data, &TEST_DATA[0..130]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 0);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..100]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[100..130]);
    }

    #[test]
    fn write_first_2_block_in_middle_partially() {
        let bs = BlockSplitterWrite::new(100, 30, &TEST_DATA[0..90]);
        assert_eq!(bs.first_data_start_position, 30);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..70]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[70..90]);
    }

    #[test]
    fn write_first_2_block_at_end_partially() {
        let bs = BlockSplitterWrite::new(100, 70, &TEST_DATA[0..130]);
        assert_eq!(bs.first_data_start_position, 70);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 1);
        assert_eq!(bs.mid_data.data, &TEST_DATA[30..130]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_first_2_block_fully() {
        let bs = BlockSplitterWrite::new(100, 0, &TEST_DATA[0..200]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.data, &TEST_DATA[0..200]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 0);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 0);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..200]);
        assert_eq!(bs.last_data.block_index, 1);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    // ========================================== write 3 blocks at offset 10 partially+fully
    #[test]
    fn write_3_block_at_offset_10_from_start_partially() {
        let bs = BlockSplitterWrite::new(100, 1000, &TEST_DATA[0..230]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 1000);
        assert_eq!(bs.data, &TEST_DATA[0..230]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 10);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 10);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..200]);
        assert_eq!(bs.last_data.block_index, 12);
        assert_eq!(bs.last_data.data, &TEST_DATA[200..230]);
    }

    #[test]
    fn write_3_block_at_offset_10_in_middle_partially() {
        let bs = BlockSplitterWrite::new(100, 1030, &TEST_DATA[0..190]);
        assert_eq!(bs.first_data_start_position, 30);
        assert_eq!(bs.first_data.block_index, 10);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..70]);
        assert_eq!(bs.mid_data.block_index, 11);
        assert_eq!(bs.mid_data.data, &TEST_DATA[70..170]);
        assert_eq!(bs.last_data.block_index, 12);
        assert_eq!(bs.last_data.data, &TEST_DATA[170..190]);
    }

    #[test]
    fn write_3_block_at_offset_10_at_end_partially() {
        let bs = BlockSplitterWrite::new(100, 1070, &TEST_DATA[0..230]);
        assert_eq!(bs.first_data_start_position, 70);
        assert_eq!(bs.first_data.block_index, 10);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..30]);
        assert_eq!(bs.mid_data.block_index, 11);
        assert_eq!(bs.mid_data.data, &TEST_DATA[30..230]);
        assert_eq!(bs.last_data.block_index, 12);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }

    #[test]
    fn write_3_block_at_offset_10_fully() {
        let bs = BlockSplitterWrite::new(100, 1000, &TEST_DATA[0..300]);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 1000);
        assert_eq!(bs.data, &TEST_DATA[0..300]);
        assert_eq!(bs.first_data_start_position, 0);
        assert_eq!(bs.first_data.block_index, 10);
        assert_eq!(bs.first_data.data, &TEST_DATA[0..0]);
        assert_eq!(bs.mid_data.block_index, 10);
        assert_eq!(bs.mid_data.data, &TEST_DATA[0..300]);
        assert_eq!(bs.last_data.block_index, 12);
        assert_eq!(bs.last_data.data, &TEST_DATA[0..0]);
    }
}

#[cfg(test)]
mod test_reader {
    use super::BlockSplitterRead;

    // ========================================== read first block partially+fully
    #[test]
    fn read_first_block_from_start_partially() {
        let bs = BlockSplitterRead::new(100, 0, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 1);
    }

    #[test]
    fn read_first_block_middle_partially() {
        let bs = BlockSplitterRead::new(100, 30, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 30);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 1);
    }

    #[test]
    fn read_first_block_end_partially() {
        let bs = BlockSplitterRead::new(100, 60, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 60);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 60);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 1);
    }

    #[test]
    fn read_first_block_fully() {
        let bs = BlockSplitterRead::new(100, 0, 100);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 0);
        assert_eq!(bs.size, 100);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 1);
    }

    // ========================================== read second block partially+fully
    #[test]
    fn read_second_block_from_start_partially() {
        let bs = BlockSplitterRead::new(100, 100, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 100);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 1);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_second_block_middle_partially() {
        let bs = BlockSplitterRead::new(100, 130, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 130);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 1);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_second_block_end_partially() {
        let bs = BlockSplitterRead::new(100, 160, 40);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 160);
        assert_eq!(bs.size, 40);
        assert_eq!(bs.first_block_index, 1);
        assert_eq!(bs.first_block_read_offset, 60);
        assert_eq!(bs.bytes_to_read_first_block, 40);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_second_block_fully() {
        let bs = BlockSplitterRead::new(100, 100, 100);
        assert_eq!(bs.block_size, 100);
        assert_eq!(bs.start, 100);
        assert_eq!(bs.size, 100);
        assert_eq!(bs.first_block_index, 1);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 1);
        assert_eq!(bs.end_block_index, 2);
    }

    // ========================================== read first 2 blocks partially+fully
    #[test]
    fn read_first_two_blocks_partially_from_start() {
        let bs = BlockSplitterRead::new(100, 0, 150);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 2);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_first_two_blocks_partially_middle() {
        let bs = BlockSplitterRead::new(100, 30, 110);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 70);
        assert_eq!(bs.block_count, 2);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_first_two_blocks_partially_end() {
        let bs = BlockSplitterRead::new(100, 30, 170);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 70);
        assert_eq!(bs.block_count, 2);
        assert_eq!(bs.end_block_index, 2);
    }

    #[test]
    fn read_first_two_blocks_fully() {
        let bs = BlockSplitterRead::new(100, 0, 200);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 2);
        assert_eq!(bs.end_block_index, 2);
    }

    // ========================================== read first 3 blocks partially+fully
    #[test]
    fn read_first_3_blocks_partially_from_start() {
        let bs = BlockSplitterRead::new(100, 0, 230);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 3);
        assert_eq!(bs.end_block_index, 3);
    }

    #[test]
    fn read_first_3_blocks_partially_middle() {
        let bs = BlockSplitterRead::new(100, 90, 120);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 90);
        assert_eq!(bs.bytes_to_read_first_block, 10);
        assert_eq!(bs.block_count, 3);
        assert_eq!(bs.end_block_index, 3);
    }

    #[test]
    fn read_first_3_blocks_partially_end() {
        let bs = BlockSplitterRead::new(100, 30, 270);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 70);
        assert_eq!(bs.block_count, 3);
        assert_eq!(bs.end_block_index, 3);
    }

    #[test]
    fn read_first_3_blocks_fully() {
        let bs = BlockSplitterRead::new(100, 0, 300);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 3);
        assert_eq!(bs.end_block_index, 3);
    }

    // ========================================== read first 4 blocks partially+fully
    #[test]
    fn read_first_4_blocks_partially_from_start() {
        let bs = BlockSplitterRead::new(100, 0, 330);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 4);
    }

    #[test]
    fn read_first_4_blocks_partially_middle() {
        let bs = BlockSplitterRead::new(100, 90, 220);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 90);
        assert_eq!(bs.bytes_to_read_first_block, 10);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 4);
    }

    #[test]
    fn read_first_4_blocks_partially_end() {
        let bs = BlockSplitterRead::new(100, 30, 370);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 70);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 4);
    }

    #[test]
    fn read_first_4_blocks_fully() {
        let bs = BlockSplitterRead::new(100, 0, 400);
        assert_eq!(bs.first_block_index, 0);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 4);
    }

    // ========================================== read 4 blocks with offset 10 partially+fully
    #[test]
    fn read_4_blocks_offset_10_partially_from_start() {
        let bs = BlockSplitterRead::new(100, 1000, 330);
        assert_eq!(bs.first_block_index, 10);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 14);
    }

    #[test]
    fn read_4_blocks_offset_10_partially_middle() {
        let bs = BlockSplitterRead::new(100, 1090, 220);
        assert_eq!(bs.first_block_index, 10);
        assert_eq!(bs.first_block_read_offset, 90);
        assert_eq!(bs.bytes_to_read_first_block, 10);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 14);
    }

    #[test]
    fn read_4_blocks_offset_10_partially_end() {
        let bs = BlockSplitterRead::new(100, 1030, 370);
        assert_eq!(bs.first_block_index, 10);
        assert_eq!(bs.first_block_read_offset, 30);
        assert_eq!(bs.bytes_to_read_first_block, 70);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 14);
    }

    #[test]
    fn read_4_blocks_offset_10_fully() {
        let bs = BlockSplitterRead::new(100, 1000, 400);
        assert_eq!(bs.first_block_index, 10);
        assert_eq!(bs.first_block_read_offset, 0);
        assert_eq!(bs.bytes_to_read_first_block, 100);
        assert_eq!(bs.block_count, 4);
        assert_eq!(bs.end_block_index, 14);
    }



}
