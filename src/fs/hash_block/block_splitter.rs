use std::ops::Range;


#[derive(Default)]
pub struct BlockIndexAndData<'a> {
    pub block_index: u64,
    pub data: &'a[u8],
}

#[derive(Default)]
pub struct BlockSplitter<'a> {
    pub block_size: u64,
    pub start: u64,
    pub data: &'a[u8],
    pub first_data_start_position: usize,
    pub first_data: BlockIndexAndData<'a>,
    pub mid_data: BlockIndexAndData<'a>,
    pub last_data: BlockIndexAndData<'a>,
}

impl<'a> BlockSplitter<'a> {
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

        let remaining_after_start_block = rest.len() - self.first_data.data.len();
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
