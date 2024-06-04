

pub fn debug_print_start_and_end_bytes_of_buffer(
    print_len: usize,
    data: &[u8],
) -> String {
    let len = data.len();
    let complete = len <= (print_len+3);
    if complete {
        format!("data[{}]: [{}]",
            data.len(),
            data.escape_ascii(),
        )
    } else {
        let part_len = print_len / 2;
        let start = data.get(0..part_len).unwrap();
        let end = data.get((len-part_len)..len).unwrap();
        format!("data[{}]: [{}...{}]",
            data.len(),
            start.escape_ascii(),
            end.escape_ascii(),
        )
    }
}
