pub fn get_position_within_page(page_blob_position: usize, page_size: usize) -> usize {
    let page_no = get_page_no_from_page_blob_position(page_blob_position, page_size);
    return page_blob_position - page_no * page_size;
}

//TODO - Moved to read_write::utils module
pub fn get_page_no_from_page_blob_position(page_blob_position: usize, page_size: usize) -> usize {
    return page_blob_position / page_size;
}
