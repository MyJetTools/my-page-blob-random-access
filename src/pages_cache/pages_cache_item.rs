use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct PageCacheItem {
    pub data: Vec<u8>,
    pub page_id: usize,
    pub last_access: DateTimeAsMicroseconds,
}

impl PageCacheItem {
    pub fn new(data: Vec<u8>, page_id: usize) -> Self {
        Self {
            data,
            page_id,
            last_access: DateTimeAsMicroseconds::now(),
        }
    }
}
