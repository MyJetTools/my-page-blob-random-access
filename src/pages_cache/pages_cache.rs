use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::PageCacheItem;

pub struct PagesCache {
    max_pages_size: usize,
    data: HashMap<usize, PageCacheItem>,
}

impl PagesCache {
    pub fn new(max_pages_size: usize) -> Self {
        Self {
            max_pages_size,
            data: HashMap::new(),
        }
    }

    fn gc(&mut self) {
        if self.data.len() <= self.max_pages_size {
            return;
        }

        let mut by_date: BTreeMap<i64, usize> = BTreeMap::new();

        for itm in self.data.values() {
            by_date.insert(itm.last_access.unix_microseconds, itm.page_id);
        }

        for id in by_date.values() {
            self.data.remove(id);
            if self.data.len() <= self.max_pages_size {
                return;
            }
        }
    }

    pub fn add_page(&mut self, page_id: usize, page_data: Vec<u8>) {
        self.data
            .insert(page_id, PageCacheItem::new(page_data, page_id));
        self.gc();
    }

    pub fn get_page(&mut self, page_id: usize) -> Option<&PageCacheItem> {
        let result = self.data.get_mut(&page_id)?;
        result.last_access = DateTimeAsMicroseconds::now();

        Some(result)
    }

    pub fn get_page_mut<'t>(&'t mut self, page_id: usize) -> Option<&'t mut [u8]> {
        let result = self.data.get_mut(&page_id)?;
        result.last_access = DateTimeAsMicroseconds::now();

        let result = result.data.as_mut_slice();
        Some(result)
    }

    pub fn has_page(&self, page_id: usize) -> bool {
        self.data.contains_key(&page_id)
    }

    pub fn clone_page(&self, page_id: usize) -> Option<Vec<u8>> {
        let result = self.data.get(&page_id)?;
        Some(result.data.clone())
    }

    /*
    pub fn fill_pages_which_are_in_cache(
        &self,
        start_page_id: usize,
        pages_amount: usize,
        buffer_to_fill: &mut [u8],
    ) -> Vec<usize> {
        let mut missing_pages = Vec::new();

        let mut pos: usize = 0;

        let mut page_id = start_page_id;
        while page_id < start_page_id + pages_amount {
            let cache_data = self.data.get(&page_id);

            if cache_data.is_none() {
                missing_pages.push(page_id);
            } else {
                let cache_data = cache_data.unwrap();

                &buffer_to_fill[pos..pos + BLOB_PAGE_SIZE]
                    .copy_from_slice(cache_data.data.as_slice());
            }

            page_id += 1;
            pos += BLOB_PAGE_SIZE;
        }

        missing_pages
    }
    */
}
