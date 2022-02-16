use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{
    page_blob_utils::*,
    pages_cache::{PageCacheItem, PagesCache},
    PageBlobRandomAccessError,
};

pub struct PageBlobRandomAccess<TMyPageBlob: MyPageBlob> {
    pages_cache: PagesCache,
    pub page_blob: TMyPageBlob,
    pages_amount: Option<usize>,
    auto_resize_if_requires: bool,
    max_pages_to_write_per_request: usize,
}

impl<TMyPageBlob: MyPageBlob> PageBlobRandomAccess<TMyPageBlob> {
    pub fn new(
        page_blob: TMyPageBlob,
        auto_resize_if_requires: bool,
        max_pages_to_write_per_request: usize,
    ) -> Self {
        Self {
            pages_cache: PagesCache::new(4),
            page_blob,
            pages_amount: None,
            auto_resize_if_requires,
            max_pages_to_write_per_request,
        }
    }

    pub async fn get_pages_amount_in_blob(&mut self) -> Result<usize, AzureStorageError> {
        if self.pages_amount.is_some() {
            return Ok(self.pages_amount.unwrap());
        }

        let props = self.page_blob.get_blob_properties().await?;

        self.pages_amount = Some(props.blob_size);

        return Ok(props.blob_size);
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.page_blob.resize(pages_amount).await?;

        self.pages_amount = Some(pages_amount);

        Ok(())
    }

    async fn read_page(&mut self, page_no: usize) -> Result<&PageCacheItem, AzureStorageError> {
        if self.pages_cache.has_page(page_no) {
            return Ok(self.pages_cache.get_page(page_no).unwrap());
        }

        let data = self.page_blob.get(page_no, 1).await?;

        self.pages_cache.add_page(page_no, data);
        let page = self.pages_cache.get_page(page_no);
        return Ok(page.unwrap());
    }

    pub async fn read(
        &mut self,
        start_pos: usize,
        copy_to: &mut [u8],
    ) -> Result<(), PageBlobRandomAccessError> {
        let pages_amount = self.get_pages_amount_in_blob().await?;

        let max_len = start_pos + copy_to.len();

        let last_page_to_read = get_page_no_from_page_blob_position(max_len, BLOB_PAGE_SIZE);

        if last_page_to_read > pages_amount {
            let msg = format!(
                "Position end is {} which shuld be in blob page: {}, but blob has max pages: {}",
                max_len, last_page_to_read, pages_amount
            );
            return Err(PageBlobRandomAccessError::IndexRangeViolation(msg));
        }

        let page_no =
            crate::page_blob_utils::get_page_no_from_page_blob_position(start_pos, BLOB_PAGE_SIZE);

        let page = self.read_page(page_no).await?;

        let pos_in_page =
            crate::page_blob_utils::get_position_within_page(start_pos, BLOB_PAGE_SIZE);

        copy_to.copy_from_slice(&page.data[pos_in_page..pos_in_page + &copy_to.len()]);

        Ok(())
    }

    pub async fn make_sure_page_is_in_cache(
        &mut self,
        page_no: usize,
    ) -> Result<(), AzureStorageError> {
        let has_page = self.pages_cache.has_page(page_no);

        if has_page {
            return Ok(());
        }

        let page_data = self.page_blob.get(page_no, 1).await?;

        self.pages_cache.add_page(page_no, page_data);

        Ok(())
    }

    pub async fn write(
        &mut self,
        start_pos: usize,

        payload: &[u8],
    ) -> Result<(), PageBlobRandomAccessError> {
        let page_no =
            crate::page_blob_utils::get_page_no_from_page_blob_position(start_pos, BLOB_PAGE_SIZE);

        let pages_in_blob = self.get_pages_amount_in_blob().await?;

        let end_pos = start_pos + payload.len();

        let required_pages_amount = get_required_pages_amount(end_pos, BLOB_PAGE_SIZE);

        if required_pages_amount > pages_in_blob {
            if self.auto_resize_if_requires {
                self.resize(required_pages_amount).await?;
            } else {
                let msg = format!("There is an attempt to write payload to blob starting from:{start_pos}, with len: {len}, which last page should be: {required_pages_amount}, but blob has pages {pages_in_blob}. Consider enable autoresizing", len = payload.len());

                return Err(PageBlobRandomAccessError::IndexRangeViolation(msg));
            }
        }

        {
            self.make_sure_page_is_in_cache(page_no).await?;

            let pos_in_page =
                crate::page_blob_utils::get_position_within_page(start_pos, BLOB_PAGE_SIZE);

            let buf = self.pages_cache.get_page_mut(page_no).unwrap();

            let src = &mut buf[pos_in_page..pos_in_page + payload.len()];

            src.copy_from_slice(payload);
        }

        my_azure_page_blob::my_page_blob_utils::write_pages(
            &self.page_blob,
            page_no,
            self.max_pages_to_write_per_request,
            self.pages_cache.clone_page(page_no).unwrap(),
        )
        .await?;

        Ok(())
    }

    pub async fn create_new(&mut self, pages: usize) -> Result<(), AzureStorageError> {
        self.page_blob.create_if_not_exists(pages).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use my_azure_page_blob::MyPageBlobMock;

    use super::*;

    #[tokio::test]
    async fn test_basic_cases() {
        let page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();
        page_blob.create_if_not_exists(0).await.unwrap();

        let mut random_access = PageBlobRandomAccess::new(page_blob, true, 10);

        random_access
            .write(3, vec![1u8, 2u8, 3u8].as_slice())
            .await
            .unwrap();

        let result = random_access.page_blob.download().await.unwrap();

        assert_eq!(vec![0u8, 0u8, 0u8, 1u8, 2u8, 3u8, 0u8, 0u8], result[0..8]);

        let mut result = Vec::with_capacity(4);
        result.resize(4, 0);
        random_access.read(3, &mut result).await.unwrap();

        assert_eq!(vec![1u8, 2u8, 3u8, 0u8], result);
    }
}
