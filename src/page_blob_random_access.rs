use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::pages_cache::{PageCacheItem, PagesCache};

pub struct PageBlobRandomAccess<TMyPageBlob: MyPageBlob> {
    pages_cache: PagesCache,
    pub page_blob: TMyPageBlob,
    blob_size: Option<usize>,
}

impl<TMyPageBlob: MyPageBlob> PageBlobRandomAccess<TMyPageBlob> {
    pub fn new(page_blob: TMyPageBlob) -> Self {
        Self {
            pages_cache: PagesCache::new(4),
            page_blob,
            blob_size: None,
        }
    }

    pub async fn get_blob_size(&mut self) -> Result<usize, AzureStorageError> {
        if self.blob_size.is_some() {
            return Ok(self.blob_size.unwrap());
        }

        let props = self.page_blob.get_blob_properties().await?;

        self.blob_size = Some(props.blob_size);

        return Ok(self.blob_size.unwrap());
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
    ) -> Result<(), AzureStorageError> {
        let blob_size = self.get_blob_size().await?;

        let max_len = start_pos + copy_to.len();

        if max_len > blob_size {
            return Err(AzureStorageError::UnknownError {
                msg: "insufficient size".to_string(),
            });
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
        max_pages_to_write: usize,
        payload: &[u8],
    ) -> Result<(), AzureStorageError> {
        let page_no =
            crate::page_blob_utils::get_page_no_from_page_blob_position(start_pos, BLOB_PAGE_SIZE);

        {
            self.make_sure_page_is_in_cache(page_no).await?;

            let pos_in_page =
                crate::page_blob_utils::get_position_within_page(start_pos, BLOB_PAGE_SIZE);

            let buf = self.pages_cache.get_page_mut(page_no).unwrap();

            &buf[pos_in_page..pos_in_page + payload.len()].copy_from_slice(payload);
        }

        self.page_blob
            .save_pages(
                page_no,
                max_pages_to_write,
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
