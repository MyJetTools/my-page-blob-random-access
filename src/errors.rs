use my_azure_storage_sdk::AzureStorageError;

#[derive(Debug)]
pub enum PageBlobRandomAccessError {
    IndexRangeViolation(String),
    AzureStorageError(AzureStorageError),
}

impl From<AzureStorageError> for PageBlobRandomAccessError {
    fn from(src: AzureStorageError) -> Self {
        Self::AzureStorageError(src)
    }
}
