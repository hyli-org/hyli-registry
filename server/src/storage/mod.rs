use anyhow::Result;
use async_trait::async_trait;

mod local;
mod gcs;

pub use local::LocalStorageBackend;
pub use gcs::GcsStorageBackend;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &'static str;
    async fn read_object(&self, path: &str) -> Result<Option<Vec<u8>>>;
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<()>;
    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn delete_object(&self, path: &str) -> Result<()>;
}
