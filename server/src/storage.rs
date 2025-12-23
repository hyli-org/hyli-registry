use anyhow::{Context, Result};
use async_trait::async_trait;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::Error as GcsError;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &'static str;
    async fn read_object(&self, path: &str) -> Result<Option<Vec<u8>>>;
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<()>;
    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>>;
}

pub struct LocalStorageBackend {
    root: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn resolve_path(&self, object: &str) -> PathBuf {
        self.root.join(object)
    }

    async fn walk_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = Vec::new();
        let mut dirs = VecDeque::new();
        dirs.push_back(dir.to_path_buf());
        while let Some(current) = dirs.pop_front() {
            let mut read_dir = match fs::read_dir(&current).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            while let Some(entry) = read_dir.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    dirs.push_back(path);
                } else if metadata.is_file() {
                    entries.push(path);
                }
            }
        }
        Ok(entries)
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn read_object(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let path = self.resolve_path(path);
        match fs::read(path).await {
            Ok(data) => Ok(Some(data)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).context("reading local object"),
        }
    }

    async fn write_object(&self, path: &str, data: &[u8]) -> Result<()> {
        let path = self.resolve_path(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, data).await.context("writing local object")
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let base = match prefix {
            Some(prefix) => self.resolve_path(prefix),
            None => self.root.clone(),
        };
        let entries = self.walk_dir(&base).await.context("listing local objects")?;
        let mut objects = Vec::new();
        for path in entries {
            let relative = path
                .strip_prefix(&self.root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            objects.push(relative);
        }
        Ok(objects)
    }
}

pub struct GcsStorageBackend {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl GcsStorageBackend {
    pub async fn new(bucket: String, prefix: Option<String>) -> Result<Self> {
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config);
        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    fn object_path(&self, object: &str) -> String {
        match &self.prefix {
            Some(prefix) if !prefix.is_empty() => format!("{}/{}", prefix.trim_end_matches('/'), object),
            _ => object.to_string(),
        }
    }

    fn strip_prefix(&self, object: &str) -> String {
        match &self.prefix {
            Some(prefix) if !prefix.is_empty() => object
                .strip_prefix(prefix.trim_end_matches('/'))
                .and_then(|suffix| suffix.strip_prefix('/'))
                .unwrap_or(object)
                .to_string(),
            _ => object.to_string(),
        }
    }

    fn is_not_found(err: &GcsError) -> bool {
        matches!(err, GcsError::Response(response) if response.code == 404)
    }
}

#[async_trait]
impl StorageBackend for GcsStorageBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    async fn read_object(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let object = self.object_path(path);
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object,
            ..Default::default()
        };
        match self.client.download_object(&request, &Range::default()).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if Self::is_not_found(&err) => Ok(None),
            Err(err) => Err(err).context("reading gcs object"),
        }
    }

    async fn write_object(&self, path: &str, data: &[u8]) -> Result<()> {
        let object = self.object_path(path);
        let upload_type = UploadType::Simple(Media::new(object.clone()));
        let request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };
        let payload = data.to_vec();
        self.client
            .upload_object(&request, payload, &upload_type)
            .await
            .context("writing gcs object")?;
        Ok(())
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let list_prefix = match (self.prefix.as_deref(), prefix) {
            (Some(base), Some(extra)) if !base.is_empty() && !extra.is_empty() => {
                Some(format!("{}/{}", base.trim_end_matches('/'), extra))
            }
            (Some(base), None) if !base.is_empty() => Some(base.to_string()),
            (_, Some(extra)) => Some(extra.to_string()),
            _ => None,
        };
        let mut objects = Vec::new();
        let mut page_token = None;
        loop {
            let response = self
                .client
                .list_objects(&ListObjectsRequest {
                    bucket: self.bucket.clone(),
                    prefix: list_prefix.clone(),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await
                .context("listing gcs objects")?;
            if let Some(items) = response.items {
                for item in items {
                    objects.push(self.strip_prefix(&item.name));
                }
            }
            match response.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => break,
            }
        }
        Ok(objects)
    }
}
