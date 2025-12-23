use super::StorageBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io;
use tracing::info;

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
        info!("Writing object to local storage at path: {}", path);
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
        let entries = self
            .walk_dir(&base)
            .await
            .context("listing local objects")?;
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
