use super::StorageBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::http;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::Error as GcsError;
use tracing::info;

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
            Some(prefix) if !prefix.is_empty() => {
                format!("{}/{}", prefix.trim_end_matches('/'), object)
            }
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
            || matches!(err, GcsError::HttpClient(http_err) if http_err.status() == Some(http::status::StatusCode::NOT_FOUND))
    }
}

#[async_trait]
impl StorageBackend for GcsStorageBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    async fn read_object(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let object = self.object_path(path);
        info!("Reading object from GCS at path: {}", object);
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object,
            ..Default::default()
        };
        match self
            .client
            .download_object(&request, &Range::default())
            .await
        {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if Self::is_not_found(&err) => Ok(None),
            Err(err) => Err(err).context("reading gcs object"),
        }
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, data)))]
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<()> {
        let object = self.object_path(path);
        info!("Writing object to GCS at path: {}", object);
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

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        info!("Listing objects in GCS with prefix: {:?}", prefix);
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

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    async fn delete_object(&self, path: &str) -> Result<()> {
        let object = self.object_path(path);
        info!("Deleting object from GCS at path: {}", object);
        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object,
            ..Default::default()
        };
        match self.client.delete_object(&request).await {
            Ok(()) => Ok(()),
            Err(err) if Self::is_not_found(&err) => Ok(()),
            Err(err) => Err(err).context("deleting gcs object"),
        }
    }
}
