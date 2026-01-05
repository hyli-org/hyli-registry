use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct UploadRequest<'a> {
    pub server_url: &'a str,
    pub api_key: &'a str,
    pub contract: &'a str,
    pub program_id: &'a str,
    pub binary_path: &'a Path,
    pub toolchain: &'a str,
    pub commit: &'a str,
    pub zkvm: &'a str,
}

#[derive(Debug, Clone)]
pub struct UploadResponse {
    pub program_id: String,
    pub body: String,
}

pub fn program_id_hex_from_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read program id file {}", path.display()))?;
    Ok(hex::encode(bytes))
}

pub fn program_id_from_file(path: &Path) -> Result<String> {
    let raw = fs::read(path)
        .with_context(|| format!("Failed to read program id file {}", path.display()))?;
    let text = String::from_utf8(raw).context("Program id file is not valid UTF-8")?;
    Ok(text.trim().to_string())
}

/// Core upload function that sends binary bytes to the registry
async fn upload_bytes(
    server_url: &str,
    api_key: &str,
    contract: &str,
    program_id: &str,
    binary_bytes: Vec<u8>,
    metadata: JsonValue,
) -> Result<UploadResponse> {
    let binary_size = binary_bytes.len();
    tracing::info!(
        program_id = %program_id,
        contract = %contract,
        binary_size = %binary_size,
        metadata = %metadata,
        "Starting upload to registry"
    );

    let form = reqwest::multipart::Form::new()
        .text("program_id", program_id.to_string())
        .text("metadata", metadata.to_string())
        .part(
            "file",
            reqwest::multipart::Part::bytes(binary_bytes)
                .file_name("program.bin")
                .mime_str("application/octet-stream")?,
        );

    let url = format!("{}/api/elfs/{}", server_url.trim_end_matches('/'), contract);
    tracing::debug!(url = %url, "Sending POST request");

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .header("x-api-key", api_key)
        .multipart(form)
        .send()
        .await
        .context("Failed to send upload request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        tracing::error!(
            status = %status,
            body = %body,
            program_id = %program_id,
            "Upload failed"
        );
        return Err(anyhow!("Upload failed: {status} {body}"));
    }

    let body = response.text().await.unwrap_or_default();
    tracing::info!(
        program_id = %program_id,
        status = %status,
        "Upload successful"
    );

    Ok(UploadResponse {
        program_id: program_id.to_string(),
        body,
    })
}

/// Upload an ELF binary with minimal metadata (zkvm only)
/// Reads server URL from HYLI_REGISTRY_URL env var and API key from HYLI_REGISTRY_API_KEY
/// Additional metadata fields can be provided via the `additional_metadata` parameter
pub async fn upload_elf(
    elf_bytes: &[u8],
    program_id: &str,
    contract: &str,
    zkvm: &str,
    additional_metadata: Option<JsonValue>,
) -> Result<UploadResponse> {
    tracing::debug!("Reading registry configuration from environment variables");
    let server_url = std::env::var("HYLI_REGISTRY_URL")
        .context("HYLI_REGISTRY_URL environment variable not set")?;
    let api_key = std::env::var("HYLI_REGISTRY_API_KEY")
        .context("HYLI_REGISTRY_API_KEY environment variable not set")?;

    tracing::debug!(server_url = %server_url, "Using registry URL from environment");

    let mut metadata = serde_json::json!({
        "zkvm": zkvm,
    });

    // Merge additional metadata if provided
    if let Some(additional) = additional_metadata {
        if let (Some(base_obj), Some(add_obj)) = (metadata.as_object_mut(), additional.as_object())
        {
            tracing::debug!("Merging additional metadata fields");
            for (key, value) in add_obj {
                base_obj.insert(key.clone(), value.clone());
            }
        }
    }

    upload_bytes(
        &server_url,
        &api_key,
        contract,
        program_id,
        elf_bytes.to_vec(),
        metadata,
    )
    .await
}

pub async fn upload(request: UploadRequest<'_>) -> Result<UploadResponse> {
    let binary_bytes = fs::read(request.binary_path).with_context(|| {
        format!(
            "Failed to read binary file {}",
            request.binary_path.display()
        )
    })?;

    let metadata = serde_json::json!({
        "toolchain": request.toolchain,
        "commit": request.commit,
        "zkvm": request.zkvm,
    });

    upload_bytes(
        request.server_url,
        request.api_key,
        request.contract,
        request.program_id,
        binary_bytes,
        metadata,
    )
    .await
}

/// Download an ELF binary from the registry
/// Reads server URL from HYLI_REGISTRY_URL env var and API key from HYLI_REGISTRY_API_KEY
pub async fn download_elf(contract: &str, program_id: &str) -> Result<Vec<u8>> {
    tracing::debug!("Reading registry configuration from environment variables");
    let server_url = std::env::var("HYLI_REGISTRY_URL")
        .context("HYLI_REGISTRY_URL environment variable not set")?;
    let api_key = std::env::var("HYLI_REGISTRY_API_KEY")
        .context("HYLI_REGISTRY_API_KEY environment variable not set")?;

    tracing::info!(
        program_id = %program_id,
        contract = %contract,
        "Downloading ELF from registry"
    );

    let url = format!(
        "{}/api/elfs/{}/{}",
        server_url.trim_end_matches('/'),
        contract,
        program_id
    );
    tracing::debug!(url = %url, "Sending GET request");

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("x-api-key", &api_key)
        .send()
        .await
        .context("Failed to send download request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        tracing::error!(
            status = %status,
            body = %body,
            program_id = %program_id,
            contract = %contract,
            "Download failed"
        );
        return Err(anyhow!("Download failed: {status} {body}"));
    }

    let bytes = response
        .bytes()
        .await
        .context("Failed to read response body")?;

    tracing::info!(
        program_id = %program_id,
        contract = %contract,
        size = %bytes.len(),
        "Download successful"
    );

    Ok(bytes.to_vec())
}
