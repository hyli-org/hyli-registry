use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

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
    })
    .to_string();

    let form = reqwest::multipart::Form::new()
        .text("program_id", request.program_id.to_string())
        .text("metadata", metadata)
        .part(
            "file",
            reqwest::multipart::Part::bytes(binary_bytes)
                .file_name("program.bin")
                .mime_str("application/octet-stream")?,
        );

    let url = format!(
        "{}/api/elfs/{}",
        request.server_url.trim_end_matches('/'),
        request.contract
    );

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .header("x-api-key", request.api_key)
        .multipart(form)
        .send()
        .await
        .context("Failed to send upload request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Upload failed: {status} {body}"));
    }

    let body = response.text().await.unwrap_or_default();

    Ok(UploadResponse {
        program_id: request.program_id.to_string(),
        body,
    })
}
