use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Json, Multipart, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use client_sdk::contract_indexer::AppError;

use hyli_modules::{
    bus::SharedMessageBus,
    module_bus_client, module_handle_messages,
    modules::{BuildApiContextInner, Module},
};
use tower_http::cors::{Any, CorsLayer};

use crate::conf::Conf;
use crate::registry::{ProgramInfo, ProgramMetadata, RegistryService};

pub struct AppModule {
    bus: AppModuleBusClient,
}

pub struct AppModuleCtx {
    pub api: Arc<BuildApiContextInner>,
    pub config: Arc<Conf>,
}

module_bus_client! {
#[derive(Debug)]
pub struct AppModuleBusClient {
}
}

impl Module for AppModule {
    type Context = Arc<AppModuleCtx>;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let registry = RegistryService::new(&ctx.config).await?;
        let state = RouterCtx {
            registry: Arc::new(registry),
            api_key: ctx.config.api_key.clone(),
        };

        // Créer un middleware CORS
        let cors = CorsLayer::new()
            .allow_origin(Any) // Permet toutes les origines (peut être restreint)
            .allow_methods(vec![Method::GET, Method::POST]) // Permet les méthodes nécessaires
            .allow_headers(Any); // Permet tous les en-têtes

        let api = Router::new()
            .route("/_health", get(health))
            .route("/api/elfs", get(list_elfs))
            .route("/api/elfs/{contract}", get(list_contract).post(upload_elf))
            .route("/api/elfs/{contract}/{program_id}", get(download_elf))
            .with_state(state)
            .layer(cors);

        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.merge(api));
            }
        }
        let bus = AppModuleBusClient::new_from_bus(bus.new_handle()).await;

        Ok(AppModule { bus })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
        };

        Ok(())
    }
}

#[derive(Clone)]
struct RouterCtx {
    registry: Arc<RegistryService>,
    api_key: String,
}

async fn health() -> impl IntoResponse {
    Json("OK")
}

// --------------------------------------------------------
//     Routes
// --------------------------------------------------------

const API_KEY_HEADER: &str = "x-api-key";

fn require_api_key(headers: &HeaderMap, expected: &str) -> Result<(), AppError> {
    let key = headers
        .get(API_KEY_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError(StatusCode::UNAUTHORIZED, anyhow::anyhow!("Missing API key")))?;
    if key != expected {
        return Err(AppError(
            StatusCode::UNAUTHORIZED,
            anyhow::anyhow!("Invalid API key"),
        ));
    }
    Ok(())
}

fn validate_contract_name(contract: &str) -> Result<(), AppError> {
    if contract.is_empty()
        || !contract
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow::anyhow!("Invalid contract name"),
        ));
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct UploadMetadataPayload {
    toolchain: String,
    commit: String,
    zkvm: String,
}

#[derive(Debug, serde::Serialize)]
struct UploadResponse {
    program_id: String,
    contract: String,
    size_bytes: u64,
    uploaded_at: String,
    metadata: ProgramMetadata,
}

async fn upload_elf(
    State(state): State<RouterCtx>,
    Path(contract): Path<String>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, AppError> {
    require_api_key(&headers, &state.api_key)?;
    validate_contract_name(&contract)?;

    let mut program_id = None;
    let mut metadata = None;
    let mut file_bytes = None;

    while let Some(field) = multipart.next_field().await? {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "program_id" => {
                program_id = Some(field.text().await?);
            }
            "metadata" => {
                let raw = field.text().await?;
                let parsed: UploadMetadataPayload = serde_json::from_str(&raw).map_err(|err| {
                    AppError(
                        StatusCode::BAD_REQUEST,
                        anyhow::anyhow!("Invalid metadata: {err}"),
                    )
                })?;
                metadata = Some(ProgramMetadata {
                    toolchain: parsed.toolchain,
                    commit: parsed.commit,
                    zkvm: parsed.zkvm,
                });
            }
            "file" => {
                let bytes = field.bytes().await?;
                file_bytes = Some(bytes);
            }
            _ => {}
        }
    }

    let program_id = program_id.ok_or_else(|| {
        AppError(
            StatusCode::BAD_REQUEST,
            anyhow::anyhow!("Missing program_id"),
        )
    })?;
    let metadata = metadata
        .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow::anyhow!("Missing metadata")))?;
    let file_bytes = file_bytes
        .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow::anyhow!("Missing ELF file")))?;

    let entry = state
        .registry
        .upload(&contract, &program_id, metadata, file_bytes)
        .await
        .map_err(|err| AppError(StatusCode::INTERNAL_SERVER_ERROR, err))?;

    Ok(Json(UploadResponse {
        program_id: entry.program_id,
        contract: entry.contract,
        size_bytes: entry.size_bytes,
        uploaded_at: entry.uploaded_at,
        metadata: entry.metadata,
    }))
}

async fn list_elfs(
    State(state): State<RouterCtx>,
) -> Result<Json<HashMap<String, Vec<ProgramInfo>>>, AppError> {
    let contracts = state.registry.list_all().await;
    Ok(Json(contracts))
}

async fn list_contract(
    State(state): State<RouterCtx>,
    Path(contract): Path<String>,
) -> Result<Json<Vec<ProgramInfo>>, AppError> {
    validate_contract_name(&contract)?;
    match state.registry.list_contract(&contract).await {
        Some(entries) => Ok(Json(entries)),
        None => Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow::anyhow!("Contract not found"),
        )),
    }
}

async fn download_elf(
    State(state): State<RouterCtx>,
    Path((contract, program_id)): Path<(String, String)>,
) -> Result<Response, AppError> {
    validate_contract_name(&contract)?;
    let bytes = match state.registry.download(&contract, &program_id).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(AppError(
                StatusCode::NOT_FOUND,
                anyhow::anyhow!("ELF not found"),
            ))
        }
        Err(err) => return Err(AppError(StatusCode::INTERNAL_SERVER_ERROR, err)),
    };

    let mut response = bytes.into_response();
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}
