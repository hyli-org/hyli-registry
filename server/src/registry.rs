use crate::conf::Conf;
use crate::storage::{GcsStorageBackend, LocalStorageBackend, StorageBackend};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use chrono::Utc;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, Opts};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::info;

const INDEX_FILE_NAME: &str = "index.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexFile {
    pub contracts: HashMap<String, ContractIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContractIndex {
    pub programs: HashMap<String, ProgramEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramEntry {
    pub program_id: String,
    pub contract: String,
    pub object_path: String,
    pub metadata_path: String,
    pub size_bytes: u64,
    pub uploaded_at: String,
    pub metadata: ProgramMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramMetadata {
    pub toolchain: String,
    pub commit: String,
    pub zkvm: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgramInfo {
    pub program_id: String,
    pub size_bytes: u64,
    pub uploaded_at: String,
    pub metadata: ProgramMetadata,
}

impl ProgramInfo {
    fn from_entry(entry: &ProgramEntry) -> Self {
        Self {
            program_id: entry.program_id.clone(),
            size_bytes: entry.size_bytes,
            uploaded_at: entry.uploaded_at.clone(),
            metadata: entry.metadata.clone(),
        }
    }
}

pub struct RegistryService {
    storage: Arc<dyn StorageBackend>,
    index: Arc<RwLock<IndexFile>>,
    cache: Arc<RwLock<BinaryCache>>,
    metrics: RegistryMetrics,
}

impl RegistryService {
    pub async fn new(config: &Conf) -> Result<Self> {
        let storage = create_storage_backend(config).await?;
        let metrics = RegistryMetrics::new()?;
        let index = load_or_rebuild_index(storage.as_ref(), &metrics).await?;

        info!(
            "Registry initialized with {} contracts and {} programs",
            index.contracts.len(),
            index
                .contracts
                .values()
                .map(|entry| entry.programs.len() as u64)
                .sum::<u64>()
        );
        Ok(Self {
            storage,
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(RwLock::new(BinaryCache::default())),
            metrics,
        })
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn list_all(&self) -> HashMap<String, Vec<ProgramInfo>> {
        let index = self.index.read().await;
        self.metrics.requests.with_label_values(&["list_all"]).inc();
        index
            .contracts
            .iter()
            .map(|(contract, entry)| {
                let programs = entry
                    .programs
                    .values()
                    .map(ProgramInfo::from_entry)
                    .collect::<Vec<_>>();
                (contract.clone(), programs)
            })
            .collect()
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn list_contract(&self, contract: &str) -> Option<Vec<ProgramInfo>> {
        let index = self.index.read().await;
        self.metrics
            .requests
            .with_label_values(&["list_contract"])
            .inc();
        index.contracts.get(contract).map(|entry| {
            entry
                .programs
                .values()
                .map(ProgramInfo::from_entry)
                .collect()
        })
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn upload(
        &self,
        contract: &str,
        program_id: &str,
        metadata: ProgramMetadata,
        bytes: Bytes,
    ) -> Result<ProgramEntry> {
        let object_path = binary_object_path(contract, program_id);
        let metadata_path = metadata_object_path(contract, program_id);
        let size_bytes = bytes.len() as u64;
        let uploaded_at = Utc::now().to_rfc3339();

        let storage_start = Instant::now();
        self.storage
            .write_object(&object_path, &bytes)
            .await
            .context("storing elf")?;
        self.metrics
            .storage_latency
            .with_label_values(&["write", self.storage.name()])
            .observe(storage_start.elapsed().as_secs_f64());

        let entry = ProgramEntry {
            program_id: program_id.to_string(),
            contract: contract.to_string(),
            object_path: object_path.clone(),
            metadata_path: metadata_path.clone(),
            size_bytes,
            uploaded_at,
            metadata,
        };

        let metadata_bytes = serde_json::to_vec(&entry).context("serializing metadata")?;
        let metadata_start = Instant::now();
        self.storage
            .write_object(&metadata_path, &metadata_bytes)
            .await
            .context("storing metadata")?;
        self.metrics
            .storage_latency
            .with_label_values(&["write_metadata", self.storage.name()])
            .observe(metadata_start.elapsed().as_secs_f64());

        let index_bytes = {
            let mut index = self.index.write().await;
            let contract_entry = index.contracts.entry(contract.to_string()).or_default();
            contract_entry
                .programs
                .insert(program_id.to_string(), entry.clone());
            serde_json::to_vec(&*index).context("serializing index")?
        };

        let index_start = Instant::now();
        self.storage
            .write_object(INDEX_FILE_NAME, &index_bytes)
            .await
            .context("writing index")?;
        self.metrics
            .storage_latency
            .with_label_values(&["write_index", self.storage.name()])
            .observe(index_start.elapsed().as_secs_f64());

        {
            let mut cache = self.cache.write().await;
            cache.insert(contract, program_id, bytes);
        }

        self.metrics.requests.with_label_values(&["upload"]).inc();
        self.metrics
            .bytes
            .with_label_values(&["upload"])
            .inc_by(size_bytes);

        Ok(entry)
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn download(&self, contract: &str, program_id: &str) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.write().await.get_and_touch(contract, program_id) {
            self.metrics.cache_hits.inc();
            self.metrics.requests.with_label_values(&["download"]).inc();
            self.metrics
                .bytes
                .with_label_values(&["download"])
                .inc_by(bytes.len() as u64);
            return Ok(Some(bytes));
        }
        self.metrics.cache_misses.inc();

        let object_path = {
            let index = self.index.read().await;
            let entry = index
                .contracts
                .get(contract)
                .and_then(|contract_entry| contract_entry.programs.get(program_id))
                .cloned();
            match entry {
                Some(entry) => entry.object_path,
                None => return Ok(None),
            }
        };

        let start = Instant::now();
        let bytes = match self.storage.read_object(&object_path).await? {
            Some(bytes) => Bytes::from(bytes),
            None => return Ok(None),
        };
        self.metrics
            .storage_latency
            .with_label_values(&["read", self.storage.name()])
            .observe(start.elapsed().as_secs_f64());

        {
            let mut cache = self.cache.write().await;
            cache.insert(contract, program_id, bytes.clone());
        }

        self.metrics.requests.with_label_values(&["download"]).inc();
        self.metrics
            .bytes
            .with_label_values(&["download"])
            .inc_by(bytes.len() as u64);

        Ok(Some(bytes))
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn delete_program(&self, contract: &str, program_id: &str) -> Result<bool> {
        let entry = {
            let index = self.index.read().await;
            index
                .contracts
                .get(contract)
                .and_then(|contract_entry| contract_entry.programs.get(program_id))
                .cloned()
        };
        let Some(entry) = entry else {
            return Ok(false);
        };

        self.storage
            .delete_object(&entry.object_path)
            .await
            .context("deleting elf")?;
        self.storage
            .delete_object(&entry.metadata_path)
            .await
            .context("deleting metadata")?;

        let index_bytes = {
            let mut index = self.index.write().await;
            if let Some(contract_entry) = index.contracts.get_mut(contract) {
                contract_entry.programs.remove(program_id);
                if contract_entry.programs.is_empty() {
                    index.contracts.remove(contract);
                }
            }
            serde_json::to_vec(&*index).context("serializing index")?
        };
        self.storage
            .write_object(INDEX_FILE_NAME, &index_bytes)
            .await
            .context("writing index")?;

        {
            let mut cache = self.cache.write().await;
            cache.remove_program(contract, program_id);
        }

        self.metrics
            .requests
            .with_label_values(&["delete_program"])
            .inc();

        Ok(true)
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn delete_contract(&self, contract: &str) -> Result<bool> {
        let entries = {
            let index = self.index.read().await;
            index
                .contracts
                .get(contract)
                .map(|entry| entry.programs.values().cloned().collect::<Vec<_>>())
        };
        let Some(entries) = entries else {
            return Ok(false);
        };

        for entry in &entries {
            self.storage
                .delete_object(&entry.object_path)
                .await
                .with_context(|| format!("deleting elf {}", entry.object_path))?;
            self.storage
                .delete_object(&entry.metadata_path)
                .await
                .with_context(|| format!("deleting metadata {}", entry.metadata_path))?;
        }

        let index_bytes = {
            let mut index = self.index.write().await;
            index.contracts.remove(contract);
            serde_json::to_vec(&*index).context("serializing index")?
        };
        self.storage
            .write_object(INDEX_FILE_NAME, &index_bytes)
            .await
            .context("writing index")?;

        {
            let mut cache = self.cache.write().await;
            cache.remove_contract(contract);
        }

        self.metrics
            .requests
            .with_label_values(&["delete_contract"])
            .inc();

        Ok(true)
    }
}

#[derive(Default)]
struct BinaryCache {
    per_contract: HashMap<String, VecDeque<CacheEntry>>,
}

impl BinaryCache {
    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    fn get_and_touch(&mut self, contract: &str, program_id: &str) -> Option<Bytes> {
        let entries = self.per_contract.get_mut(contract)?;
        let position = entries
            .iter()
            .position(|entry| entry.program_id == program_id)?;
        let entry = entries.remove(position)?;
        let bytes = entry.bytes.clone();
        entries.push_front(entry);
        Some(bytes)
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    fn insert(&mut self, contract: &str, program_id: &str, bytes: Bytes) {
        let entries = self.per_contract.entry(contract.to_string()).or_default();
        entries.retain(|entry| entry.program_id != program_id);
        entries.push_front(CacheEntry {
            program_id: program_id.to_string(),
            bytes,
        });
        while entries.len() > 2 {
            entries.pop_back();
        }
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    fn remove_program(&mut self, contract: &str, program_id: &str) {
        if let Some(entries) = self.per_contract.get_mut(contract) {
            entries.retain(|entry| entry.program_id != program_id);
            if entries.is_empty() {
                self.per_contract.remove(contract);
            }
        }
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    fn remove_contract(&mut self, contract: &str) {
        self.per_contract.remove(contract);
    }
}

struct CacheEntry {
    program_id: String,
    bytes: Bytes,
}

struct RegistryMetrics {
    requests: IntCounterVec,
    bytes: IntCounterVec,
    cache_hits: IntCounter,
    cache_misses: IntCounter,
    index_rebuilds: IntCounter,
    storage_latency: HistogramVec,
}

impl RegistryMetrics {
    fn new() -> Result<Self> {
        let requests = IntCounterVec::new(
            Opts::new(
                "hyli_registry_requests_total",
                "Total registry requests by operation.",
            ),
            &["op"],
        )?;
        let bytes = IntCounterVec::new(
            Opts::new(
                "hyli_registry_bytes_total",
                "Total bytes transferred by operation.",
            ),
            &["op"],
        )?;
        let cache_hits = IntCounter::new("hyli_registry_cache_hits_total", "Registry cache hits.")?;
        let cache_misses =
            IntCounter::new("hyli_registry_cache_misses_total", "Registry cache misses.")?;
        let index_rebuilds =
            IntCounter::new("hyli_registry_index_rebuilds_total", "Index rebuild count.")?;
        let storage_latency = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "hyli_registry_storage_latency_seconds",
                "Latency of storage operations.",
            ),
            &["op", "backend"],
        )?;

        let registry = prometheus::default_registry();
        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(bytes.clone()))?;
        registry.register(Box::new(cache_hits.clone()))?;
        registry.register(Box::new(cache_misses.clone()))?;
        registry.register(Box::new(index_rebuilds.clone()))?;
        registry.register(Box::new(storage_latency.clone()))?;

        Ok(Self {
            requests,
            bytes,
            cache_hits,
            cache_misses,
            index_rebuilds,
            storage_latency,
        })
    }
}

fn binary_object_path(contract: &str, program_id: &str) -> String {
    let digest = program_id_digest(program_id);
    format!("{}/{}.elf", contract, digest)
}

fn metadata_object_path(contract: &str, program_id: &str) -> String {
    let digest = program_id_digest(program_id);
    format!("{}/{}.json", contract, digest)
}

fn program_id_digest(program_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(program_id.as_bytes());
    hex::encode(hasher.finalize())
}

async fn create_storage_backend(config: &Conf) -> Result<Arc<dyn StorageBackend>> {
    match config.storage_backend.trim().to_lowercase().as_str() {
        "local" => {
            let root = config
                .local_storage_directory
                .clone()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| PathBuf::from(&config.data_directory).join("registry"));
            Ok(Arc::new(LocalStorageBackend::new(root)))
        }
        "gcs" => {
            let bucket = config
                .gcs_bucket
                .clone()
                .filter(|bucket| !bucket.trim().is_empty())
                .ok_or_else(|| anyhow!("gcs_bucket must be set for gcs backend"))?;
            let prefix = config
                .gcs_prefix
                .clone()
                .filter(|value| !value.trim().is_empty());
            let backend = GcsStorageBackend::new(bucket, prefix).await?;
            Ok(Arc::new(backend))
        }
        backend => Err(anyhow!("unsupported storage_backend: {backend}")),
    }
}

async fn load_or_rebuild_index(
    storage: &dyn StorageBackend,
    metrics: &RegistryMetrics,
) -> Result<IndexFile> {
    match storage.read_object(INDEX_FILE_NAME).await? {
        Some(bytes) => {
            let index: IndexFile = serde_json::from_slice(&bytes).context("parsing index")?;
            Ok(index)
        }
        None => {
            info!("Index file not found, rebuilding index from stored objects");
            metrics.index_rebuilds.inc();
            let objects = storage.list_objects(None).await?;
            let mut index = IndexFile::default();
            for object in objects {
                if object == INDEX_FILE_NAME || !object.ends_with(".json") {
                    continue;
                }
                let Some(metadata_bytes) = storage.read_object(&object).await? else {
                    continue;
                };
                let entry: ProgramEntry = match serde_json::from_slice(&metadata_bytes) {
                    Ok(entry) => entry,
                    Err(_) => continue,
                };
                index
                    .contracts
                    .entry(entry.contract.clone())
                    .or_default()
                    .programs
                    .insert(entry.program_id.clone(), entry);
            }
            let index_bytes = serde_json::to_vec(&index).context("serializing rebuilt index")?;
            storage
                .write_object(INDEX_FILE_NAME, &index_bytes)
                .await
                .context("writing rebuilt index")?;
            Ok(index)
        }
    }
}
