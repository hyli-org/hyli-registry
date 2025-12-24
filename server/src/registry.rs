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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalStorageBackend;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    fn test_metrics() -> RegistryMetrics {
        let requests = IntCounterVec::new(
            Opts::new(
                "hyli_registry_requests_total_test",
                "Total registry requests by operation.",
            ),
            &["op"],
        )
        .unwrap();
        let bytes = IntCounterVec::new(
            Opts::new(
                "hyli_registry_bytes_total_test",
                "Total bytes transferred by operation.",
            ),
            &["op"],
        )
        .unwrap();
        let cache_hits = IntCounter::new(
            "hyli_registry_cache_hits_total_test",
            "Registry cache hits.",
        )
        .unwrap();
        let cache_misses = IntCounter::new(
            "hyli_registry_cache_misses_total_test",
            "Registry cache misses.",
        )
        .unwrap();
        let index_rebuilds = IntCounter::new(
            "hyli_registry_index_rebuilds_total_test",
            "Index rebuild count.",
        )
        .unwrap();
        let storage_latency = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "hyli_registry_storage_latency_seconds_test",
                "Latency of storage operations.",
            ),
            &["op", "backend"],
        )
        .unwrap();

        RegistryMetrics {
            requests,
            bytes,
            cache_hits,
            cache_misses,
            index_rebuilds,
            storage_latency,
        }
    }

    async fn make_service() -> (RegistryService, TempDir) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(LocalStorageBackend::new(temp_dir.path().to_path_buf()));
        let metrics = test_metrics();
        let index = load_or_rebuild_index(storage.as_ref(), &metrics)
            .await
            .expect("load index");
        let service = RegistryService {
            storage,
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(RwLock::new(BinaryCache::default())),
            metrics,
        };
        (service, temp_dir)
    }

    fn sample_metadata(toolchain: &str) -> ProgramMetadata {
        ProgramMetadata {
            toolchain: toolchain.to_string(),
            commit: "abc123".to_string(),
            zkvm: "sp1".to_string(),
        }
    }

    #[tokio::test]
    async fn program_id_hash_paths_are_stable() {
        let digest = program_id_digest("hello");
        assert_eq!(
            digest,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        let object_path = binary_object_path("contract", "hello");
        let metadata_path = metadata_object_path("contract", "hello");
        assert_eq!(
            object_path,
            "contract/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.elf"
        );
        assert_eq!(
            metadata_path,
            "contract/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.json"
        );
    }

    #[tokio::test]
    async fn upload_overwrite_updates_index_and_storage() {
        let (service, _temp_dir) = make_service().await;
        let contract = "orders";
        let program_id = "program-a";

        service
            .upload(
                contract,
                program_id,
                sample_metadata("toolchain-v1"),
                Bytes::from_static(b"first"),
            )
            .await
            .expect("upload v1");

        service
            .upload(
                contract,
                program_id,
                sample_metadata("toolchain-v2"),
                Bytes::from_static(b"second"),
            )
            .await
            .expect("upload v2");

        let index = service.index.read().await;
        let entry = index
            .contracts
            .get(contract)
            .and_then(|contract_entry| contract_entry.programs.get(program_id))
            .expect("entry present");
        assert_eq!(entry.metadata.toolchain, "toolchain-v2");
        assert_eq!(entry.size_bytes, 6);

        let stored = service
            .storage
            .read_object(&entry.object_path)
            .await
            .expect("read object");
        assert_eq!(stored.as_deref(), Some(b"second".as_slice()));

        let metadata_bytes = service
            .storage
            .read_object(&entry.metadata_path)
            .await
            .expect("read metadata")
            .expect("metadata exists");
        let stored_entry: ProgramEntry =
            serde_json::from_slice(&metadata_bytes).expect("parse metadata");
        assert_eq!(stored_entry.metadata.toolchain, "toolchain-v2");
        assert_eq!(stored_entry.size_bytes, 6);
    }

    #[tokio::test]
    async fn delete_program_removes_objects_and_updates_index() {
        let (service, _temp_dir) = make_service().await;
        let contract = "orders";

        service
            .upload(
                contract,
                "program-a",
                sample_metadata("toolchain-a"),
                Bytes::from_static(b"alpha"),
            )
            .await
            .expect("upload a");
        service
            .upload(
                contract,
                "program-b",
                sample_metadata("toolchain-b"),
                Bytes::from_static(b"beta"),
            )
            .await
            .expect("upload b");

        let deleted = service
            .delete_program(contract, "program-a")
            .await
            .expect("delete program");
        assert!(deleted);

        let index = service.index.read().await;
        let contract_entry = index.contracts.get(contract).expect("contract exists");
        assert_eq!(contract_entry.programs.len(), 1);
        assert!(contract_entry.programs.contains_key("program-b"));

        let removed_entry = contract_entry.programs.get("program-a");
        assert!(removed_entry.is_none());
    }

    #[tokio::test]
    async fn delete_program_removes_storage_objects() {
        let (service, _temp_dir) = make_service().await;
        let contract = "orders";
        let program_id = "program-a";

        let entry = service
            .upload(
                contract,
                program_id,
                sample_metadata("toolchain-a"),
                Bytes::from_static(b"alpha"),
            )
            .await
            .expect("upload");

        service
            .delete_program(contract, program_id)
            .await
            .expect("delete");

        let object = service
            .storage
            .read_object(&entry.object_path)
            .await
            .expect("read object");
        assert!(object.is_none());
        let metadata = service
            .storage
            .read_object(&entry.metadata_path)
            .await
            .expect("read metadata");
        assert!(metadata.is_none());
    }

    #[tokio::test]
    async fn delete_contract_removes_all_programs() {
        let (service, _temp_dir) = make_service().await;
        let contract = "orders";

        let entry_a = service
            .upload(
                contract,
                "program-a",
                sample_metadata("toolchain-a"),
                Bytes::from_static(b"alpha"),
            )
            .await
            .expect("upload a");
        let entry_b = service
            .upload(
                contract,
                "program-b",
                sample_metadata("toolchain-b"),
                Bytes::from_static(b"beta"),
            )
            .await
            .expect("upload b");

        let deleted = service
            .delete_contract(contract)
            .await
            .expect("delete contract");
        assert!(deleted);

        let index = service.index.read().await;
        assert!(index.contracts.is_empty());

        let object_a = service
            .storage
            .read_object(&entry_a.object_path)
            .await
            .expect("read object a");
        assert!(object_a.is_none());
        let object_b = service
            .storage
            .read_object(&entry_b.object_path)
            .await
            .expect("read object b");
        assert!(object_b.is_none());
    }

    #[tokio::test]
    async fn rebuild_index_from_metadata() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(LocalStorageBackend::new(temp_dir.path().to_path_buf()));
        let contract = "orders";
        let program_id = "program-a";
        let entry = ProgramEntry {
            program_id: program_id.to_string(),
            contract: contract.to_string(),
            object_path: binary_object_path(contract, program_id),
            metadata_path: metadata_object_path(contract, program_id),
            size_bytes: 42,
            uploaded_at: "2024-01-01T00:00:00Z".to_string(),
            metadata: sample_metadata("toolchain-a"),
        };
        let metadata_bytes = serde_json::to_vec(&entry).expect("serialize metadata");
        storage
            .write_object(&entry.metadata_path, &metadata_bytes)
            .await
            .expect("write metadata");

        let metrics = test_metrics();
        let index = load_or_rebuild_index(storage.as_ref(), &metrics)
            .await
            .expect("rebuild index");
        let contract_entry = index.contracts.get(contract).expect("contract exists");
        let stored = contract_entry
            .programs
            .get(program_id)
            .expect("program exists");
        assert_eq!(stored.size_bytes, 42);

        let index_bytes = storage
            .read_object(INDEX_FILE_NAME)
            .await
            .expect("read index")
            .expect("index exists");
        let disk_index: IndexFile = serde_json::from_slice(&index_bytes).expect("parse index");
        assert!(disk_index.contracts.contains_key(contract));
    }
}
