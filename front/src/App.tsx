import { useEffect, useMemo, useState, type CSSProperties } from "react";
import "./App.css";

type ProgramMetadata = {
  toolchain: string;
  commit: string;
  zkvm: string;
};

type ProgramInfo = {
  program_id: string;
  size_bytes: number;
  uploaded_at: string;
  metadata: ProgramMetadata;
};

type RegistryIndex = Record<string, ProgramInfo[]>;

const baseUrl = import.meta.env.VITE_SERVER_BASE_URL || "";

const buildUrl = (path: string) => {
  if (!baseUrl) {
    return path;
  }
  return `${baseUrl}${path}`;
};

const formatBytes = (value: number) => {
  if (value <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    Math.floor(Math.log(value) / Math.log(1024)),
    units.length - 1,
  );
  const scaled = value / Math.pow(1024, exponent);
  const digits = scaled >= 10 ? 1 : 2;
  return `${scaled.toFixed(digits)} ${units[exponent]}`;
};

const formatDate = (value: string | null) => {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
};

const ADMIN_KEY_STORAGE = "hyli_registry_admin_key";

const loadAdminKey = () => {
  if (typeof window === "undefined") {
    return "";
  }
  return window.localStorage.getItem(ADMIN_KEY_STORAGE) || "";
};

function App() {
  const [registry, setRegistry] = useState<RegistryIndex>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [adminKey, setAdminKey] = useState(loadAdminKey);
  const [adminDraft, setAdminDraft] = useState(adminKey);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const fetchRegistry = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(buildUrl("/api/elfs"));
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `HTTP ${response.status}`);
      }
      const data = (await response.json()) as RegistryIndex;
      setRegistry(data || {});
      setLastUpdated(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRegistry();
    const interval = setInterval(fetchRegistry, 30000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    if (adminKey.trim()) {
      window.localStorage.setItem(ADMIN_KEY_STORAGE, adminKey.trim());
    } else {
      window.localStorage.removeItem(ADMIN_KEY_STORAGE);
    }
  }, [adminKey]);

  const stats = useMemo(() => {
    const entries = Object.entries(registry);
    let totalPrograms = 0;
    let totalBytes = 0;
    let latestUpload: string | null = null;
    for (const [, programs] of entries) {
      totalPrograms += programs.length;
      for (const program of programs) {
        totalBytes += program.size_bytes;
        if (!latestUpload || program.uploaded_at > latestUpload) {
          latestUpload = program.uploaded_at;
        }
      }
    }
    return {
      totalContracts: entries.length,
      totalPrograms,
      totalBytes,
      latestUpload,
    };
  }, [registry]);

  const filteredContracts = useMemo(() => {
    const query = filter.trim().toLowerCase();
    const entries = Object.entries(registry);
    if (!query) {
      return entries.sort(([a], [b]) => a.localeCompare(b));
    }
    return entries
      .filter(([contract, programs]) => {
        if (contract.toLowerCase().includes(query)) {
          return true;
        }
        return programs.some(
          (program) =>
            program.program_id.toLowerCase().includes(query) ||
            program.metadata.toolchain?.toLowerCase().includes(query) ||
            program.metadata.zkvm?.toLowerCase().includes(query),
        );
      })
      .sort(([a], [b]) => a.localeCompare(b));
  }, [registry, filter]);

  const isAdmin = adminKey.trim().length > 0;

  const saveAdminKey = () => {
    setAdminKey(adminDraft.trim());
  };

  const clearAdminKey = () => {
    setAdminDraft("");
    setAdminKey("");
  };

  const deleteProgram = async (contract: string, programId: string) => {
    if (!adminKey.trim()) {
      return;
    }
    const confirmed = window.confirm(
      `Delete program ${programId} from ${contract}?`,
    );
    if (!confirmed) {
      return;
    }
    const key = `${contract}:${programId}`;
    setDeleteTarget(key);
    setError(null);
    try {
      const response = await fetch(
        buildUrl(`/api/elfs/${contract}/${programId}`),
        {
          method: "DELETE",
          headers: {
            "x-api-key": adminKey,
          },
        },
      );
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `HTTP ${response.status}`);
      }
      await fetchRegistry();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleteTarget(null);
    }
  };

  const deleteContract = async (contract: string) => {
    if (!adminKey.trim()) {
      return;
    }
    const confirmed = window.confirm(
      `Delete contract ${contract} and all binaries?`,
    );
    if (!confirmed) {
      return;
    }
    setDeleteTarget(contract);
    setError(null);
    try {
      const response = await fetch(buildUrl(`/api/elfs/${contract}`), {
        method: "DELETE",
        headers: {
          "x-api-key": adminKey,
        },
      });
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `HTTP ${response.status}`);
      }
      await fetchRegistry();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleteTarget(null);
    }
  };

  return (
    <div className="page">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Hyli Registry</p>
          <h1>Zero-knowledge binaries, indexed and ready.</h1>
          <p className="subtitle">
            Browse contract ELFs, inspect metadata, and pull the exact program
            you need. Public read access, fast cache, clean index.
          </p>
          <div className="hero-meta">
            <span>Source: {baseUrl || "same origin"}</span>
            <span>Last refresh: {formatDate(lastUpdated)}</span>
          </div>
        </div>
        <div className="hero-panel">
          <div className="stat-card">
            <span className="stat-label">Contracts</span>
            <strong>{stats.totalContracts}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Binaries</span>
            <strong>{stats.totalPrograms}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Total size</span>
            <strong>{formatBytes(stats.totalBytes)}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Latest upload</span>
            <strong>{formatDate(stats.latestUpload)}</strong>
          </div>
        </div>
      </header>

      <section className="controls">
        <div className="search">
          <input
            type="text"
            placeholder="Filter by contract, program id, toolchain, zkvm"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
        </div>
        <div className="controls-actions">
          <div className="admin-login">
            <input
              type="password"
              placeholder="Admin key"
              value={adminDraft}
              onChange={(event) => setAdminDraft(event.target.value)}
            />
            <button className="ghost" onClick={saveAdminKey}>
              {isAdmin ? "Update" : "Set"}
            </button>
            {isAdmin && (
              <button className="ghost" onClick={clearAdminKey}>
                Clear
              </button>
            )}
          </div>
          <button className="refresh" onClick={fetchRegistry}>
            Refresh
          </button>
        </div>
      </section>

      {loading && (
        <div className="status-panel">
          <span className="status-dot" />
          Loading registry data...
        </div>
      )}
      {error && (
        <div className="status-panel error">
          <span className="status-dot" />
          {error}
        </div>
      )}

      <section className="registry-grid">
        {filteredContracts.length === 0 && !loading ? (
          <div className="status-panel">
            <span className="status-dot" />
            No contracts match this filter yet.
          </div>
        ) : (
          filteredContracts.map(([contract, programs], index) => {
            const orderedPrograms = [...programs].sort((a, b) =>
              b.uploaded_at.localeCompare(a.uploaded_at),
            );
            return (
              <article
                className="contract-card"
                key={contract}
                style={{ "--delay": `${index * 70}ms` } as CSSProperties}
              >
                <header>
                  <div>
                    <h2>{contract}</h2>
                    <p>{orderedPrograms.length} binaries</p>
                  </div>
                  <div className="contract-actions">
                    <span className="contract-size">
                      {formatBytes(
                        orderedPrograms.reduce(
                          (sum, program) => sum + program.size_bytes,
                          0,
                        ),
                      )}
                    </span>
                    {isAdmin && (
                      <button
                        className="danger"
                        onClick={() => deleteContract(contract)}
                        disabled={deleteTarget === contract}
                      >
                        Delete contract
                      </button>
                    )}
                  </div>
                </header>
                <div className="program-list">
                  {orderedPrograms.map((program) => (
                    <div className="program-row" key={program.program_id}>
                      <div className="program-meta">
                        <div className="program-id" title={program.program_id}>
                          {program.program_id}
                        </div>
                        <div className="program-details">
                          <span>{formatBytes(program.size_bytes)}</span>
                          <span>{formatDate(program.uploaded_at)}</span>
                          <span>{program.metadata.zkvm}</span>
                        </div>
                      </div>
                      <div className="program-actions">
                        {program.metadata.toolchain && (
                          <span className="badge">
                            {program.metadata.toolchain}
                          </span>
                        )}
                        {program.metadata.commit && (
                          <span className="badge mono">
                            commit: {program.metadata.commit.slice(0, 8)}
                          </span>
                        )}
                        <a
                          className="download"
                          href={buildUrl(
                            `/api/elfs/${contract}/${program.program_id}`,
                          )}
                        >
                          Download
                        </a>
                        {isAdmin && (
                          <button
                            className="danger"
                            onClick={() =>
                              deleteProgram(contract, program.program_id)
                            }
                            disabled={
                              deleteTarget ===
                              `${contract}:${program.program_id}`
                            }
                          >
                            Delete
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </article>
            );
          })
        )}
      </section>
    </div>
  );
}

export default App;
