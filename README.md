# Hyli ZKVM Registry

A lightweight registry for ZKVM binaries (ELFs). Applications upload their built contract binaries and later download them by `contract` + `program_id` at runtime. The registry supports both local filesystem and Google Cloud Storage backends, keeps a single JSON index at the root, and exposes a simple HTTP API plus a small web UI.

## What this provides

- **Upload API** (authenticated) for ELFs + metadata.
- **Public read APIs** to list contracts/programs and download ELFs.
- **Delete APIs** (admin key) to remove a program or an entire contract.
- **Storage backends**: local data dir or GCS bucket/prefix.
- **Index file** stored at the storage root; rebuilt if missing.
- **Caching**: in-memory LRU for index and recent binaries to reduce GCS calls.
- **Prometheus metrics** exposed by the existing `/v1/metrics` stack.
- **Uploader CLI/Lib** for sending binaries from CI or tooling.

## Repository layout

- `server/` – HTTP API + storage backends
- `front/` – registry UI
- `hyli-registry-uploader/` – CLI + lib to upload binaries

## Quick start

### 1) Run the server

```bash
cargo run -p server
```

By default, it uses local storage under `data/` and a development API key.

### 2) Run the UI

```bash
cd front
bun install
bun run dev
```

## Configuration

All config values can be set via TOML or `HYLI__...` env vars.

Key settings:

- `api_key`: required for upload endpoints.
- `admin_key`: required for delete endpoints.
- `storage_backend`: `"local"` or `"gcs"`.
- `gcs_bucket`: required when using GCS.
- `gcs_prefix`: optional prefix inside the bucket.
- `data_directory`: base directory (default `data`).
- `local_storage_directory`: optional override for local storage path.
- `rest_server_max_body_size`: set `0` for unlimited upload size.

Example env:

```bash
export HYLI_API_KEY="dev-api"
export HYLI_ADMIN_KEY="dev-admin"
export HYLI_STORAGE_BACKEND="local"
```

## API

### Upload (authenticated)

`POST /api/elfs/:contract`

Headers:
- `x-api-key`: upload API key

Form fields (multipart):
- `program_id`: string (no validation)
- `metadata`: JSON string
  - `toolchain`
  - `commit`
  - `zkvm`
- `file`: ELF binary

Behavior:
- Overwrites if `(contract, program_id)` already exists.
- `program_id` is hashed for storage file names (prevents long filename issues).
- Contract name must be lowercase with no slashes.

### Read (public)

- `GET /api/elfs` – list all contracts + programs
- `GET /api/elfs/:contract` – list programs for a contract
- `GET /api/elfs/:contract/:program_id` – download ELF

### Delete (admin key)

Headers:
- `x-api-key`: admin key

- `DELETE /api/elfs/:contract/:program_id` – delete one program
- `DELETE /api/elfs/:contract` – delete whole contract

## Storage model

- Objects are stored under `:contract/` folder.
- Each program stores:
  - ELF binary: `:contract/:hash.elf`
  - Metadata: `:contract/:hash.json`
- Root `index.json` maps contracts to program entries.
- Index is rebuilt by scanning metadata if `index.json` is missing.

## Caching

- Index cache: keeps latest in memory.
- Binary cache: keeps the 2 latest binaries per contract in memory.

## Uploader CLI / Lib

The uploader can be used as a binary or imported as a library in other crates.

Library entrypoints:

- `upload(UploadRequest)` – send a binary to the registry
- `program_id_hex_from_file(path)` – read bytes and hex-encode (SP1-style)
- `program_id_from_file(path)` – read raw program id from file

CLI subcommands:

- `sp1` – takes an ELF + vk file, hex-encodes program_id from vk
- `risc0` – takes an ELF + explicit program_id

## UI

The frontend provides:

- Contract list + program listing
- Simple stats
- Download links
- Admin login (secret only) + delete actions

Admin key is stored in localStorage as `hyli_registry_admin_key`.

## Metrics

Prometheus metrics are served by the existing `/v1/metrics` endpoint. No extra setup is required; exporters are configured in the stack.

## Development notes

- No file size limits, no content-type restrictions.
- Timestamps are server-generated.
- `program_id` is stored in the index but not used as a filename.

