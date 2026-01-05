use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use hyli_registry::{program_id_hex_from_file, upload, UploadRequest};

#[derive(Debug, Parser)]
#[command(author, version, about = "Upload ZKVM binaries to the Hyli registry")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Upload an SP1 ELF using a verification key (vk)
    Sp1(Sp1Args),
    /// Upload a RISC0 image with a provided program_id
    Risc0(Risc0Args),
}

#[derive(Debug, Parser)]
struct CommonArgs {
    /// Registry base URL (e.g. http://localhost:9003)
    #[arg(
        long,
        env = "HYLI_REGISTRY_URL",
        default_value = "http://localhost:9003"
    )]
    server_url: String,
    /// API key for the registry (x-api-key)
    #[arg(long, env = "HYLI_REGISTRY_API_KEY", default_value = "dev")]
    api_key: String,
    /// Contract name (lowercase, no slashes)
    #[arg(long)]
    contract: String,
    /// Toolchain identifier (e.g. cargo-sp1)
    #[arg(long)]
    toolchain: String,
    /// Commit identifier
    #[arg(long)]
    commit: String,
}

#[derive(Debug, Parser)]
struct Sp1Args {
    #[command(flatten)]
    common: CommonArgs,
    /// Path to the ELF binary
    #[arg(long)]
    elf: PathBuf,
    /// Path to the SP1 verification key (vk)
    #[arg(long)]
    vk: PathBuf,
    /// zkVM identifier
    #[arg(long, default_value = "sp1")]
    zkvm: String,
}

#[derive(Debug, Parser)]
struct Risc0Args {
    #[command(flatten)]
    common: CommonArgs,
    /// Path to the RISC0 image
    #[arg(long)]
    img: PathBuf,
    /// Program id for the RISC0 image
    #[arg(long)]
    program_id: String,
    /// zkVM identifier
    #[arg(long, default_value = "risc0")]
    zkvm: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let response = match args.command {
        Command::Sp1(args) => {
            let program_id = program_id_hex_from_file(&args.vk)?;
            upload(UploadRequest {
                server_url: &args.common.server_url,
                api_key: &args.common.api_key,
                contract: &args.common.contract,
                program_id: &program_id,
                binary_path: &args.elf,
                toolchain: &args.common.toolchain,
                commit: &args.common.commit,
                zkvm: &args.zkvm,
            })
            .await?
        }
        Command::Risc0(args) => {
            upload(UploadRequest {
                server_url: &args.common.server_url,
                api_key: &args.common.api_key,
                contract: &args.common.contract,
                program_id: &args.program_id,
                binary_path: &args.img,
                toolchain: &args.common.toolchain,
                commit: &args.common.commit,
                zkvm: &args.zkvm,
            })
            .await?
        }
    };

    println!("Uploaded program_id: {}", response.program_id);
    println!("Response: {}", response.body);

    Ok(())
}
