use std::path::PathBuf;

use clap::Parser;

/// Serve sealmesh to a coding agent over MCP on stdio.
///
/// Stdout carries the protocol and nothing else, so this binary never prints
/// to it once serving. Mount it with `claude mcp add` or `codex mcp add`.
#[derive(Debug, Parser)]
#[command(name = "sealmesh-mcp", version, about)]
struct Cli {
    /// Path to the sealmesh config. Read only when a tool reaches the
    /// router, never at startup, so the handshake works on a machine with none.
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Stdout is the MCP transport. rmcp's `tracing::debug!` calls format raw
    // input lines, so no tracing subscriber is installed here, and any future
    // one must write to stderr. One pointed at stdout corrupts the protocol and
    // echoes raw client input into the stream.
    let server = sealmesh_mcp::SealmeshMcp::new(cli.config);
    sealmesh_mcp::serve(server, rmcp::transport::stdio()).await
}
