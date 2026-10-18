//! The MCP server a coding agent mounts to reach sealmesh.
//!
//! Claude Code and Codex both speak MCP over stdio, so one binary serves both:
//! `claude mcp add sealmesh -- sealmesh-mcp` or
//! `codex mcp add sealmesh -- sealmesh-mcp`.
//!
//! Four properties this crate holds, each for a reason stated where it lives:
//!
//! - **No tool may take another agent's id as a target (Q5).** Checked on the
//!   tool signatures by [`tools::check`]; see [`tools`].
//! - **One credential path.** A tool reaches the router only through
//!   [`router::RouterAccess`], which reuses the CLI's `gh` credential path and
//!   holds no token of its own; see [`router`].
//! - **Agent state only through the router; never daemon code.** This crate
//!   depends on the CLI library for its credential path, its reader query
//!   (`who::ask`) and the local socket client (`room::connect`), and nothing
//!   else: it may not call `sealmesh::collect`, `daemon`, `hook` or
//!   `install_hook`. A tool that needs agent activity asks the router, so the
//!   read is narrowed by the router's repo-access gate and lands in the owning
//!   machine's `sealmesh audit` ledger (rule 16). Running this machine's
//!   collector directly would skip both. Talking to the *running* daemon over
//!   its local socket is not calling daemon code, and it is the only way a
//!   tool reaches a room: the daemon holds this machine's one router
//!   connection that may join rooms, and its token never crosses the socket.
//!   The room tools read the caller's own checkout with `gitref::read`, and
//!   `room_post` reads one file, its parent Claude Code session's, for a drain
//!   key ([`session`]). Neither is the collector, and neither reads anyone
//!   else's activity.
//! - **No redefined type.** Anything that crosses the wire comes from
//!   [`wire`] or `sealmesh_proto`, so producer and consumer cannot drift.

pub mod router;
pub mod session;
pub mod tools;

pub use sealmesh_wire as wire;

use anyhow::{Context, Result};
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt as _,
    model::{
        CacheScope, CallToolRequestParams, CallToolResponse, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
    },
    service::{QuitReason, RequestContext},
    transport::IntoTransport,
};

use crate::router::RouterAccess;

/// The name this server gives in the `initialize` handshake.
pub const SERVER_NAME: &str = "sealmesh-mcp";

const INSTRUCTIONS: &str = "sealmesh shows which coding agents are running across the team, \
     and lets an agent talk in the chat rooms of the checkout it is working in. No tool here \
     takes another agent as its target: an agent may change its own plan, never another's.";

#[derive(Debug, Clone)]
pub struct SealmeshMcp {
    router: RouterAccess,
}

impl SealmeshMcp {
    pub fn new(config: Option<std::path::PathBuf>) -> Self {
        Self {
            router: RouterAccess::new(config),
        }
    }
}

impl ServerHandler for SealmeshMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(SERVER_NAME, env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        // Protocol 2026-07-28 requires `ttlMs` and `cacheScope` on every list
        // result, and Claude Code rejects a tool list without them, so no tool
        // loads. Zero and private match what rmcp's own discover answer sends.
        Ok(ListToolsResult::with_all_items(tools::registry())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        tools::call(&self.router, request).await
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::registry().into_iter().find(|tool| tool.name == name)
    }
}

/// Serve one MCP session on `transport` until the client goes away.
///
/// For the binary that transport is stdio, where the harness closing stdin is
/// the normal end of a session and returns `Ok`.
pub async fn serve<T, E, A>(server: SealmeshMcp, transport: T) -> Result<()>
where
    T: IntoTransport<RoleServer, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let running = server
        .serve(transport)
        .await
        .context("completing the MCP initialize handshake")?;
    match running.waiting().await.context("serving the MCP session")? {
        // `Closed` is the harness closing stdin, the normal end of a session.
        // `Cancelled` only follows a cancellation this server never issues.
        QuitReason::Closed | QuitReason::Cancelled => Ok(()),
        QuitReason::JoinError(error) => {
            Err(anyhow::Error::new(error).context("the MCP session task failed"))
        }
        // `QuitReason` is non-exhaustive. An end this build cannot name is not
        // evidence the session ended cleanly, so it is not reported as one.
        _ => Err(anyhow::anyhow!(
            "the MCP session ended for a reason this build does not recognise"
        )),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::time::Duration;

    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, DuplexStream, Lines},
        io::{ReadHalf, WriteHalf},
        task::JoinHandle,
    };

    use super::*;

    const WAIT: Duration = Duration::from_secs(10);

    /// The client end of a session, speaking raw newline-delimited JSON-RPC —
    /// the bytes a harness writes to the binary's stdin, not the SDK's own
    /// client, so this tests what Claude Code and Codex will actually send.
    struct Harness {
        lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
        writer: WriteHalf<DuplexStream>,
        server: JoinHandle<Result<()>>,
    }

    impl Harness {
        /// A session whose config does not exist, so no test here can reach a
        /// real router with a real credential, whatever this machine has.
        fn start() -> Self {
            Self::start_with(SealmeshMcp::new(Some(std::path::PathBuf::from(
                "/nonexistent/sealmesh-mcp-test/config.toml",
            ))))
        }

        fn start_with(server: SealmeshMcp) -> Self {
            let (server_io, client_io) = tokio::io::duplex(64 * 1024);
            let server = tokio::spawn(serve(server, server_io));
            let (reader, writer) = tokio::io::split(client_io);
            Self {
                lines: BufReader::new(reader).lines(),
                writer,
                server,
            }
        }

        async fn send(&mut self, message: Value) {
            let mut line = serde_json::to_vec(&message).unwrap();
            line.push(b'\n');
            self.writer.write_all(&line).await.unwrap();
        }

        /// The response to request `id`, skipping any notification before it.
        async fn response(&mut self, id: u64) -> Value {
            loop {
                let line = tokio::time::timeout(WAIT, self.lines.next_line())
                    .await
                    .expect("the server did not answer in time")
                    .unwrap()
                    .expect("the server closed stdout before answering");
                let message: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(message["jsonrpc"], "2.0", "{message}");
                if message.get("id") == Some(&json!(id)) {
                    return message;
                }
            }
        }

        async fn initialize(&mut self, client: &str, version: &str, capabilities: Value) -> Value {
            self.send(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": version,
                    "capabilities": capabilities,
                    "clientInfo": { "name": client, "version": "0.0.0" }
                }
            }))
            .await;
            let response = self.response(1).await;
            self.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
                .await;
            response
        }
    }

    #[tokio::test]
    async fn initialize_completes_for_both_harness_shapes_and_exposes_the_inventory() {
        // The client names, protocol versions and capability shapes Claude
        // Code and Codex open with, plus the oldest and newest versions the
        // spec defines, so a harness on either side of an upgrade still mounts.
        for (client, version, capabilities) in [
            ("claude-code", "2025-06-18", json!({ "roots": {} })),
            (
                "codex-mcp-client",
                "2025-06-18",
                json!({ "elicitation": {} }),
            ),
            ("oldest", "2024-11-05", json!({})),
            ("newest", "2025-11-25", json!({})),
        ] {
            let mut harness = Harness::start();
            let init = harness.initialize(client, version, capabilities).await;

            assert!(init.get("error").is_none(), "{client}: {init}");
            let result = &init["result"];
            assert_eq!(result["protocolVersion"], version, "{client}: {init}");
            assert_eq!(result["serverInfo"]["name"], SERVER_NAME, "{client}");
            assert_eq!(
                result["serverInfo"]["version"],
                env!("CARGO_PKG_VERSION"),
                "{client}"
            );
            assert!(
                result["capabilities"]["tools"].is_object(),
                "{client}: {init}"
            );
            for absent in ["resources", "prompts", "logging", "completions"] {
                assert!(
                    result["capabilities"].get(absent).is_none(),
                    "{client}: offered {absent} it does not serve: {init}"
                );
            }

            harness
                .send(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }))
                .await;
            let listed = harness.response(2).await;
            let names: Vec<&str> = listed["result"]["tools"]
                .as_array()
                .unwrap_or_else(|| panic!("{client}: {listed}"))
                .iter()
                .map(|tool| tool["name"].as_str().unwrap())
                .collect();
            assert_eq!(names, tools::TOOL_INVENTORY, "{client}: {listed}");
        }
    }

    #[tokio::test]
    async fn a_discover_opened_session_lists_tools_with_the_cache_fields_2026_07_28_requires() {
        // Claude Code 2.1.285 opens with `server/discover` at 2026-07-28 and
        // then validates every `tools/list` answer against that revision's
        // schema. Without `ttlMs` and `cacheScope` it rejects the list and no
        // sealmesh tool loads at all.
        let meta = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": { "name": "claude-code", "version": "2.1.285" },
            "io.modelcontextprotocol/clientCapabilities": { "roots": { "listChanged": true } }
        });
        let mut harness = Harness::start();
        harness
            .send(json!({
                "jsonrpc": "2.0", "id": 1, "method": "server/discover",
                "params": { "_meta": meta }
            }))
            .await;
        let discovered = harness.response(1).await;
        assert!(discovered.get("error").is_none(), "{discovered}");

        harness
            .send(json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/list",
                "params": { "_meta": meta }
            }))
            .await;
        let listed = harness.response(2).await;
        let result = &listed["result"];
        assert!(result["ttlMs"].is_u64(), "{listed}");
        assert!(
            matches!(result["cacheScope"].as_str(), Some("public" | "private")),
            "{listed}"
        );
        let names: Vec<&str> = result["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("{listed}"))
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, tools::TOOL_INVENTORY, "{listed}");
    }

    async fn call(harness: &mut Harness, id: u64, name: &str, arguments: Value) -> Value {
        harness
            .send(json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": { "name": name, "arguments": arguments }
            }))
            .await;
        harness.response(id).await
    }

    #[tokio::test]
    async fn a_tool_name_not_listed_is_refused_at_call_time() {
        // The dispatch reads the same table as the listing, so a handler for a
        // name the client was never shown has nowhere to live. This holds that
        // from the outside: near-misses of every listed name, and names a
        // handler bypassing the table would plausibly use.
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;
        let mut names: Vec<String> = ["steer_agent", "send_message", "update_plan", ""]
            .map(str::to_owned)
            .into();
        for listed in tools::TOOL_INVENTORY {
            names.push(listed.to_uppercase());
            names.push(format!("{listed} "));
            names.push(listed.replace('_', "-"));
            names.push(format!("{listed}_v2"));
        }
        for (id, name) in (2..).zip(names) {
            let called = call(&mut harness, id, &name, json!({})).await;
            assert!(called.get("result").is_none(), "{name:?}: {called}");
            assert_eq!(
                called["error"]["message"], "no such tool",
                "{name:?}: {called}"
            );
        }
    }

    #[tokio::test]
    async fn fleet_status_refuses_an_argument_naming_an_agent_before_it_runs() {
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;
        let called = call(
            &mut harness,
            2,
            "fleet_status",
            json!({ "agent_id": "someone-else" }),
        )
        .await;
        assert!(called.get("result").is_none(), "{called}");
        let message = called["error"]["message"].as_str().unwrap();
        assert!(!message.contains("agent_id"), "{called}");
        assert!(!message.contains("someone-else"), "{called}");
    }

    #[tokio::test]
    async fn fleet_status_without_a_usable_config_is_a_structured_error_not_prose() {
        let mut harness = Harness::start();
        harness
            .initialize("codex-mcp-client", "2025-06-18", json!({}))
            .await;
        let called = call(&mut harness, 2, "fleet_status", json!({})).await;
        let result = &called["result"];
        assert_eq!(result["isError"], json!(true), "{called}");
        assert_eq!(
            result["structuredContent"],
            json!({ "error": "config_unavailable" }),
            "{called}"
        );
        assert!(
            !called.to_string().contains("nonexistent"),
            "the config path leaked into the result: {called}"
        );
    }

    #[tokio::test]
    async fn the_room_tools_refuse_a_consumer_key_or_a_target_before_they_run() {
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;
        let valid = [
            ("room_join", json!({})),
            (
                "room_post",
                json!({ "room": "repo", "kind": "claim", "body": "x" }),
            ),
            ("room_read", json!({ "room": "repo" })),
        ];
        let mut id = 2;
        for (name, arguments) in valid {
            for (extra, value) in [
                ("consumer", json!("0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11")),
                ("agent_id", json!("someone-else")),
                (
                    "room_id",
                    json!({ "project": "example-repo", "scope": "project" }),
                ),
            ] {
                let mut arguments = arguments.clone();
                arguments[extra] = value;
                let called = call(&mut harness, id, name, arguments).await;
                id += 1;
                assert!(called.get("result").is_none(), "{name} {extra}: {called}");
                let message = called["error"]["message"].as_str().unwrap();
                assert!(!message.contains(extra), "{called}");
            }
        }
        // A free-form room where a scope word belongs is refused as well.
        let called = call(
            &mut harness,
            id,
            "room_post",
            json!({ "room": { "project": "example-repo", "scope": "project" }, "kind": "claim", "body": "x" }),
        )
        .await;
        assert!(called.get("result").is_none(), "{called}");
    }

    #[tokio::test]
    async fn the_room_tools_without_a_usable_config_are_structured_errors() {
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;
        for (id, (name, arguments)) in (2..).zip([
            ("room_join", json!({ "project": true })),
            (
                "room_post",
                json!({ "room": "ticket", "kind": "ack", "body": "ok" }),
            ),
            ("room_read", json!({ "room": "ticket", "after": 3 })),
        ]) {
            let called = call(&mut harness, id, name, arguments).await;
            let result = &called["result"];
            assert_eq!(result["isError"], json!(true), "{name}: {called}");
            assert_eq!(
                result["structuredContent"],
                json!({ "error": "config_unavailable" }),
                "{name}: {called}"
            );
        }
    }

    #[tokio::test]
    async fn calling_a_tool_that_does_not_exist_is_an_error_and_the_session_survives() {
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;

        harness
            .send(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": { "name": "steer_agent_ghp_secretlooking", "arguments": {} }
            }))
            .await;
        let called = harness.response(2).await;
        assert!(called.get("result").is_none(), "{called}");
        let message = called["error"]["message"].as_str().unwrap();
        assert!(
            !message.contains("steer_agent_ghp_secretlooking"),
            "the refusal echoed the caller's input back: {called}"
        );

        harness
            .send(json!({ "jsonrpc": "2.0", "id": 3, "method": "ping" }))
            .await;
        let pong = harness.response(3).await;
        assert_eq!(pong["result"], json!({}), "{pong}");
    }

    #[tokio::test]
    async fn the_harness_closing_stdin_ends_the_session_cleanly() {
        let mut harness = Harness::start();
        harness
            .initialize("codex-mcp-client", "2025-06-18", json!({}))
            .await;

        let Harness {
            lines,
            writer,
            server,
        } = harness;
        drop(writer);
        drop(lines);

        let outcome = tokio::time::timeout(WAIT, server)
            .await
            .expect("the server kept running after its client went away")
            .unwrap();
        outcome.unwrap();
    }

    #[tokio::test]
    async fn the_tools_a_client_sees_pass_the_seam_and_match_the_inventory() {
        // The seam is checked on what reaches a client, not only on
        // `tools::registry`, so a tool served any other way fails here.
        let mut harness = Harness::start();
        harness
            .initialize("claude-code", "2025-06-18", json!({}))
            .await;
        harness
            .send(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }))
            .await;
        let listed = harness.response(2).await;

        let seen: Vec<Tool> = serde_json::from_value(listed["result"]["tools"].clone())
            .unwrap_or_else(|error| panic!("tools/list was not a tool list ({error}): {listed}"));
        tools::check(&seen).unwrap();
        let names: Vec<&str> = seen.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, tools::TOOL_INVENTORY);
    }

    #[test]
    fn get_tool_agrees_with_the_registry() {
        let server = SealmeshMcp::new(Some(std::path::PathBuf::from(
            "/nonexistent/sealmesh-mcp-test/config.toml",
        )));
        for name in tools::TOOL_INVENTORY {
            assert!(server.get_tool(name).is_some(), "{name}");
        }
        assert!(server.get_tool("anything").is_none());
    }
}
