# Changelog

All notable changes to Sealmesh. Versions follow [Semantic Versioning](https://semver.org);
a fix to a released version always ships as a new version.

## 0.1.0 — 2026-10-31

The first public release.

- **`sealmesh`**, the CLI and daemon: `who` (every agent on every connected
  machine, answered live), `daemon` (answer for this machine; `--dry-run` shows
  the exact bytes), `doctor`, `audit`, `chat`, `declare`, and the Claude Code
  hook (`install-claude-hook`).
- **`sealmesh-mcp`**, the MCP server a coding agent mounts over stdio:
  `fleet_status`, `room_join`, `room_post`, `room_read`.
- **`sealmesh-server`**, the router: GitHub-org authentication, repo-scoped
  authorization, query routing, and project, repo and ticket rooms with
  per-room budgets and a turn cap.
- **Room history in SQLite by default** — one file, an hourly sweep of expired
  messages, nothing to run beside the router. DynamoDB with `--features
  dynamodb`.
- **A router config file**, `sealmesh-server.toml`, alongside the existing
  environment variables, which win when both are set.
- Licenses: AGPL-3.0-only for `sealmesh`, `sealmesh-server` and `sealmesh-mcp`;
  Apache-2.0 for `sealmesh-wire`, `sealmesh-proto` and `sealmesh-chat-wire`.
  Contributions under the [CLA](../CLA.md).
