//! `agents.sock`: how agents other than Claude reach named rooms in std mode.
//!
//! Plain HTTP/1.1 over a Unix socket, so a bot needs nothing but
//! `curl --unix-socket`. The socket is this user's alone, three ways over,
//! exactly like the daemon's own socket (`room::socket`): a 0700 directory,
//! a 0600 socket file, and a peer-uid check on every accepted connection.
//!
//! Identity is never claimed. Each `[[agent]]` gets a token file the daemon
//! writes ([`tokens`]); a request's `Authorization: Bearer` token is looked up
//! in constant time, and the agent it names is the author the router stamps on
//! every post ([`seat`]). No request field can name an author: the post body
//! is `{"body": "..."}` (and an optional `kind`), with unknown fields refused.
//!
//! The API, all JSON:
//!
//! - `GET /v1/whoami`: this agent's name and rooms.
//! - `GET /v1/rooms`: the rooms it may use, with their members.
//! - `POST /v1/rooms/{name}/messages`: post, `201` with the router's `seq`.
//! - `GET /v1/rooms/{name}/messages?after=<seq>`: stored history, a page.
//! - `GET /v1/rooms/{name}/stream`: new messages as NDJSON, one per line, for
//!   as long as the connection stays open. The bot pulls; the daemon never
//!   calls out.
//!
//! Errors are `{"error": "<word>"}`: `401` unknown token, `403` a room this
//! agent may not use (one answer whether or not the room exists, rule 25),
//! `413` over the body cap, `429` the router's post rate, token allowance or
//! turn cap, `400` a malformed request, `503` chat off or history
//! unavailable.
//!
//! Every post attempt that names a room is written to the audit ledger with
//! the agent's name, never its body.

pub mod tokens;

#[cfg(feature = "std-router")]
pub mod link;
#[cfg(feature = "std-router")]
pub mod seat;
