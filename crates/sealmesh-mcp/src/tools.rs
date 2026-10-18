//! The tool registry, and the seam every tool passes through to get on it.
//!
//! # No tool may take another agent's id as a target (Q5)
//!
//! An agent may change its own plan. It may never start, stop, steer, or write
//! into another agent's workspace. That is a property of the tool *signatures*,
//! so it is checked on the signatures — every tool's input schema — rather than
//! left to a reviewer reading each handler.
//!
//! [`check`] enforces it on every schema, and each part fails closed:
//!
//! 1. **A schema must prove what it admits.** A subschema passes only if its
//!    `type` provably excludes `"object"` — for an array, its `items` must pass
//!    this same rule — or it is a closed object (`additionalProperties: false`)
//!    whose property subschemas pass this same rule. `true`, `{}`, an
//!    annotation-only schema, `not`, `if`/`then`/`else`, `patternProperties` and
//!    any keyword the seam does not understand are refused. `anyOf`, `oneOf`
//!    and `allOf` pass only if every branch does. `$ref` is followed only to a
//!    local `#/$defs/…` or `#/definitions/…` entry, which must pass too, and a
//!    `$ref` cycle passes only when a `properties` or `items` level sits
//!    between its references. `const` and `enum` may hold only scalars.
//!    `$schema` may appear only at the root and only as the 2020-12 metaschema,
//!    since a custom metaschema can switch vocabularies off and make
//!    `additionalProperties` inert; `$id` may appear only at the root, since
//!    one below it changes what a `$ref` resolves to.
//! 2. **An allowlist of input field names.** Every property any input schema
//!    declares, at any depth including `$defs`, must be in
//!    [`ALLOWED_INPUT_FIELDS`]. A field nobody listed is refused. This is the
//!    primary control.
//! 3. **A refusal of target-shaped names.** A field whose name reads as
//!    identifying an agent, a job, a task, a run, a session, a thread, a host, a
//!    process, a workspace, a user, a login, an assignee, a member, an owner, a
//!    recipient or a target is refused *even if it is on the allowlist*: a
//!    second net under it. A name that is not plain ASCII, or that has no
//!    letter or digit in it, is refused outright rather than matched, because
//!    `аgent_id` with a Cyrillic `а` reads as `agent_id` to a person and not to
//!    a substring match.
//!
//! # A tool's schema is its handler's type
//!
//! A tool is an [`McpTool`] implementation, and nothing else can put one on
//! the registry. Its input schema is *derived* from its `Args` type, so the
//! schema a client sees and the type the handler parses cannot drift: a field a
//! handler would accept is a field the seam has checked. Its handler is reached
//! only through the same table, and receives `Args` already parsed by
//! [`parse_arguments`] — it never sees the raw arguments at all.
//!
//! Arguments are held to the signature at call time in two layers:
//!
//! - [`call`] refuses any **top-level** argument the schema does not declare,
//!   before dispatch.
//! - The `Args` type is `#[serde(deny_unknown_fields)]` at **every** level, so
//!   a nested field the signature never named is refused too. CI proves that
//!   for every registered tool from the registry itself: each tool supplies a
//!   complete sample of its arguments, the test checks the sample reaches every
//!   declared field and array, and then probes an undeclared field into every
//!   object in it. A tool without a sample does not compile under test.
//! - The names the `Args` type *parses* are the names its schema *shows*. CI
//!   reads the field and variant names serde's derive accepts at every level
//!   — aliases included, however they were attached — and requires them to
//!   equal the schema's property names and enum words, and each to pass the
//!   target-shaped check (`tools/trace.rs`). So a `skip` in a `schemars`
//!   attribute, a `serde(alias)` behind `cfg_attr` or a macro, or a
//!   `schemars(rename)` cannot hide an input from the seam.
//!
//! What a client actually sees is checked as well: a handshake test runs
//! [`check`] on the parsed `tools/list` response and compares its names to
//! [`TOOL_INVENTORY`], so a tool that reaches a client without going through
//! [`registry`] fails CI.
//!
//! # Output
//!
//! A tool returns structured content only, serialised from its `Output` or its
//! `Failure` type, and never free text. Both are built from `sealmesh_wire`,
//! `sealmesh_proto` and `sealmesh_chat_wire` types and closed enums. rmcp
//! sends a structured result twice — as `structuredContent`, and again as a
//! text block holding the same JSON inside a JSON string, escaped a second
//! time — so the budget is measured on the whole serialised result, not on
//! either copy: see [`fits`]. A result over [`MAX_OUTPUT_BYTES`] is answered
//! with the tool's own `output_too_large` failure rather than sent.
//!
//! # Adding a tool
//!
//! 1. Write a unit struct in `tools/<name>.rs` implementing [`McpTool`]. Its
//!    `Args` derives `Deserialize` and `JsonSchema` (with
//!    `#[schemars(crate = "rmcp::schemars")]`) and is
//!    `#[serde(deny_unknown_fields)]` at every level. `complete_sample` returns
//!    arguments with every optional field present and every array non-empty.
//! 2. Add `entry::<YourTool>()` to [`ENTRIES`]. There is no other dispatch:
//!    a tool that is not in that table cannot be listed or called.
//! 3. Add its name to [`TOOL_INVENTORY`].
//! 4. Add each input field name to [`ALLOWED_INPUT_FIELDS`].
//! 5. Say in the PR body what each field can hold and why none of them can name
//!    another agent.
//! 6. If it reaches agent state, go through [`crate::router::RouterAccess`] —
//!    never the CLI's `collect`, `daemon`, `hook` or `install_hook`. If it
//!    reaches a room, go through the running daemon's local socket with
//!    `sealmesh::room::connect`; that talks to the daemon, it does not run
//!    daemon code.
//! 7. Declare `output_too_large` on its `Failure` type and return it from
//!    [`McpTool::output_too_large`], and name it in the description: a result
//!    over the budget is answered with that category, and it is the tool's own.
//! 8. Keep what the handler parses equal to what the schema shows. The
//!    registry's type probe enforces it on every tool, from serde's own
//!    field and variant lists, however an attribute was attached. As a cheap
//!    second net, a source scan also refuses `alias`, `flatten`, `with` and
//!    `deserialize_with` in a `serde` attribute; `with`, `schema_with`,
//!    `skip`, `rename`, `transform` and `extend` in a `schemars` one; either
//!    behind `cfg_attr`; and a hand-written `impl JsonSchema`.
//! 9. If it reaches something another party pays for, give it a process-wide
//!    [`cooldown::Cooldown`] and a `rate_limited` failure.
//! 10. Mind the shapes schemars emits that the seam refuses, which fail CI rather
//!     than ship: a signed `NonZero` integer (`NonZeroI32`) emits
//!     `not: {const: 0}` — use `NonZeroU32`, which emits `minimum: 1`, or check
//!     the value in the handler. An `Args` type that contains itself directly
//!     emits `$ref: "#"`, which is refused — only `#/$defs/…` is followed — so
//!     put the recursion in a named inner type. A `serde_json::Value` field emits
//!     `true`, which admits any object, and is refused.

pub(crate) mod cooldown;
pub mod fleet_status;
pub mod room_join;
pub mod room_post;
pub mod room_read;
pub mod rooms;
#[cfg(test)]
mod seam_fixtures;
#[cfg(test)]
mod trace;

use std::{collections::BTreeSet, future::Future, io, pin::Pin, sync::Arc};

use rmcp::{
    ErrorData,
    handler::server::common::schema_for_input,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, Tool, ToolAnnotations,
    },
    schemars::JsonSchema,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::router::RouterAccess;

/// Every tool this server exposes, by name, in registration order.
pub const TOOL_INVENTORY: &[&str] = &["fleet_status", "room_join", "room_post", "room_read"];

/// Every property name a tool's input schema may declare.
///
/// Adding a name is a mandate, not a field: step 5 of "Adding a tool".
/// Each name's justification is in the tool that takes it: `project`
/// ([`room_join`]), `room`, `kind` and `body` ([`room_post`]), `room` and
/// `after` ([`room_read`]). None names an agent, a host, a session or a room
/// owner: `room` is a scope word resolved against the caller's own checkout.
pub const ALLOWED_INPUT_FIELDS: &[&str] = &["project", "room", "kind", "body", "after"];

/// The largest tool result, in bytes of JSON, this server will send. A result
/// is assembled from answers that arrived from other machines, so its size is
/// not this server's to assume (rule 42).
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

/// Fragments that make a field name read as naming someone else's agent. A
/// name is normalised — lowercased, everything but ASCII letters and digits
/// removed — before matching, so `agent_id`, `agentId`, `target-session` and
/// `a g e n t` are all caught.
const TARGET_SHAPED: &[&str] = &[
    "agent",
    "job",
    "session",
    "host",
    "machine",
    "target",
    "pid",
    "process",
    "worker",
    "workspace",
    "worktree",
    "peer",
    "task",
    "run",
    "recipient",
    "user",
    "login",
    "assignee",
    "owner",
    "member",
    "thread",
    // A drain cursor's owner. Naming one would let an agent consume another
    // agent's room messages; the key is derived, never taken.
    "consumer",
    "sender",
];

/// Normalised names refused whole rather than as fragments: short enough that
/// matching them as substrings would refuse ordinary words.
const TARGET_SHAPED_WHOLE: &[&str] = &["id", "ids"];

/// The only metaschema a root may declare.
const METASCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

/// How deep a schema may nest before the seam refuses it rather than recurse.
const MAX_SCHEMA_DEPTH: usize = 32;

/// Every keyword the seam understands. Anything else is refused: a keyword the
/// seam cannot reason about is one it cannot prove closed.
const UNDERSTOOD_KEYWORDS: &[&str] = &[
    // Structure.
    "type",
    "properties",
    "additionalProperties",
    "required",
    "items",
    "prefixItems",
    "anyOf",
    "oneOf",
    "allOf",
    "$ref",
    "$defs",
    "definitions",
    "const",
    "enum",
    // Constraints that only narrow what is admitted.
    "minItems",
    "maxItems",
    "uniqueItems",
    "minProperties",
    "maxProperties",
    "minLength",
    "maxLength",
    "pattern",
    "format",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    // Root-only identifiers, checked where they appear.
    "$schema",
    "$id",
    // Annotations.
    "$comment",
    "title",
    "description",
    "default",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
    "nullable",
];

const JSON_TYPES: &[&str] = &[
    "object", "array", "string", "number", "integer", "boolean", "null",
];

/// One tool: its name, what it takes, what it returns, and what it does.
///
/// Every part a client sees is derived from this, so a tool's listing and its
/// handler are one definition rather than two that have to agree.
pub trait McpTool: 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    /// The arguments. The input schema is generated from this type, and the
    /// handler receives it already parsed through [`parse_arguments`].
    type Args: DeserializeOwned + JsonSchema + Send + 'static;
    /// What a successful call returns, as structured content.
    type Output: Serialize + Send + 'static;
    /// What a failed call returns, as structured content with `isError`. A
    /// closed enum: a failure is a category, never a message.
    type Failure: Serialize + Send + 'static;

    fn annotations() -> ToolAnnotations;

    /// This tool's own `output_too_large` failure, answered when a result does
    /// not [`fits`] in [`MAX_OUTPUT_BYTES`]. It must serialise as
    /// `{"error":"output_too_large"}`, which a test checks for every tool.
    fn output_too_large() -> Self::Failure;

    fn run(
        router: RouterAccess,
        args: Self::Args,
    ) -> impl Future<Output = Result<Self::Output, Self::Failure>> + Send;

    /// Arguments with every optional field present and every array
    /// non-empty, which the registry test probes at every level.
    #[cfg(test)]
    fn complete_sample() -> Value;
}

type Handled = Pin<Box<dyn Future<Output = Result<CallToolResponse, ErrorData>> + Send>>;

/// A registered tool, erased to what the registry and dispatch need.
pub struct Entry {
    name: &'static str,
    tool: fn() -> Tool,
    handle: fn(RouterAccess, Option<Map<String, Value>>) -> Handled,
    #[cfg(test)]
    probe: fn() -> Result<(), String>,
    #[cfg(test)]
    too_large: fn() -> Value,
}

pub const fn entry<T: McpTool>() -> Entry {
    Entry {
        name: T::NAME,
        tool: tool_for::<T>,
        handle: handle_for::<T>,
        #[cfg(test)]
        probe: tests::probe_for::<T>,
        #[cfg(test)]
        too_large: tests::too_large_for::<T>,
    }
}

/// Every tool, in [`TOOL_INVENTORY`] order. The listing and the dispatch are
/// both read from here, so neither can hold a tool the other does not.
pub const ENTRIES: &[Entry] = &[
    entry::<fleet_status::FleetStatusTool>(),
    entry::<room_join::RoomJoinTool>(),
    entry::<room_post::RoomPostTool>(),
    entry::<room_read::RoomReadTool>(),
];

/// Every tool this server exposes.
pub fn registry() -> Vec<Tool> {
    ENTRIES.iter().map(|entry| (entry.tool)()).collect()
}

fn tool_for<T: McpTool>() -> Tool {
    // A type schemars cannot express as an object gets an empty schema, which
    // `check` refuses: it fails CI rather than serving an unchecked signature.
    let input_schema =
        schema_for_input::<T::Args>().unwrap_or_else(|_| Arc::new(JsonObject::new()));
    Tool::new(T::NAME, T::DESCRIPTION, input_schema).with_annotations(T::annotations())
}

fn handle_for<T: McpTool>(router: RouterAccess, arguments: Option<Map<String, Value>>) -> Handled {
    Box::pin(async move {
        let args = parse_arguments::<T::Args>(arguments)?;
        let encoded = match T::run(router, args).await {
            Ok(output) => encode(&output).map(CallToolResult::structured),
            Err(failure) => encode(&failure).map(CallToolResult::structured_error),
        };
        let result = match encoded {
            Ok(result) if fits(&result) => result,
            Ok(_) | Err(Unencodable::TooLarge) => match encode(&T::output_too_large()) {
                Ok(failure) => CallToolResult::structured_error(failure),
                Err(_) => {
                    return Err(ErrorData::internal_error(
                        "tool output could not be encoded",
                        None,
                    ));
                }
            },
            Err(Unencodable::Failed) => {
                return Err(ErrorData::internal_error(
                    "tool output could not be encoded",
                    None,
                ));
            }
        };
        Ok(CallToolResponse::from(result))
    })
}

enum Unencodable {
    TooLarge,
    Failed,
}

/// Whether `result`, serialised as it is sent, fits [`MAX_OUTPUT_BYTES`] —
/// measured while writing, so an oversized one stops there. This is the
/// measure that counts: the text copy of a structured result escapes its JSON
/// a second time, so a quote in a body costs two bytes in one copy and four
/// in the other.
pub fn fits(result: &CallToolResult) -> bool {
    let mut counted = ByteBudget {
        left: MAX_OUTPUT_BYTES,
        exceeded: false,
    };
    serde_json::to_writer(&mut counted, result).is_ok()
}

/// Serialise a tool's result, refusing it once it passes [`MAX_OUTPUT_BYTES`]
/// on its own — measured while writing, so an oversized one stops there
/// before the whole response is built and measured by [`fits`].
fn encode<T: Serialize>(value: &T) -> Result<Value, Unencodable> {
    let mut counted = ByteBudget {
        left: MAX_OUTPUT_BYTES,
        exceeded: false,
    };
    if serde_json::to_writer(&mut counted, value).is_err() {
        return Err(if counted.exceeded {
            Unencodable::TooLarge
        } else {
            Unencodable::Failed
        });
    }
    serde_json::to_value(value).map_err(|_| Unencodable::Failed)
}

struct ByteBudget {
    left: usize,
    exceeded: bool,
}

impl io::Write for ByteBudget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.left.checked_sub(buf.len()) {
            Some(left) => {
                self.left = left;
                Ok(buf.len())
            }
            None => {
                self.exceeded = true;
                Err(io::Error::other("over the output budget"))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Dispatch one `tools/call`.
///
/// A name not in [`ENTRIES`] is refused, and the refusal does not repeat the
/// name back (rule 14).
pub async fn call(
    router: &RouterAccess,
    request: CallToolRequestParams,
) -> Result<CallToolResponse, ErrorData> {
    let Some(entry) = ENTRIES.iter().find(|entry| request.name == entry.name) else {
        return Err(ErrorData::invalid_params("no such tool", None));
    };
    refuse_undeclared_arguments(&(entry.tool)(), request.arguments.as_ref())?;
    (entry.handle)(router.clone(), request.arguments).await
}

/// Parse a tool's arguments into its handler type. This is the only way a
/// handler reads them.
///
/// `T` must be `#[serde(deny_unknown_fields)]` at every level, or a nested field
/// the signature never declared would be accepted and silently dropped. Absent
/// arguments parse as `{}`. The refusal does not carry the parser's message,
/// which quotes the offending input (rule 14).
pub fn parse_arguments<T: DeserializeOwned>(
    arguments: Option<Map<String, Value>>,
) -> Result<T, ErrorData> {
    serde_json::from_value(Value::Object(arguments.unwrap_or_default()))
        .map_err(|_| ErrorData::invalid_params("arguments do not match this tool's input", None))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeamViolation {
    #[error("tool `{tool}` takes `{field}`, which reads as naming another agent")]
    TargetShaped { tool: String, field: String },
    #[error("tool `{tool}` takes a field whose name is not plain ASCII letters and digits")]
    IllegibleName { tool: String },
    #[error("tool `{tool}` takes `{field}`, which is not in ALLOWED_INPUT_FIELDS")]
    Unlisted { tool: String, field: String },
    #[error("tool `{tool}` has a schema that could admit an object it does not close")]
    OpenObject { tool: String },
    #[error("tool `{tool}` uses `{keyword}` in a way the seam cannot prove closed")]
    Unsupported { tool: String, keyword: String },
    #[error("tool `{tool}` has a `$ref` cycle with no property or item between its references")]
    UnguardedCycle { tool: String },
    #[error("tool `{tool}` nests its schema deeper than the seam checks")]
    TooDeep { tool: String },
    #[error("tool `{tool}` is registered twice")]
    Duplicate { tool: String },
}

/// Check every tool against the Q5 seam.
pub fn check(tools: &[Tool]) -> Result<(), SeamViolation> {
    check_with(tools, ALLOWED_INPUT_FIELDS)
}

fn check_with(tools: &[Tool], allowed: &[&str]) -> Result<(), SeamViolation> {
    let mut seen = BTreeSet::new();
    for tool in tools {
        let name = tool.name.to_string();
        if !seen.insert(name.clone()) {
            return Err(SeamViolation::Duplicate { tool: name });
        }
        let root: &Map<String, Value> = &tool.input_schema;
        // MCP requires a tool's input to be an object, so the root must say so
        // in exactly those words before anything else is considered.
        if root.get("type").and_then(Value::as_str) != Some("object") {
            return Err(SeamViolation::OpenObject { tool: name });
        }
        Seam {
            tool: &name,
            root,
            allowed,
            resolving: Vec::new(),
            guards: 0,
        }
        .object_schema(root, 0)?;
    }
    Ok(())
}

struct Seam<'a> {
    tool: &'a str,
    root: &'a Map<String, Value>,
    allowed: &'a [&'a str],
    /// `$ref` targets being checked right now, each with the number of
    /// `properties`/`items` levels on the path when it was entered.
    resolving: Vec<(&'a str, usize)>,
    /// `properties`/`items` levels on the path to the subschema being checked.
    /// A reference back to a definition already being checked is a recursive
    /// type only if this has grown since that definition was entered.
    guards: usize,
}

impl<'a> Seam<'a> {
    fn open(&self) -> SeamViolation {
        SeamViolation::OpenObject {
            tool: self.tool.to_owned(),
        }
    }

    fn unsupported(&self, keyword: &str) -> SeamViolation {
        SeamViolation::Unsupported {
            tool: self.tool.to_owned(),
            keyword: keyword.to_owned(),
        }
    }

    fn schema(&mut self, schema: &'a Value, depth: usize) -> Result<(), SeamViolation> {
        match schema {
            // `false` admits nothing.
            Value::Bool(false) => Ok(()),
            Value::Object(map) => self.object_schema(map, depth),
            // `true` admits everything, and anything else is not a schema.
            Value::Bool(true)
            | Value::Null
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => Err(self.open()),
        }
    }

    /// Check a subschema one `properties` or `items` level down.
    fn guarded(&mut self, schema: &'a Value, depth: usize) -> Result<(), SeamViolation> {
        self.guards = self.guards.saturating_add(1);
        let result = self.schema(schema, depth);
        self.guards = self.guards.saturating_sub(1);
        result
    }

    fn object_schema(
        &mut self,
        map: &'a Map<String, Value>,
        depth: usize,
    ) -> Result<(), SeamViolation> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(SeamViolation::TooDeep {
                tool: self.tool.to_owned(),
            });
        }
        if let Some(keyword) = map
            .keys()
            .find(|keyword| !UNDERSTOOD_KEYWORDS.contains(&keyword.as_str()))
        {
            return Err(self.unsupported(keyword));
        }
        self.identifiers(map)?;
        let next = depth.saturating_add(1);

        // Every definition passes, whether or not anything refers to it.
        for key in ["$defs", "definitions"] {
            if let Some(defs) = map.get(key) {
                let Value::Object(defs) = defs else {
                    return Err(self.unsupported(key));
                };
                for definition in defs.values() {
                    self.schema(definition, next)?;
                }
            }
        }

        // Keys under `properties` are field names, not schema keywords: each is
        // checked as a field, and only its value is walked as a schema.
        if let Some(properties) = map.get("properties") {
            let Value::Object(properties) = properties else {
                return Err(self.unsupported("properties"));
            };
            for (field, subschema) in properties {
                check_field(self.tool, field, self.allowed)?;
                self.guarded(subschema, next)?;
            }
        }

        // Whether something other than `type` bounds what this schema admits.
        let mut bounded = false;

        for key in ["anyOf", "oneOf", "allOf"] {
            if let Some(branches) = map.get(key) {
                let Value::Array(branches) = branches else {
                    return Err(self.unsupported(key));
                };
                if branches.is_empty() {
                    return Err(self.unsupported(key));
                }
                for branch in branches {
                    self.schema(branch, next)?;
                }
                bounded = true;
            }
        }

        for key in ["const", "enum"] {
            if let Some(value) = map.get(key) {
                let scalars_only = match value {
                    Value::Array(values) if key == "enum" => {
                        !values.is_empty() && values.iter().all(is_scalar)
                    }
                    value => key == "const" && is_scalar(value),
                };
                if !scalars_only {
                    return Err(self.unsupported(key));
                }
                bounded = true;
            }
        }

        if let Some(reference) = map.get("$ref") {
            self.reference(reference, next)?;
            bounded = true;
        }

        match declared_types(map) {
            // Untyped admits every type, objects included, unless something
            // above bounds it.
            Types::Absent if bounded => Ok(()),
            Types::Absent => Err(self.open()),
            Types::Malformed => Err(self.unsupported("type")),
            Types::Declared(types) => {
                if types.contains(&"object")
                    && map.get("additionalProperties") != Some(&Value::Bool(false))
                {
                    return Err(self.open());
                }
                if types.contains(&"array") {
                    self.array(map, next)?;
                }
                Ok(())
            }
        }
    }

    /// `$schema` and `$id` are root-only, and `$schema` names 2020-12.
    fn identifiers(&self, map: &Map<String, Value>) -> Result<(), SeamViolation> {
        let at_root = std::ptr::eq(map, self.root);
        if let Some(metaschema) = map.get("$schema")
            && !(at_root && metaschema.as_str() == Some(METASCHEMA_2020_12))
        {
            return Err(self.unsupported("$schema"));
        }
        if let Some(id) = map.get("$id")
            && !(at_root && id.is_string())
        {
            return Err(self.unsupported("$id"));
        }
        Ok(())
    }

    fn array(&mut self, map: &'a Map<String, Value>, depth: usize) -> Result<(), SeamViolation> {
        // Without `items`, an array admits any element, objects included.
        let Some(items) = map.get("items") else {
            return Err(self.open());
        };
        self.guarded(items, depth)?;
        if let Some(prefix) = map.get("prefixItems") {
            let Value::Array(prefix) = prefix else {
                return Err(self.unsupported("prefixItems"));
            };
            for item in prefix {
                self.guarded(item, depth)?;
            }
        }
        Ok(())
    }

    fn reference(&mut self, reference: &'a Value, depth: usize) -> Result<(), SeamViolation> {
        let Some(pointer) = reference.as_str() else {
            return Err(self.unsupported("$ref"));
        };
        let Some((defs_key, name)) = pointer
            .strip_prefix("#/$defs/")
            .map(|name| ("$defs", name))
            .or_else(|| {
                pointer
                    .strip_prefix("#/definitions/")
                    .map(|name| ("definitions", name))
            })
        else {
            return Err(self.unsupported("$ref"));
        };
        // A JSON Pointer escape or a deeper path is not a plain definition name.
        if name.is_empty() || name.contains(['/', '~']) {
            return Err(self.unsupported("$ref"));
        }
        // A definition already being checked above. It is a recursive type,
        // which ends when the instance does, only if a property or item sits
        // between the two references; otherwise it refers to itself without
        // consuming anything, and a validator following it never returns.
        if let Some(&(_, guards_then)) = self
            .resolving
            .iter()
            .find(|(resolving, _)| *resolving == pointer)
        {
            return if self.guards > guards_then {
                Ok(())
            } else {
                Err(SeamViolation::UnguardedCycle {
                    tool: self.tool.to_owned(),
                })
            };
        }
        let root = self.root;
        let Some(target) = root
            .get(defs_key)
            .and_then(Value::as_object)
            .and_then(|definitions| definitions.get(name))
        else {
            return Err(self.unsupported("$ref"));
        };
        self.resolving.push((pointer, self.guards));
        let result = self.schema(target, depth);
        self.resolving.pop();
        result
    }
}

enum Types<'a> {
    Absent,
    Declared(Vec<&'a str>),
    Malformed,
}

fn declared_types(map: &Map<String, Value>) -> Types<'_> {
    let names: Option<Vec<&str>> = match map.get("type") {
        None => return Types::Absent,
        Some(Value::String(kind)) => Some(vec![kind.as_str()]),
        Some(Value::Array(kinds)) => kinds.iter().map(Value::as_str).collect(),
        Some(_) => None,
    };
    match names {
        Some(names) if !names.is_empty() && names.iter().all(|name| JSON_TYPES.contains(name)) => {
            Types::Declared(names)
        }
        _ => Types::Malformed,
    }
}

fn is_scalar(value: &Value) -> bool {
    !matches!(value, Value::Object(_) | Value::Array(_))
}

fn check_field(tool: &str, field: &str, allowed: &[&str]) -> Result<(), SeamViolation> {
    if !is_legible(field) {
        return Err(SeamViolation::IllegibleName {
            tool: tool.to_owned(),
        });
    }
    if is_target_shaped(field) {
        return Err(SeamViolation::TargetShaped {
            tool: tool.to_owned(),
            field: field.to_owned(),
        });
    }
    if !allowed.contains(&field) {
        return Err(SeamViolation::Unlisted {
            tool: tool.to_owned(),
            field: field.to_owned(),
        });
    }
    Ok(())
}

/// Refuse any argument the tool's input schema does not declare at its top
/// level. Nested fields are refused by the handler type; see
/// [`parse_arguments`]. The refusal does not name the argument (rule 14).
fn refuse_undeclared_arguments(
    tool: &Tool,
    arguments: Option<&Map<String, Value>>,
) -> Result<(), ErrorData> {
    let declared = tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object);
    let undeclared = arguments
        .into_iter()
        .flat_map(Map::keys)
        .any(|key| !declared.is_some_and(|fields| fields.contains_key(key)));
    if undeclared {
        return Err(ErrorData::invalid_params(
            "argument not accepted by this tool",
            None,
        ));
    }
    Ok(())
}

/// A field name the target-shape match can read: printable ASCII with at least
/// one letter or digit.
fn is_legible(field: &str) -> bool {
    field.chars().all(|c| c.is_ascii_graphic() || c == ' ')
        && field.chars().any(|c| c.is_ascii_alphanumeric())
}

fn is_target_shaped(field: &str) -> bool {
    let normalised: String = field
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    TARGET_SHAPED_WHOLE.contains(&normalised.as_str())
        || TARGET_SHAPED.iter().any(|shape| normalised.contains(shape))
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use serde_json::json;

    use super::*;

    const PROBE: &str = "undeclared_probe";

    /// Every copy of `value` with [`PROBE`] added to exactly one object in it.
    fn with_probe_everywhere(value: &Value) -> Vec<Value> {
        let mut variants = Vec::new();
        match value {
            Value::Object(map) => {
                let mut here = map.clone();
                here.insert(PROBE.to_owned(), Value::Bool(true));
                variants.push(Value::Object(here));
                for (key, child) in map {
                    for probed in with_probe_everywhere(child) {
                        let mut copy = map.clone();
                        copy.insert(key.clone(), probed);
                        variants.push(Value::Object(copy));
                    }
                }
            }
            Value::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    for probed in with_probe_everywhere(child) {
                        let mut copy = items.clone();
                        if let Some(slot) = copy.get_mut(index) {
                            *slot = probed;
                        }
                        variants.push(Value::Array(copy));
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        variants
    }

    /// The first copy of `sample` with an undeclared field that `T` still
    /// accepts, or `None` if `T` refuses the probe at every level.
    pub(crate) fn undeclared_field_accepted<T: DeserializeOwned>(sample: &Value) -> Option<Value> {
        with_probe_everywhere(sample)
            .into_iter()
            .find(|probed| serde_json::from_value::<T>(probed.clone()).is_ok())
    }

    /// `sample` is valid, and `T` refuses an undeclared field at every level.
    pub(crate) fn assert_undeclared_fields_refused<T: DeserializeOwned>(sample: &Value) {
        assert!(
            serde_json::from_value::<T>(sample.clone()).is_ok(),
            "the sample must be valid arguments: {sample}"
        );
        if let Some(accepted) = undeclared_field_accepted::<T>(sample) {
            panic!("accepted an undeclared field: {accepted}");
        }
    }

    /// The nested-field probe for one registered tool, driven from its own
    /// `Args` type and sample. Every entry in [`ENTRIES`] carries this.
    pub(crate) fn probe_for<T: McpTool>() -> Result<(), String> {
        let name = T::NAME;
        let tool = tool_for::<T>();
        let sample = T::complete_sample();
        if let Err(error) = serde_json::from_value::<T::Args>(sample.clone()) {
            return Err(format!(
                "`{name}`: the sample is not valid arguments ({error}): {sample}"
            ));
        }
        let schema = Value::Object(tool.input_schema.as_ref().clone());
        sample_covers(&tool.input_schema, &schema, &sample, 0)
            .map_err(|gap| format!("`{name}`: the sample is not complete: {gap}"))?;
        if let Some(accepted) = undeclared_field_accepted::<T::Args>(&sample) {
            return Err(format!("`{name}` accepted an undeclared field: {accepted}"));
        }
        parsed_names_are_shown::<T::Args>(&schema).map_err(|gap| format!("`{name}`: {gap}"))
    }

    /// Every property name and every enum or const word anywhere in `schema`.
    fn shown_names(schema: &Value, names: &mut BTreeSet<String>) {
        match schema {
            Value::Object(map) => {
                if let Some(Value::Object(properties)) = map.get("properties") {
                    names.extend(properties.keys().cloned());
                }
                if let Some(Value::Array(words)) = map.get("enum") {
                    names.extend(words.iter().filter_map(Value::as_str).map(str::to_owned));
                }
                if let Some(Value::String(word)) = map.get("const") {
                    names.insert(word.clone());
                }
                for child in map.values() {
                    shown_names(child, names);
                }
            }
            Value::Array(items) => {
                for child in items {
                    shown_names(child, names);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }

    /// The names `A` parses — read from serde's derive, not from the source —
    /// are exactly the names `schema` shows, none reads as naming another
    /// agent, and nothing in `A` is a shape the tracer cannot enumerate.
    pub(crate) fn parsed_names_are_shown<A: DeserializeOwned>(
        schema: &Value,
    ) -> Result<(), String> {
        let accepted = trace::accepted::<A>()?;
        let parsed: BTreeSet<String> = accepted
            .fields
            .iter()
            .chain(&accepted.variants)
            .cloned()
            .collect();
        let mut shown = BTreeSet::new();
        shown_names(schema, &mut shown);
        let hidden: Vec<&String> = parsed.difference(&shown).collect();
        if !hidden.is_empty() {
            return Err(format!(
                "its handler parses {hidden:?}, which its schema does not show"
            ));
        }
        let unparsed: Vec<&String> = shown.difference(&parsed).collect();
        if !unparsed.is_empty() {
            return Err(format!(
                "its schema shows {unparsed:?}, which its handler does not parse"
            ));
        }
        if let Some(name) = parsed
            .iter()
            .find(|name| !is_legible(name) || is_target_shaped(name))
        {
            return Err(format!(
                "its handler parses `{name}`, which reads as naming another agent"
            ));
        }
        if !accepted.untraceable.is_empty() {
            return Err(format!(
                "its handler has a shape the probe cannot enumerate: {:?}",
                accepted.untraceable
            ));
        }
        Ok(())
    }

    fn schema_of<A: JsonSchema + 'static>() -> Value {
        Value::Object(
            schema_for_input::<A>()
                .map(|schema| schema.as_ref().clone())
                .unwrap_or_default(),
        )
    }

    #[test]
    fn a_variant_hidden_from_the_schema_passes_the_seam_and_fails_the_probe() {
        use seam_fixtures::HiddenVariantArgs;
        let schema = schema_of::<HiddenVariantArgs>();
        // The schema shows three words and nothing target-shaped...
        let Value::Object(map) = schema.clone() else {
            panic!("an object schema");
        };
        check_with(&[Tool::new("hidden", "t", Arc::new(map))], &["room"]).unwrap();
        // ...and the parser takes a session id in a fourth.
        assert!(
            serde_json::from_value::<HiddenVariantArgs>(
                json!({ "room": { "target_session": 42 } })
            )
            .is_ok()
        );
        let error = parsed_names_are_shown::<HiddenVariantArgs>(&schema).unwrap_err();
        assert!(error.contains("target_session"), "{error}");
    }

    #[test]
    fn an_alias_behind_cfg_attr_or_a_macro_passes_the_seam_and_fails_the_probe() {
        use seam_fixtures::{CfgAliasArgs, MacroAliasArgs};
        let schema = schema_of::<CfgAliasArgs>();
        let Value::Object(map) = schema.clone() else {
            panic!("an object schema");
        };
        check_with(
            &[Tool::new("aliased", "t", Arc::new(map))],
            &["room", "after"],
        )
        .unwrap();
        assert!(
            serde_json::from_value::<CfgAliasArgs>(json!({ "room": "repo", "target_session": 7 }))
                .is_ok()
        );
        let error = parsed_names_are_shown::<CfgAliasArgs>(&schema).unwrap_err();
        assert!(error.contains("target_session"), "{error}");
        let error =
            parsed_names_are_shown::<MacroAliasArgs>(&schema_of::<MacroAliasArgs>()).unwrap_err();
        assert!(error.contains("target_session"), "{error}");
    }

    #[test]
    fn an_alias_that_names_no_one_still_fails_the_probe_for_being_hidden() {
        use seam_fixtures::InnocentAliasArgs;
        let schema = schema_of::<InnocentAliasArgs>();
        let error = parsed_names_are_shown::<InnocentAliasArgs>(&schema).unwrap_err();
        assert!(error.contains("does not show"), "{error}");
        assert!(error.contains("later"), "{error}");
    }

    #[test]
    fn a_variant_renamed_in_the_schema_alone_fails_the_probe() {
        use seam_fixtures::RenamedArgs;
        let error = parsed_names_are_shown::<RenamedArgs>(&schema_of::<RenamedArgs>()).unwrap_err();
        assert!(error.contains("target_host"), "{error}");
    }

    #[test]
    fn every_registered_tool_parses_exactly_the_names_its_schema_shows() {
        // The positive control, and the registry's own check: without it a
        // probe that refused everything would pass every test above.
        (entry::<room_post::RoomPostTool>().probe)().unwrap();
        (entry::<room_read::RoomReadTool>().probe)().unwrap();
        (entry::<room_join::RoomJoinTool>().probe)().unwrap();
        (entry::<fleet_status::FleetStatusTool>().probe)().unwrap();
    }

    /// What a tool answers when its result is over the budget, encoded.
    pub(crate) fn too_large_for<T: McpTool>() -> Value {
        serde_json::to_value(T::output_too_large()).unwrap()
    }

    /// Whether `sample` reaches every declared field and every array in
    /// `schema`, so the probe lands in every object the handler type has. A
    /// sample that leaves an optional struct as `null` or an array empty would
    /// never probe inside it.
    fn sample_covers(
        root: &Map<String, Value>,
        schema: &Value,
        sample: &Value,
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_SCHEMA_DEPTH.saturating_mul(2) {
            return Err("the schema nests past the depth cap".to_owned());
        }
        let Value::Object(map) = schema else {
            return Ok(());
        };
        let next = depth.saturating_add(1);
        if let Some(pointer) = map.get("$ref").and_then(Value::as_str) {
            let target = pointer
                .strip_prefix("#/$defs/")
                .and_then(|name| root.get("$defs")?.get(name))
                .or_else(|| {
                    pointer
                        .strip_prefix("#/definitions/")
                        .and_then(|name| root.get("definitions")?.get(name))
                })
                .ok_or_else(|| format!("`{pointer}` does not resolve"))?;
            sample_covers(root, target, sample, next)?;
        }
        if let Some(Value::Array(branches)) = map.get("allOf") {
            for branch in branches {
                sample_covers(root, branch, sample, next)?;
            }
        }
        for key in ["anyOf", "oneOf"] {
            if let Some(Value::Array(branches)) = map.get(key) {
                let substantive: Vec<&Value> = branches
                    .iter()
                    .filter(|branch| !only_null(branch))
                    .collect();
                let candidates = if substantive.is_empty() {
                    branches.iter().collect()
                } else {
                    substantive
                };
                if !candidates
                    .iter()
                    .any(|branch| sample_covers(root, branch, sample, next).is_ok())
                {
                    return Err(format!("no non-null `{key}` branch covers {sample}"));
                }
            }
        }
        if let Some(Value::Object(properties)) = map.get("properties") {
            let Value::Object(fields) = sample else {
                return Err(format!("expected an object, got {sample}"));
            };
            for (field, subschema) in properties {
                let value = fields
                    .get(field)
                    .ok_or_else(|| format!("it omits `{field}`"))?;
                sample_covers(root, subschema, value, next)?;
            }
        }
        if let Some(items) = map.get("items") {
            let Value::Array(elements) = sample else {
                return Err(format!("expected an array, got {sample}"));
            };
            if elements.is_empty() {
                return Err("an array is empty".to_owned());
            }
            for element in elements {
                sample_covers(root, items, element, next)?;
            }
        }
        if sample.is_null() && !only_null(schema) {
            return Err("a value that could be more than null is null".to_owned());
        }
        Ok(())
    }

    fn only_null(schema: &Value) -> bool {
        schema.get("type") == Some(&json!("null"))
            || schema.get("type") == Some(&json!(["null"]))
            || schema.get("const") == Some(&Value::Null)
    }

    /// The whole registry check: the seam over every listed tool, then the
    /// nested probe for each. Returns the first failure.
    fn audit(entries: &[Entry], allowed: &[&str]) -> Result<(), String> {
        let tools: Vec<Tool> = entries.iter().map(|entry| (entry.tool)()).collect();
        check_with(&tools, allowed).map_err(|violation| violation.to_string())?;
        for entry in entries {
            (entry.probe)()?;
        }
        Ok(())
    }

    fn tool(name: &'static str, schema: Value) -> Tool {
        let Value::Object(map) = schema else {
            panic!("a test schema must be an object");
        };
        Tool::new(name, "test tool", Arc::new(map))
    }

    /// A closed top-level object whose one field `change` has `field` as its
    /// schema.
    fn wrap(field: Value) -> Value {
        json!({
            "type": "object",
            "properties": { "change": field },
            "additionalProperties": false
        })
    }

    #[test]
    fn the_registry_is_the_locked_inventory() {
        let names: Vec<String> = registry().iter().map(|t| t.name.to_string()).collect();
        assert_eq!(names, TOOL_INVENTORY);
        let entries: Vec<&str> = ENTRIES.iter().map(|entry| entry.name).collect();
        assert_eq!(entries, TOOL_INVENTORY);
    }

    #[test]
    fn the_registry_passes_the_seam() {
        check(&registry()).unwrap();
    }

    #[test]
    fn every_registered_tool_refuses_undeclared_fields_at_every_level() {
        audit(ENTRIES, ALLOWED_INPUT_FIELDS).unwrap();
    }

    #[test]
    fn no_allowed_field_is_target_shaped() {
        for field in ALLOWED_INPUT_FIELDS {
            assert!(is_legible(field), "{field}");
            assert!(!is_target_shaped(field), "{field}");
        }
    }

    #[test]
    fn a_tool_taking_an_agent_id_is_refused_even_when_allowlisted() {
        let steer = tool(
            "steer",
            json!({
                "type": "object",
                "properties": { "agent_id": { "type": "string" } },
                "additionalProperties": false
            }),
        );
        assert_eq!(
            check_with(&[steer], &["agent_id"]),
            Err(SeamViolation::TargetShaped {
                tool: "steer".to_owned(),
                field: "agent_id".to_owned()
            })
        );
    }

    #[test]
    fn target_shaped_names_are_caught_in_any_spelling() {
        for field in [
            "agentId",
            "AGENT_ID",
            "target",
            "targetSession",
            "job-id",
            "session_id",
            "host",
            "machine",
            "pid",
            "worktree",
            "workspace_path",
            "worker",
            "peer",
            "id",
            "ids",
            "IDs",
            "task_id",
            "runId",
            "RUN-ID",
            "recipient",
            "user",
            "login",
            "assignee",
            "ownerLogin",
            "member_id",
            "thread-id",
            "a g e n t",
            "agent.id",
            "agent\tid",
        ] {
            assert!(is_target_shaped(field), "{field:?} should be refused");
        }
        for field in ["plan", "note", "step", "status"] {
            assert!(
                !is_target_shaped(field),
                "{field} should pass the shape check"
            );
        }
    }

    #[test]
    fn a_field_name_that_is_not_plain_ascii_is_refused_even_when_allowlisted() {
        for field in [
            "\u{0430}gent_id",  // Cyrillic а
            "\u{FF29}\u{FF24}", // fullwidth ＩＤ
            "no\u{200B}te",     // zero-width space
            "note\n",
            "",
            "__",
        ] {
            let schema = json!({
                "type": "object",
                "properties": { field: { "type": "string" } },
                "additionalProperties": false
            });
            assert!(
                matches!(
                    check_with(&[tool("illegible", schema)], &[field]),
                    Err(SeamViolation::IllegibleName { .. })
                ),
                "{field:?} should be refused"
            );
        }
    }

    #[test]
    fn a_target_hidden_in_defs_is_still_refused() {
        let nested = tool(
            "update_plan",
            json!({
                "type": "object",
                "properties": { "change": { "$ref": "#/$defs/Change" } },
                "additionalProperties": false,
                "$defs": {
                    "Change": {
                        "type": "object",
                        "properties": { "targetSessionId": { "type": "string" } },
                        "additionalProperties": false
                    }
                }
            }),
        );
        assert!(matches!(
            check_with(&[nested], &["change"]),
            Err(SeamViolation::TargetShaped { field, .. }) if field == "targetSessionId"
        ));
    }

    #[test]
    fn a_field_nobody_listed_is_refused() {
        let plan = tool(
            "update_plan",
            json!({
                "type": "object",
                "properties": { "plan": { "type": "string" } },
                "additionalProperties": false
            }),
        );
        assert!(matches!(
            check_with(std::slice::from_ref(&plan), &[]),
            Err(SeamViolation::Unlisted { field, .. }) if field == "plan"
        ));
        check_with(&[plan], &["plan"]).unwrap();
    }

    #[test]
    fn an_object_that_admits_undeclared_fields_is_refused() {
        for open in [
            json!({ "type": "object", "properties": {} }),
            json!({ "type": "object", "properties": {}, "additionalProperties": true }),
            wrap(json!({ "type": "object", "properties": {} })),
            wrap(json!({ "type": ["object", "null"], "properties": {} })),
            wrap(json!({ "type": "object", "additionalProperties": {} })),
        ] {
            assert!(
                matches!(
                    check_with(&[tool("open", open.clone())], &["change"]),
                    Err(SeamViolation::OpenObject { .. })
                ),
                "{open}"
            );
        }
    }

    #[test]
    fn a_root_that_is_not_plainly_an_object_is_refused() {
        for root in [
            json!({ "type": ["object", "null"], "properties": {} }),
            json!({ "properties": {}, "additionalProperties": false }),
            json!({}),
        ] {
            assert!(
                matches!(
                    check_with(&[tool("root", root.clone())], &[]),
                    Err(SeamViolation::OpenObject { .. })
                ),
                "{root}"
            );
        }
    }

    #[test]
    fn every_shape_that_could_admit_an_unclosed_object_is_refused() {
        // Each of these accepts `{"change": {"target_agent": "..."}}` or an
        // equivalent somewhere, without the schema saying so. Against the seam
        // this replaced, 15 of the 16 passed; only the open `allOf` branch was
        // caught.
        let passed = [
            ("empty subschema", wrap(json!({}))),
            ("boolean true subschema", wrap(json!(true))),
            (
                "annotation-only subschema",
                wrap(json!({ "description": "anything" })),
            ),
            (
                "array with open items",
                wrap(json!({ "type": "array", "items": {} })),
            ),
            ("array with no items", wrap(json!({ "type": "array" }))),
            (
                "anyOf with a not branch",
                wrap(json!({ "anyOf": [{ "type": "string" }, { "not": { "type": "string" } }] })),
            ),
            (
                "oneOf with an open branch",
                wrap(json!({ "oneOf": [{ "type": "string" }, {}] })),
            ),
            (
                "allOf with an open object branch",
                wrap(json!({ "allOf": [{ "type": "object" }] })),
            ),
            ("not", wrap(json!({ "not": { "type": "string" } }))),
            (
                "ref to an open definition",
                json!({
                    "type": "object",
                    "properties": { "change": { "$ref": "#/$defs/Change" } },
                    "additionalProperties": false,
                    "$defs": { "Change": {} }
                }),
            ),
            (
                "ref to a missing definition",
                wrap(json!({ "$ref": "#/$defs/Nowhere" })),
            ),
            (
                "remote ref",
                wrap(json!({ "$ref": "https://example.com/schema" })),
            ),
            (
                "object-valued const",
                wrap(json!({ "const": { "target_agent": "someone" } })),
            ),
            (
                "unknown keyword",
                wrap(json!({ "type": "string", "if": { "type": "string" } })),
            ),
            ("top-level empty schema", json!({})),
            (
                "top-level anyOf with not",
                json!({ "anyOf": [{ "type": "string" }, { "not": { "type": "string" } }] }),
            ),
        ]
        .into_iter()
        .filter(|(_, schema)| check_with(&[tool("probe", schema.clone())], &["change"]).is_ok())
        .map(|(label, _)| label)
        .collect::<Vec<_>>();
        assert!(passed.is_empty(), "passed the seam: {passed:#?}");
    }

    #[test]
    fn keywords_the_seam_cannot_prove_closed_are_named() {
        for (keyword, schema) in [
            (
                "patternProperties",
                wrap(json!({
                    "type": "object",
                    "patternProperties": { "^.*$": { "type": "string" } },
                    "additionalProperties": false
                })),
            ),
            ("not", wrap(json!({ "not": { "type": "string" } }))),
            ("if", wrap(json!({ "type": "string", "if": {} }))),
            ("$ref", wrap(json!({ "$ref": "#/$defs/a~1b" }))),
            ("$ref", wrap(json!({ "$ref": "#" }))),
            ("enum", wrap(json!({ "enum": ["plan", { "agent": "x" }] }))),
            ("type", wrap(json!({ "type": "Object" }))),
            ("anyOf", wrap(json!({ "anyOf": [] }))),
            ("$dynamicRef", wrap(json!({ "$dynamicRef": "#meta" }))),
            (
                "$vocabulary",
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                    "$vocabulary": { "https://json-schema.org/draft/2020-12/vocab/applicator": false }
                }),
            ),
        ] {
            let result = check_with(&[tool("keyword", schema.clone())], &["change"]);
            assert!(
                matches!(&result, Err(SeamViolation::Unsupported { keyword: named, .. }) if named == keyword),
                "{keyword}: {result:?} for {schema}"
            );
        }
    }

    #[test]
    fn a_metaschema_other_than_2020_12_is_refused() {
        for metaschema in [
            json!("https://example.com/permissive-metaschema"),
            json!("http://json-schema.org/draft-07/schema#"),
            json!("https://json-schema.org/draft/2020-12/schema#"),
            json!(true),
        ] {
            let schema = json!({
                "$schema": metaschema,
                "type": "object",
                "properties": {},
                "additionalProperties": false
            });
            assert!(
                matches!(
                    check_with(&[tool("meta", schema.clone())], &[]),
                    Err(SeamViolation::Unsupported { keyword, .. }) if keyword == "$schema"
                ),
                "{schema}"
            );
        }
        let schema = json!({
            "$schema": METASCHEMA_2020_12,
            "$id": "https://sealmesh.invalid/tool",
            "type": "object",
            "properties": {},
            "additionalProperties": false
        });
        check_with(&[tool("meta", schema)], &[]).unwrap();
    }

    #[test]
    fn schema_and_id_below_the_root_are_refused() {
        for (keyword, schema) in [
            (
                "$schema",
                wrap(json!({ "$schema": METASCHEMA_2020_12, "type": "string" })),
            ),
            (
                "$id",
                wrap(json!({ "$id": "https://example.com/elsewhere", "type": "string" })),
            ),
            (
                "$id",
                json!({
                    "type": "object",
                    "properties": { "change": { "$ref": "#/$defs/Change" } },
                    "additionalProperties": false,
                    "$defs": { "Change": { "$id": "https://example.com/c", "type": "string" } }
                }),
            ),
        ] {
            let result = check_with(&[tool("nested_id", schema.clone())], &["change"]);
            assert!(
                matches!(&result, Err(SeamViolation::Unsupported { keyword: named, .. }) if named == keyword),
                "{keyword}: {result:?} for {schema}"
            );
        }
    }

    #[test]
    fn a_ref_cycle_with_nothing_between_its_references_is_refused() {
        for (label, defs, field) in [
            (
                "self reference",
                json!({ "A": { "$ref": "#/$defs/A" } }),
                json!({ "$ref": "#/$defs/A" }),
            ),
            (
                "self reference through allOf",
                json!({ "A": { "allOf": [{ "$ref": "#/$defs/A" }] } }),
                json!({ "$ref": "#/$defs/A" }),
            ),
            (
                "A to B to A through anyOf and oneOf",
                json!({
                    "A": { "anyOf": [{ "$ref": "#/$defs/B" }, { "type": "null" }] },
                    "B": { "oneOf": [{ "$ref": "#/$defs/A" }, { "type": "string" }] }
                }),
                json!({ "type": "string" }),
            ),
        ] {
            let schema = json!({
                "type": "object",
                "properties": { "change": field },
                "additionalProperties": false,
                "$defs": defs
            });
            let result = check_with(&[tool("cycle", schema)], &["change"]);
            assert!(
                matches!(result, Err(SeamViolation::UnguardedCycle { .. })),
                "{label}: {result:?}"
            );
        }
    }

    #[test]
    fn schemas_that_prove_what_they_admit_still_pass() {
        // Without these, a seam that refused everything would pass every test.
        for (label, schema) in [
            (
                "bounded string",
                wrap(json!({ "type": "string", "maxLength": 200 })),
            ),
            (
                "nullable string",
                wrap(json!({ "type": ["string", "null"] })),
            ),
            (
                "array of integers",
                wrap(json!({ "type": "array", "items": { "type": "integer" } })),
            ),
            (
                "closed nested object",
                wrap(json!({
                    "type": "object",
                    "properties": { "note": { "type": "string" } },
                    "required": ["note"],
                    "additionalProperties": false
                })),
            ),
            ("string enum", wrap(json!({ "enum": ["pending", "done"] }))),
            ("scalar const", wrap(json!({ "const": 3 }))),
            (
                "anyOf of scalars",
                wrap(json!({ "anyOf": [{ "type": "string" }, { "type": "null" }] })),
            ),
            ("false subschema", wrap(json!(false))),
            (
                "recursive definition through a property",
                json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "title": "UpdatePlan",
                    "type": "object",
                    "properties": { "change": { "$ref": "#/$defs/Step" } },
                    "required": ["change"],
                    "additionalProperties": false,
                    "$defs": {
                        "Step": {
                            "type": "object",
                            "properties": {
                                "note": { "type": "string" },
                                "next": { "anyOf": [{ "$ref": "#/$defs/Step" }, { "type": "null" }] }
                            },
                            "additionalProperties": false
                        }
                    }
                }),
            ),
            (
                "recursive definition through items",
                json!({
                    "type": "object",
                    "properties": { "change": { "$ref": "#/$defs/Tree" } },
                    "additionalProperties": false,
                    "$defs": {
                        "Tree": {
                            "type": "array",
                            "items": { "anyOf": [{ "$ref": "#/$defs/Tree" }, { "type": "integer" }] }
                        }
                    }
                }),
            ),
        ] {
            let result = check_with(
                &[tool("closed", schema.clone())],
                &["change", "note", "next"],
            );
            assert!(result.is_ok(), "{label}: {result:?} for {schema}");
        }
    }

    #[test]
    fn a_schema_nested_past_the_depth_cap_is_refused() {
        let mut schema = json!({ "type": "string" });
        for _ in 0..=MAX_SCHEMA_DEPTH {
            schema = wrap(schema);
        }
        assert!(matches!(
            check_with(&[tool("deep", schema)], &["change"]),
            Err(SeamViolation::TooDeep { .. })
        ));
    }

    #[test]
    fn a_field_named_like_a_schema_keyword_is_checked_as_a_field() {
        let schema = json!({
            "type": "object",
            "properties": { "properties": { "type": "string" } },
            "additionalProperties": false
        });
        check_with(&[tool("keyword_field", schema.clone())], &["properties"]).unwrap();
        assert!(matches!(
            check_with(&[tool("keyword_field", schema)], &[]),
            Err(SeamViolation::Unlisted { field, .. }) if field == "properties"
        ));
    }

    #[test]
    fn a_top_level_argument_the_signature_does_not_declare_is_refused_before_dispatch() {
        let args = |value: Value| match value {
            Value::Object(map) => map,
            _ => panic!("arguments must be an object"),
        };
        let plan = tool(
            "update_plan",
            json!({
                "type": "object",
                "properties": { "plan": { "type": "string" } },
                "additionalProperties": false
            }),
        );

        refuse_undeclared_arguments(&plan, None).unwrap();
        refuse_undeclared_arguments(&plan, Some(&args(json!({})))).unwrap();
        refuse_undeclared_arguments(&plan, Some(&args(json!({ "plan": "ship it" })))).unwrap();

        let refused = refuse_undeclared_arguments(
            &plan,
            Some(&args(
                json!({ "plan": "ship it", "agent_id": "someone-else" }),
            )),
        )
        .unwrap_err();
        assert!(!refused.message.contains("agent_id"), "{refused:?}");

        let bare = tool(
            "bare",
            json!({ "type": "object", "additionalProperties": false }),
        );
        assert!(refuse_undeclared_arguments(&bare, Some(&args(json!({ "x": 1 })))).is_err());
    }

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct StrictPlan {
        plan: String,
        steps: Vec<StrictStep>,
    }

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct StrictStep {
        note: String,
    }

    /// Strict at the top, lenient one level down: the shape the top-level
    /// check in `call` cannot see.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct LenientInside {
        plan: String,
        steps: Vec<LenientStep>,
    }

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    struct LenientStep {
        note: String,
    }

    /// The carry-forward's first case: a handler type that grew a target field
    /// its hand-written schema never declared.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct DriftPlan {
        steps: Vec<DriftStep>,
    }

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct DriftStep {
        note: String,
        target_agent: Option<String>,
    }

    /// The carry-forward's second case: a `Value` field, which accepts nested
    /// objects a string sample never probes.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize, JsonSchema)]
    #[schemars(crate = "rmcp::schemars")]
    #[serde(deny_unknown_fields)]
    struct ValuePlan {
        note: String,
        extra: Value,
    }

    fn plan_sample() -> Value {
        json!({ "plan": "ship it", "steps": [{ "note": "merge" }] })
    }

    macro_rules! test_tool {
        ($tool:ident, $name:literal, $args:ty, $sample:expr) => {
            struct $tool;

            impl McpTool for $tool {
                const NAME: &'static str = $name;
                const DESCRIPTION: &'static str = "test tool";
                type Args = $args;
                type Output = ();
                type Failure = ();

                fn annotations() -> ToolAnnotations {
                    ToolAnnotations::new()
                }

                fn output_too_large() {}

                fn run(
                    _router: RouterAccess,
                    _args: $args,
                ) -> impl Future<Output = Result<(), ()>> + Send {
                    std::future::ready(Ok(()))
                }

                fn complete_sample() -> Value {
                    $sample
                }
            }
        };
    }

    test_tool!(StrictTool, "update_plan", StrictPlan, plan_sample());
    test_tool!(LenientTool, "update_plan", LenientInside, plan_sample());
    test_tool!(
        DriftTool,
        "drift",
        DriftPlan,
        json!({ "steps": [{ "note": "n" }] })
    );
    test_tool!(
        DriftToolWithFullSample,
        "drift",
        DriftPlan,
        json!({ "steps": [{ "note": "n", "target_agent": "someone" }] })
    );
    test_tool!(
        ValueTool,
        "value",
        ValuePlan,
        json!({ "note": "n", "extra": "s" })
    );
    test_tool!(
        IncompleteSampleTool,
        "update_plan",
        StrictPlan,
        json!({ "plan": "ship it", "steps": [] })
    );

    const PLAN_FIELDS: &[&str] = &["plan", "steps", "note", "target_agent", "extra"];

    #[test]
    fn a_strict_tool_passes_the_registry_audit() {
        // The positive control: without it, an audit that refused every tool
        // would pass every test below.
        audit(&[entry::<StrictTool>()], PLAN_FIELDS).unwrap();
    }

    #[test]
    fn a_handler_type_lenient_below_the_top_fails_the_registry_audit_twice() {
        // The schema is derived from the type, so the lenient level is open in
        // the schema the seam checks...
        let error = audit(&[entry::<LenientTool>()], PLAN_FIELDS).unwrap_err();
        assert!(error.contains("could admit an object"), "{error}");
        // ...and the probe, run from the registry entry alone, catches it too.
        let probed = (entry::<LenientTool>().probe)().unwrap_err();
        assert!(probed.contains("accepted an undeclared field"), "{probed}");
        assert!(probed.contains(PROBE), "{probed}");
    }

    #[test]
    fn a_target_field_the_handler_type_grew_is_on_the_schema_and_refused() {
        // Before the schema was derived from the type, a hand-written schema
        // without `target_agent` passed and `parse_arguments` accepted it.
        let error = audit(&[entry::<DriftTool>()], PLAN_FIELDS).unwrap_err();
        assert!(error.contains("`target_agent`"), "{error}");
        assert!(error.contains("naming another agent"), "{error}");
        // The sample that skipped the field no longer counts as complete.
        let probed = (entry::<DriftTool>().probe)().unwrap_err();
        assert!(probed.contains("omits `target_agent`"), "{probed}");
        // With the field sampled, the type probe names it as target-shaped.
        let probed = (entry::<DriftToolWithFullSample>().probe)().unwrap_err();
        assert!(probed.contains("naming another agent"), "{probed}");
    }

    #[test]
    fn a_value_field_is_refused_by_the_seam_even_though_its_sample_cannot_probe_it() {
        let schema = tool_for::<ValueTool>().input_schema;
        assert_eq!(schema["properties"]["extra"], json!(true), "{schema:?}");
        // The undeclared-field probe cannot reach inside a string sample, but
        // the type probe cannot enumerate a `Value` and refuses it...
        let probed = (entry::<ValueTool>().probe)().unwrap_err();
        assert!(probed.contains("cannot enumerate"), "{probed}");
        // ...and the derived schema closes it too.
        let error = audit(&[entry::<ValueTool>()], PLAN_FIELDS).unwrap_err();
        assert!(error.contains("could admit an object"), "{error}");
    }

    #[test]
    fn a_sample_that_never_reaches_a_nested_object_fails_the_registry_audit() {
        let error = audit(&[entry::<IncompleteSampleTool>()], PLAN_FIELDS).unwrap_err();
        assert!(error.contains("an array is empty"), "{error}");
    }

    #[test]
    fn a_nested_undeclared_field_is_refused_by_a_strict_handler_type() {
        assert_undeclared_fields_refused::<StrictPlan>(&plan_sample());

        let Value::Object(nested_probe) =
            json!({ "plan": "ship it", "steps": [{ "note": "merge", "target_agent": "someone" }] })
        else {
            panic!("arguments must be an object");
        };
        let refused = parse_arguments::<StrictPlan>(Some(nested_probe)).unwrap_err();
        assert!(!refused.message.contains("target_agent"), "{refused:?}");
    }

    #[test]
    fn the_nested_probe_catches_a_handler_type_lenient_below_the_top() {
        let accepted = undeclared_field_accepted::<LenientInside>(&plan_sample())
            .expect("a lenient nested type must be caught");
        assert_eq!(
            accepted["steps"][0][PROBE],
            json!(true),
            "the probe that got through should be the nested one: {accepted}"
        );
        assert!(undeclared_field_accepted::<StrictPlan>(&plan_sample()).is_none());
    }

    #[test]
    fn absent_arguments_parse_as_an_empty_object() {
        #[derive(Debug, serde::Deserialize, PartialEq)]
        #[serde(deny_unknown_fields)]
        struct NoArguments {}
        assert_eq!(
            parse_arguments::<NoArguments>(None).unwrap(),
            NoArguments {}
        );
        assert!(parse_arguments::<StrictPlan>(None).is_err());
    }

    #[test]
    fn a_name_registered_twice_is_refused() {
        let schema = json!({ "type": "object", "properties": {}, "additionalProperties": false });
        assert!(matches!(
            check_with(&[tool("a", schema.clone()), tool("a", schema)], &[]),
            Err(SeamViolation::Duplicate { .. })
        ));
    }

    #[tokio::test]
    async fn a_name_not_in_the_table_is_refused_before_any_handler_runs() {
        for name in [
            "FLEET_STATUS",
            "fleet_status ",
            " fleet_status",
            "fleet-status",
            "fleetstatus",
            "fleet_status\u{0}",
            "steer_agent",
            "",
        ] {
            let request = CallToolRequestParams::new(name.to_owned());
            let refused = call(&no_config(), request)
                .await
                .expect_err("an unlisted name must not dispatch");
            assert_eq!(refused.message, "no such tool", "{name:?}");
        }
    }

    /// A router access whose config does not exist, so no test dispatching
    /// through it can read this machine's real config, reach a real router
    /// or run `gh`.
    pub(crate) fn no_config() -> RouterAccess {
        RouterAccess::new(Some(std::path::PathBuf::from(
            "/nonexistent/sealmesh-mcp-test/config.toml",
        )))
    }

    #[test]
    fn every_tool_declares_output_too_large_as_its_own_failure_and_says_so() {
        for entry in ENTRIES {
            assert_eq!(
                (entry.too_large)(),
                json!({ "error": "output_too_large" }),
                "{}",
                entry.name
            );
            let tool = (entry.tool)();
            let description = tool.description.as_deref().unwrap_or_default();
            assert!(
                description.contains("output_too_large"),
                "`{}` does not name output_too_large: {description}",
                entry.name
            );
        }
    }

    #[test]
    fn a_consumer_key_field_is_refused_in_any_args_even_when_allowlisted() {
        // The drain consumer key decides whose room messages a drain takes.
        // A tool taking it would let one agent consume another's (Q5).
        for field in ["consumer", "consumer_key", "consumerKey", "CONSUMER"] {
            let schema = json!({
                "type": "object",
                "properties": { field: { "type": "string" } },
                "additionalProperties": false
            });
            assert!(
                matches!(
                    check_with(&[tool("room_post", schema)], &[field]),
                    Err(SeamViolation::TargetShaped { field: named, .. }) if named == field
                ),
                "{field} passed"
            );
        }
        // Nested, too, where a handler type would hide it one level down.
        let nested = json!({
            "type": "object",
            "properties": { "room": { "$ref": "#/$defs/Room" } },
            "additionalProperties": false,
            "$defs": { "Room": {
                "type": "object",
                "properties": { "consumer": { "type": "string" } },
                "additionalProperties": false
            } }
        });
        assert!(matches!(
            check_with(&[tool("room_post", nested)], &["room", "consumer"]),
            Err(SeamViolation::TargetShaped { field, .. }) if field == "consumer"
        ));
    }

    /// Attribute keys that let a handler type accept what its derived schema
    /// does not show, or show what it does not accept. Assembled at run time,
    /// so this file's own source does not trip the scan below.
    fn forbidden_keys() -> Vec<(String, &'static str)> {
        let mut keys = Vec::new();
        for key in ["alias", "flatten", "with", "deserialize_with"] {
            keys.push((["se", "rde"].concat(), key));
        }
        for key in [
            "with",
            "schema_with",
            "skip",
            "rename",
            "transform",
            "extend",
        ] {
            keys.push((["sche", "mars"].concat(), key));
        }
        keys
    }

    /// Whether `source` attaches a serde or schemars attribute through
    /// `cfg_attr`, where the key scan below would not look.
    fn cfg_attr_carries_derive_attributes(source: &str) -> bool {
        let opener = ["#[cfg", "_attr("].concat();
        let carried = [["se", "rde("].concat(), ["sche", "mars("].concat()];
        let mut rest = source;
        while let Some(at) = rest.find(&opener) {
            rest = &rest[at + opener.len()..];
            let end = rest.find(")]").unwrap_or(rest.len());
            if carried
                .iter()
                .any(|attr| rest[..end].contains(attr.as_str()))
            {
                return true;
            }
        }
        false
    }

    /// Every `#[<attr>(...)]` in `source`, as its top-level keys: `a = 1, b`
    /// gives `a` and `b`, and a nested `length(max = 3)` gives `length`.
    fn attribute_keys(source: &str, attr: &str) -> Vec<String> {
        let opener = format!("#[{attr}(");
        let mut keys = Vec::new();
        let mut rest = source;
        while let Some(at) = rest.find(&opener) {
            rest = &rest[at + opener.len()..];
            let mut depth = 0usize;
            let mut key = String::new();
            let mut in_value = false;
            let mut in_string = false;
            for c in rest.chars() {
                if in_string {
                    in_string = c != '"';
                    continue;
                }
                match c {
                    '"' => in_string = true,
                    '(' => depth += 1,
                    ')' if depth == 0 => break,
                    ')' => depth -= 1,
                    ',' if depth == 0 => {
                        keys.push(std::mem::take(&mut key).trim().to_owned());
                        in_value = false;
                    }
                    '=' if depth == 0 => in_value = true,
                    c if depth == 0 && !in_value => key.push(c),
                    _ => {}
                }
            }
            keys.push(key.trim().to_owned());
        }
        keys.retain(|key| !key.is_empty());
        keys
    }

    /// Every `.rs` file under this crate's `src/`, with its contents.
    fn crate_sources() -> Vec<(std::path::PathBuf, String)> {
        let mut found = Vec::new();
        let mut pending = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    found.push((path, text));
                }
            }
        }
        found
    }

    /// How a source file breaks the seam through an attribute, if it does.
    fn seam_attribute_violations(source: &str) -> Vec<String> {
        let mut found = Vec::new();
        for (attr, key) in forbidden_keys() {
            if attribute_keys(source, &attr).iter().any(|k| k == key) {
                found.push(format!("{attr}({key})"));
            }
        }
        if cfg_attr_carries_derive_attributes(source) {
            found.push("a serde or schemars attribute behind cfg_attr".to_owned());
        }
        let hand_written = format!("{} for ", "JsonSchema");
        if source
            .lines()
            .any(|line| line.trim_start().starts_with("impl") && line.contains(&hand_written))
        {
            found.push("a hand-written JsonSchema impl".to_owned());
        }
        found
    }

    #[test]
    fn no_type_in_this_crate_can_make_its_schema_and_its_parser_disagree() {
        // Every type reachable from a tool's `Args` is defined in this crate:
        // no dependency's type derives `JsonSchema`. So the whole crate is
        // scanned, a superset of what an `Args` can reach. The review of
        // XYZ-67 showed each of these passing the seam, the probe and the
        // audit while `parse_arguments` accepted a field the schema never
        // named.
        let sources = crate_sources();
        assert!(
            sources
                .iter()
                .any(|(path, _)| path.ends_with("tools/room_post.rs")),
            "the scan did not find the tool modules"
        );
        let mut violations = Vec::new();
        for (path, text) in &sources {
            // The fixtures spell out what the probe must catch; see the test
            // below that keeps them test-only.
            if path.ends_with("tools/seam_fixtures.rs") {
                continue;
            }
            for found in seam_attribute_violations(text) {
                violations.push(format!("{}: {found}", path.display()));
            }
        }
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn the_one_file_the_scan_skips_is_compiled_only_under_test_and_is_caught_by_it() {
        let tools = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tools.rs"),
        )
        .unwrap();
        let declared = ["mod seam", "_fixtures;"].concat();
        let line = tools
            .lines()
            .position(|line| line.trim() == declared)
            .expect("seam_fixtures is declared");
        assert_eq!(
            tools.lines().nth(line - 1).map(str::trim),
            Some("#[cfg(test)]")
        );
        let fixtures = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tools/seam_fixtures.rs"),
        )
        .unwrap();
        assert!(!seam_attribute_violations(&fixtures).is_empty());
    }

    #[test]
    fn the_attribute_scan_catches_each_shape_it_refuses() {
        // Without this, a scan that matched nothing would pass the test above.
        let serde = ["se", "rde"].concat();
        let schemars = ["sche", "mars"].concat();
        let open = |attr: &str, body: &str| {
            format!(
                "#[{attr}({body})]
struct A;"
            )
        };
        for (source, expected) in [
            (open(&serde, r#"alias = "target_agent""#), "(alias)"),
            (open(&serde, "flatten"), "(flatten)"),
            (open(&serde, r#"default, with = "m""#), "(with)"),
            (
                open(&serde, r#"deserialize_with = "f""#),
                "(deserialize_with)",
            ),
            (open(&schemars, r#"with = "Option<String>""#), "(with)"),
            (open(&schemars, r#"schema_with = "f""#), "(schema_with)"),
            (open(&schemars, "skip"), "(skip)"),
            (open(&schemars, r#"rename = "repo""#), "(rename)"),
            (open(&schemars, r#"transform = f"#), "(transform)"),
            (open(&schemars, r#"extend("x" = 1)"#), "(extend)"),
            (
                format!(
                    "#[{}(all(), {serde}(alias = \"t\"))]\nstruct A;",
                    ["cfg", "_attr"].concat()
                ),
                "behind cfg_attr",
            ),
        ] {
            let found = seam_attribute_violations(&source);
            assert_eq!(found.len(), 1, "{source}: {found:?}");
            assert!(found[0].ends_with(expected), "{source}: {found:?}");
        }
        let hand = format!("impl rmcp::schemars::{} for A {{}}", "JsonSchema");
        assert_eq!(
            seam_attribute_violations(&hand),
            vec!["a hand-written JsonSchema impl".to_owned()]
        );
        for fine in [
            open(&serde, "deny_unknown_fields"),
            open(&serde, r#"rename_all = "snake_case", deny_unknown_fields"#),
            open(&serde, r#"tag = "error", rename_all = "snake_case""#),
            open(&serde, "default"),
            open(&schemars, "length(min = 1, max = 4000)"),
            open(&schemars, r#"crate = "rmcp::schemars""#),
            open(&serde, r#"rename = "with""#),
        ] {
            assert!(seam_attribute_violations(&fine).is_empty(), "{fine}");
        }
    }

    #[test]
    fn a_result_over_the_output_budget_is_refused_rather_than_sent() {
        let small = vec!["x"; 16];
        assert!(encode(&small).is_ok());
        let large = vec!["x".repeat(1024); MAX_OUTPUT_BYTES / 1024 + 1];
        assert!(matches!(encode(&large), Err(Unencodable::TooLarge)));
    }

    #[test]
    fn the_budget_is_measured_on_the_whole_response_with_its_doubly_escaped_copy() {
        // 400 KiB of quotes: 800 KiB as `structuredContent`, 1.6 MiB as the
        // text copy. Each copy alone is under 1 MiB of plain text, so a budget
        // on the value alone, or on half the cap per copy, lets it through.
        let quotes = "\"".repeat(400 * 1024);
        let value = encode(&quotes).unwrap_or_else(|_| panic!("the value alone fits"));
        assert!(!fits(&CallToolResult::structured(value)));
        // An ordinary result well under the cap fits.
        let value = encode(&vec!["x"; 1024]).unwrap_or_else(|_| panic!("small"));
        assert!(fits(&CallToolResult::structured(value)));
    }

    struct QuotesTool;

    impl McpTool for QuotesTool {
        const NAME: &'static str = "quotes";
        const DESCRIPTION: &'static str = "test tool";
        type Args = StrictPlan;
        type Output = String;
        type Failure = Value;

        fn annotations() -> ToolAnnotations {
            ToolAnnotations::new()
        }

        fn output_too_large() -> Value {
            json!({ "error": "output_too_large" })
        }

        fn run(
            _router: RouterAccess,
            _args: StrictPlan,
        ) -> impl Future<Output = Result<String, Value>> + Send {
            std::future::ready(Ok("\"".repeat(400 * 1024)))
        }

        fn complete_sample() -> Value {
            plan_sample()
        }
    }

    #[tokio::test]
    async fn a_result_whose_escaped_copy_breaks_the_budget_is_answered_output_too_large() {
        let Value::Object(arguments) = plan_sample() else {
            panic!("arguments must be an object");
        };
        let response = (entry::<QuotesTool>().handle)(no_config(), Some(arguments))
            .await
            .unwrap();
        let CallToolResponse::Complete(result) = response else {
            panic!("a tool call completes");
        };
        let sent = serde_json::to_value(&result).unwrap();
        assert_eq!(sent["isError"], json!(true), "{}", &sent.to_string()[..200]);
        assert_eq!(
            sent["structuredContent"],
            json!({ "error": "output_too_large" })
        );
    }
}
