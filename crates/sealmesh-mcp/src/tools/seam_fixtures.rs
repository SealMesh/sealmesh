//! Handler types that break the seam on purpose, for the tests that prove
//! the type-driven probe catches them. Compiled only under `cfg(test)`, and
//! the one file the attribute scan skips: it has to spell out the attributes
//! the scan refuses everywhere else. A test checks both.
//!
//! Each is one of the two escapes the review of PR #109 demonstrated end to
//! end: they pass `check(&registry())` and a text scan that looks only for a
//! literal `#[serde(` with a fixed key list.

#![allow(dead_code)]

use rmcp::schemars::JsonSchema;
use serde::Deserialize;

/// A scope the schema shows as three words, and the parser takes a fourth
/// that carries a session id.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "snake_case")]
pub(crate) enum HiddenVariantScope {
    Ticket,
    Repo,
    Project,
    #[schemars(skip)]
    TargetSession(u64),
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct HiddenVariantArgs {
    pub(crate) room: HiddenVariantScope,
}

/// A cursor the parser also takes under a name the schema never shows,
/// attached through `cfg_attr` so no literal `#[serde(` carries it.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct CfgAliasArgs {
    pub(crate) room: HiddenlessScope,
    #[serde(default)]
    #[cfg_attr(all(), serde(alias = "target_session"))]
    pub(crate) after: Option<u64>,
}

/// The same alias, emitted by a macro, so the attribute appears nowhere in
/// the source as written.
macro_rules! aliased {
    ($name:ident, $alias:literal) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[schemars(crate = "rmcp::schemars")]
        #[serde(deny_unknown_fields)]
        pub(crate) struct $name {
            #[serde(alias = $alias)]
            pub(crate) after: Option<u64>,
        }
    };
}

aliased!(MacroAliasArgs, "target_session");

// An alias whose name reads as nothing in particular: the name check has
// nothing to say about it, so only the comparison with the schema catches
// that the parser takes a field the schema never shows.
aliased!(InnocentAliasArgs, "later");

/// An honest scope, so [`CfgAliasArgs`] fails for its alias alone.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "snake_case")]
pub(crate) enum HiddenlessScope {
    Ticket,
    Repo,
}

/// A variant the schema shows under one name and the parser takes under
/// another.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "snake_case")]
pub(crate) enum RenamedScope {
    Ticket,
    #[schemars(rename = "repo")]
    TargetHost,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub(crate) struct RenamedArgs {
    pub(crate) room: RenamedScope,
}
