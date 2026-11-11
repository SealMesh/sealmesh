use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};

struct Subscription {
    event: &'static str,
    matcher: Option<&'static str>,
    /// Seconds Claude Code waits for the hook before going on without it.
    timeout: Option<u64>,
}

/// `PreToolUse` takes every tool (`*`): one entry covers the guardrail,
/// which must see every call, and `AskUserQuestion`, which marks the agent
/// as needing input. It runs before every tool call, so it carries a short
/// timeout; a hook that hangs past it is skipped and the call goes ahead.
const SUBSCRIPTIONS: &[Subscription] = &[
    Subscription {
        event: "UserPromptSubmit",
        matcher: None,
        timeout: None,
    },
    Subscription {
        event: "Stop",
        matcher: None,
        timeout: None,
    },
    Subscription {
        event: "PreToolUse",
        matcher: Some("*"),
        timeout: Some(5),
    },
    Subscription {
        event: "SessionEnd",
        matcher: None,
        timeout: None,
    },
];

pub struct Args {
    pub dry_run: bool,
    pub settings: Option<PathBuf>,
}

pub fn run(args: &Args) -> Result<()> {
    let path = match &args.settings {
        Some(p) => p.clone(),
        None => default_settings_path()?,
    };

    let binary = std::env::current_exe()
        .context("resolving the path to this binary")?
        .display()
        .to_string();
    let command = format!("{binary} claude-hook");

    let before = read_settings(&path)?;
    let Merged {
        settings: after,
        added,
        blocked,
    } = merge(before.clone(), &command);

    for location in &blocked {
        eprintln!(
            "sealmesh: {} holds something at {location} that is not the shape a hook \
             list takes. Left alone — it is your configuration, not ours.",
            path.display()
        );
    }

    if added.is_empty() {
        if blocked.is_empty() {
            println!("Already installed in {}. Nothing to do.", path.display());
            return Ok(());
        }
        bail!(
            "nothing was installed in {}. No hook will run, so sealmesh cannot see \
             this machine's Claude Code sessions until the shapes above are fixed.",
            path.display()
        );
    }

    println!("{}", path.display());
    for event in &added {
        println!("  + {event}  ->  {command}");
    }

    if args.dry_run {
        println!("\n--dry-run: nothing written.");
        return Ok(());
    }

    write_settings(&path, &after)?;
    println!("\nInstalled. Restart any running Claude Code session to pick it up.");
    Ok(())
}

fn default_settings_path() -> Result<PathBuf> {
    dirs::home_dir()
        .map(|home| home.join(".claude/settings.json"))
        .ok_or_else(|| anyhow!("no home directory to resolve ~/.claude/settings.json against"))
}

fn read_settings(path: &Path) -> Result<Value> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok(json!({}));
    };
    if text.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(&text).with_context(|| {
        format!(
            "{} is not valid JSON. Fix it first — refusing to overwrite it.",
            path.display()
        )
    })
}

struct Merged {
    settings: Value,
    added: Vec<String>,
    blocked: Vec<String>,
}

fn merge(mut settings: Value, command: &str) -> Merged {
    let mut added = Vec::new();
    let mut blocked = Vec::new();

    let Some(root) = settings.as_object_mut() else {
        blocked.push("the root of the file".to_owned());
        return Merged {
            settings,
            added,
            blocked,
        };
    };

    let hooks = root.entry("hooks").or_insert_with(|| json!({}));
    let Some(hooks) = hooks.as_object_mut() else {
        blocked.push("hooks".to_owned());
        return Merged {
            settings,
            added,
            blocked,
        };
    };

    for sub in SUBSCRIPTIONS {
        let event = sub.event;
        let entries = hooks.entry(event).or_insert_with(|| json!([]));
        let Some(entries) = entries.as_array_mut() else {
            blocked.push(format!("hooks.{event}"));
            continue;
        };
        let label = |matcher: Option<&str>| match matcher {
            Some(m) => format!("{event}({m})"),
            None => event.to_owned(),
        };

        // This binary's entries under any other matcher are an older
        // install (`PreToolUse(AskUserQuestion)` before the guardrail):
        // replaced, never left beside the new one to fire twice.
        let replaced: Vec<String> = take_ours_elsewhere(entries, command, sub.matcher)
            .into_iter()
            .map(|old| label(old.as_deref()))
            .collect();
        let note = if replaced.is_empty() {
            String::new()
        } else {
            format!(" (replaces {})", replaced.join(", "))
        };

        if already_installed(entries, command, sub.matcher) {
            if !replaced.is_empty() {
                added.push(format!("{}{note}", label(sub.matcher)));
            }
            continue;
        }

        let mut entry = serde_json::Map::new();
        if let Some(matcher) = sub.matcher {
            entry.insert("matcher".to_owned(), json!(matcher));
        }
        let mut hook = serde_json::Map::new();
        hook.insert("type".to_owned(), json!("command"));
        hook.insert("command".to_owned(), json!(command));
        if let Some(timeout) = sub.timeout {
            hook.insert("timeout".to_owned(), json!(timeout));
        }
        entry.insert("hooks".to_owned(), json!([Value::Object(hook)]));
        entries.push(Value::Object(entry));
        added.push(format!("{}{note}", label(sub.matcher)));
    }

    Merged {
        settings,
        added,
        blocked,
    }
}

/// Whether this binary is already wired into an event's entry list under
/// `matcher` — the same matcher, not just the same event, so an install
/// from before a matcher changed is not mistaken for the current one.
///
/// Matches the pre-rename `<binary> hook` as well as the current
/// `<binary> claude-hook`. Without that, installing over a settings file
/// written before the rename adds a second entry and both fire — the old one
/// still works, because the subcommand keeps `hook` as an alias, so the
/// duplicate would be silent.
fn already_installed(entries: &[Value], command: &str, matcher: Option<&str>) -> bool {
    entries.iter().any(|entry| {
        matcher_of(entry) == matcher
            && entry
                .get("hooks")
                .and_then(Value::as_array)
                .is_some_and(|hooks| hooks.iter().any(|h| is_ours(h, command)))
    })
}

/// Removes this binary's hooks from every entry whose matcher is not
/// `matcher`, drops an entry left with no hooks at all, and returns the
/// matchers it took them from. Anyone else's hooks in those entries stay
/// exactly where they were.
fn take_ours_elsewhere(
    entries: &mut Vec<Value>,
    command: &str,
    matcher: Option<&str>,
) -> Vec<Option<String>> {
    let mut taken = Vec::new();
    entries.retain_mut(|entry| {
        let old = matcher_of(entry).map(ToOwned::to_owned);
        if old.as_deref() == matcher {
            return true;
        }
        let Some(hooks) = entry.get_mut("hooks").and_then(Value::as_array_mut) else {
            return true;
        };
        let before = hooks.len();
        hooks.retain(|h| !is_ours(h, command));
        if hooks.len() == before {
            return true;
        }
        taken.push(old);
        !hooks.is_empty()
    });
    taken
}

fn matcher_of(entry: &Value) -> Option<&str> {
    entry.get("matcher").and_then(Value::as_str)
}

fn is_ours(hook: &Value, command: &str) -> bool {
    let legacy = command
        .strip_suffix(" claude-hook")
        .map(|binary| format!("{binary} hook"));
    let found = hook.get("command").and_then(Value::as_str);
    found == Some(command) || (found.is_some() && found == legacy.as_deref())
}

fn write_settings(path: &Path, settings: &Value) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut text = serde_json::to_string_pretty(settings).context("serialising settings")?;
    text.push('\n');

    // The rename replaces the file, so the new one takes the old one's
    // permissions explicitly; otherwise a settings file kept at 0600 would
    // come back at whatever the umask allows.
    let original = std::fs::metadata(path).ok().map(|m| m.permissions());

    let tmp = dir.join(format!(".settings.json.sealmesh.{}", std::process::id()));
    write_new(&tmp, &text, original.as_ref())
        .with_context(|| format!("writing {}", tmp.display()))?;
    if let Some(permissions) = original
        && let Err(error) = std::fs::set_permissions(&tmp, permissions)
    {
        std::fs::remove_file(&tmp).ok();
        return Err(error).with_context(|| format!("setting permissions on {}", tmp.display()));
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        std::fs::remove_file(&tmp).ok();
        return Err(error).with_context(|| format!("renaming into {}", path.display()));
    }
    Ok(())
}

/// Writes `text` to a new file at `path`, created with `original`'s mode
/// from the start where the platform allows, so a private settings file is
/// never briefly readable through its temp copy. (The umask may narrow the
/// mode at creation; the caller sets it exactly afterwards.)
fn write_new(
    path: &Path,
    text: &str,
    original: Option<&std::fs::Permissions>,
) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        if let Some(original) = original {
            options.mode(original.mode() & 0o777);
        }
    }
    #[cfg(not(unix))]
    let _ = original;
    options.open(path)?.write_all(text.as_bytes())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use super::*;

    const CMD: &str = "/usr/local/bin/sealmesh hook";

    fn realistic() -> Value {
        json!({
            "theme": "dark",
            "model": "opus",
            "agentPushNotifEnabled": true,
            "enabledPlugins": ["example-tools"],
            "hooks": {
                "PreToolUse": [{
                    "matcher": "Bash",
                    "hooks": [
                        {"type": "command", "command": "/Users/work/.claude/hooks/validate-commit-style.sh"},
                        {"type": "command", "command": "/Users/work/.claude/hooks/validate-branch-name.sh"}
                    ]
                }],
                "SessionStart": [{"hooks": [{"type": "command", "command": "/Users/work/.claude/hooks/greet.sh"}]}]
            }
        })
    }

    #[test]
    fn a_fresh_machine_gets_all_four_subscriptions() {
        let Merged {
            settings: after,
            added,
            blocked,
        } = merge(json!({}), CMD);
        assert_eq!(added.len(), 4, "{added:?}");
        assert!(blocked.is_empty(), "{blocked:?}");
        let hooks = after["hooks"].as_object().unwrap();
        for sub in SUBSCRIPTIONS {
            assert!(hooks.contains_key(sub.event), "missing {}", sub.event);
        }
        assert_eq!(after["hooks"]["PreToolUse"][0]["matcher"], json!("*"));
        assert_eq!(
            after["hooks"]["PreToolUse"][0]["hooks"][0]["timeout"],
            json!(5)
        );
        assert!(
            after["hooks"]["Stop"][0]["hooks"][0]
                .get("timeout")
                .is_none()
        );
    }

    /// The `PreToolUse` entry an install before the guardrail wrote.
    fn old_install(command: &str) -> Value {
        let mut settings = realistic();
        let hooks = settings["hooks"].as_object_mut().unwrap();
        for event in ["UserPromptSubmit", "Stop", "SessionEnd"] {
            hooks.insert(
                event.to_owned(),
                json!([{"hooks": [{"type": "command", "command": command}]}]),
            );
        }
        hooks["PreToolUse"].as_array_mut().unwrap().push(json!({
            "matcher": "AskUserQuestion",
            "hooks": [{"type": "command", "command": command}]
        }));
        settings
    }

    #[test]
    fn an_install_from_before_the_guardrail_is_migrated_not_duplicated() {
        for old in [CMD, "/usr/local/bin/sealmesh claude-hook"] {
            let merged = merge(old_install(old), "/usr/local/bin/sealmesh claude-hook");
            assert_eq!(
                merged.added,
                vec!["PreToolUse(*) (replaces PreToolUse(AskUserQuestion))".to_owned()],
                "{old}"
            );
            let pre = merged.settings["hooks"]["PreToolUse"].as_array().unwrap();
            assert_eq!(pre.len(), 2, "{pre:?}");
            assert_eq!(
                pre[0]["matcher"],
                json!("Bash"),
                "the user's own entry moved"
            );
            assert_eq!(pre[1]["matcher"], json!("*"));
            assert!(
                !merged.settings.to_string().contains("AskUserQuestion"),
                "the old matcher survived"
            );

            // And a second run is a no-op.
            let again = merge(
                merged.settings.clone(),
                "/usr/local/bin/sealmesh claude-hook",
            );
            assert!(again.added.is_empty(), "{:?}", again.added);
            assert_eq!(again.settings, merged.settings);
        }
    }

    #[test]
    fn a_migrated_entry_keeps_anyone_elses_hook_that_shared_it() {
        let mut settings = old_install(CMD);
        let pre = settings["hooks"]["PreToolUse"].as_array_mut().unwrap();
        pre[1]["hooks"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type": "command", "command": "/opt/notify.sh"}));
        let merged = merge(settings, CMD);
        let pre = merged.settings["hooks"]["PreToolUse"].as_array().unwrap();
        assert_eq!(pre.len(), 3, "{pre:?}");
        assert_eq!(pre[1]["matcher"], json!("AskUserQuestion"));
        assert_eq!(
            pre[1]["hooks"],
            json!([{"type": "command", "command": "/opt/notify.sh"}])
        );
        assert_eq!(pre[2]["matcher"], json!("*"));
    }

    #[test]
    fn being_installed_means_under_the_same_matcher() {
        let entries = vec![json!({
            "matcher": "AskUserQuestion",
            "hooks": [{"type": "command", "command": CMD}]
        })];
        assert!(already_installed(&entries, CMD, Some("AskUserQuestion")));
        assert!(!already_installed(&entries, CMD, Some("*")));
        assert!(!already_installed(&entries, CMD, None));
    }

    #[test]
    fn nothing_the_user_already_had_is_touched() {
        let before = realistic();
        let after = merge(before.clone(), CMD).settings;

        for key in ["theme", "model", "agentPushNotifEnabled", "enabledPlugins"] {
            assert_eq!(after[key], before[key], "{key} changed");
        }
        assert_eq!(
            after["hooks"]["SessionStart"],
            before["hooks"]["SessionStart"]
        );
        assert_eq!(
            after["hooks"]["PreToolUse"][0],
            before["hooks"]["PreToolUse"][0]
        );
        assert_eq!(after["hooks"]["PreToolUse"].as_array().unwrap().len(), 2);
        assert_eq!(after["hooks"]["PreToolUse"][1]["matcher"], json!("*"));
    }

    #[test]
    fn installing_twice_adds_nothing() {
        let first = merge(realistic(), CMD);
        assert_eq!(first.added.len(), 4);

        let second = merge(first.settings.clone(), CMD);
        assert!(second.added.is_empty(), "{:?}", second.added);
        assert!(second.blocked.is_empty(), "{:?}", second.blocked);
        assert_eq!(
            first.settings, second.settings,
            "a second install changed the file"
        );
    }

    #[test]
    fn a_different_binary_path_is_a_different_install() {
        let once = merge(realistic(), CMD).settings;
        let twice = merge(once, "/opt/sealmesh/bin/sealmesh hook");
        assert_eq!(twice.added.len(), 4, "{:?}", twice.added);
        assert_eq!(twice.settings["hooks"]["Stop"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn an_unexpected_shape_is_left_alone_rather_than_replaced() {
        let odd = json!({ "hooks": { "Stop": "something-else", "UserPromptSubmit": [] } });
        let merged = merge(odd.clone(), CMD);
        assert_eq!(merged.settings["hooks"]["Stop"], odd["hooks"]["Stop"]);
        assert!(
            !merged.added.iter().any(|a| a == "Stop"),
            "{:?}",
            merged.added
        );
        assert_eq!(merged.blocked, vec!["hooks.Stop".to_owned()]);
        assert!(
            merged.added.iter().any(|a| a == "UserPromptSubmit"),
            "{:?}",
            merged.added
        );

        let odder = json!({ "hooks": 42 });
        let merged = merge(odder.clone(), CMD);
        assert_eq!(merged.settings, odder);
        assert!(merged.added.is_empty());
        assert_eq!(merged.blocked, vec!["hooks".to_owned()]);
    }

    #[test]
    fn a_settings_file_that_is_not_an_object_is_reported_not_shrugged_off() {
        for shape in [json!([]), json!("hooks"), json!(42), json!(null)] {
            let merged = merge(shape.clone(), CMD);
            assert!(merged.added.is_empty(), "{shape}");
            assert_eq!(
                merged.blocked,
                vec!["the root of the file".to_owned()],
                "{shape}"
            );
            assert_eq!(merged.settings, shape, "{shape} was modified");
        }
    }

    #[test]
    fn run_fails_on_a_file_it_could_not_install_into() {
        let dir = std::env::temp_dir().join("sealmesh-installhook-notobject");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        std::fs::write(&path, "[]").unwrap();

        let args = Args {
            dry_run: false,
            settings: Some(path.clone()),
        };
        assert!(
            run(&args).is_err(),
            "a file with no hook to add reported success"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "[]");

        std::fs::write(&path, "{}").unwrap();
        run(&args).expect("first install");
        run(&args).expect("second install should be a no-op, not an error");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_settings_file_that_is_not_json_is_an_error_not_an_overwrite() {
        let dir = std::env::temp_dir().join("sealmesh-installhook-badjson");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        std::fs::write(&path, "{\"theme\": \"dark\",}").unwrap();

        assert!(read_settings(&path).is_err());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"theme\": \"dark\",}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_or_empty_settings_file_reads_as_an_empty_object() {
        let dir = std::env::temp_dir().join("sealmesh-installhook-missing");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();

        assert_eq!(read_settings(&dir.join("nope.json")).unwrap(), json!({}));
        let empty = dir.join("empty.json");
        std::fs::write(&empty, "   \n").unwrap();
        assert_eq!(read_settings(&empty).unwrap(), json!({}));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writing_leaves_valid_json_and_no_temp_file() {
        let dir = std::env::temp_dir().join("sealmesh-installhook-write");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");

        let after = merge(realistic(), CMD).settings;
        write_settings(&path, &after).unwrap();

        let back: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(back, after);

        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n != "settings.json")
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_subscriptions_cover_every_event_the_hook_handles() {
        let events: Vec<&str> = SUBSCRIPTIONS.iter().map(|s| s.event).collect();
        for required in ["UserPromptSubmit", "Stop", "PreToolUse", "SessionEnd"] {
            assert!(events.contains(&required), "not subscribed to {required}");
        }
        // The guardrail must see every tool call, not only AskUserQuestion.
        let pre = SUBSCRIPTIONS
            .iter()
            .find(|s| s.event == "PreToolUse")
            .unwrap();
        assert_eq!(pre.matcher, Some("*"));
        assert!(pre.timeout.is_some_and(|t| t <= 10));
    }

    #[test]
    fn a_settings_file_written_before_the_rename_is_not_installed_over_twice() {
        // The subcommand keeps `hook` as an alias, so a duplicate entry would
        // fire twice and never complain.
        let entries = vec![serde_json::json!({
            "hooks": [{"type": "command", "command": "/usr/local/bin/sealmesh hook"}]
        })];
        assert!(already_installed(
            &entries,
            "/usr/local/bin/sealmesh claude-hook",
            None
        ));
        assert!(
            !already_installed(&entries, "/opt/other/sealmesh claude-hook", None),
            "a different binary's old entry counted as this one's"
        );
    }

    #[test]
    fn rewriting_settings_keeps_the_files_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir =
            std::env::temp_dir().join(format!("sealmesh-installhook-mode-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");

        for mode in [0o600, 0o640, 0o644] {
            std::fs::write(&path, "{}").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            write_settings(&path, &merge(json!({}), CMD).settings).unwrap();
            let after = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(after, mode, "{mode:o} became {after:o}");
        }

        std::fs::remove_dir_all(&dir).ok();
    }
}
