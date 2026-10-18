use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};

const SUBSCRIPTIONS: &[(&str, Option<&str>)] = &[
    ("UserPromptSubmit", None),
    ("Stop", None),
    ("PreToolUse", Some("AskUserQuestion")),
    ("SessionEnd", None),
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

    for (event, matcher) in SUBSCRIPTIONS {
        let entries = hooks.entry(*event).or_insert_with(|| json!([]));
        let Some(entries) = entries.as_array_mut() else {
            blocked.push(format!("hooks.{event}"));
            continue;
        };

        if already_installed(entries, command) {
            continue;
        }

        let mut entry = serde_json::Map::new();
        if let Some(matcher) = matcher {
            entry.insert("matcher".to_owned(), json!(matcher));
        }
        entry.insert(
            "hooks".to_owned(),
            json!([{ "type": "command", "command": command }]),
        );
        entries.push(Value::Object(entry));
        added.push(match matcher {
            Some(m) => format!("{event}({m})"),
            None => (*event).to_owned(),
        });
    }

    Merged {
        settings,
        added,
        blocked,
    }
}

/// Whether this binary is already wired into an event's entry list.
///
/// Matches the pre-rename `<binary> hook` as well as the current
/// `<binary> claude-hook`. Without that, installing over a settings file
/// written before the rename adds a second entry and both fire — the old one
/// still works, because the subcommand keeps `hook` as an alias, so the
/// duplicate would be silent.
fn already_installed(entries: &[Value], command: &str) -> bool {
    let legacy = command
        .strip_suffix(" claude-hook")
        .map(|binary| format!("{binary} hook"));
    entries.iter().any(|entry| {
        entry
            .get("hooks")
            .and_then(Value::as_array)
            .is_some_and(|hooks| {
                hooks.iter().any(|h| {
                    let found = h.get("command").and_then(Value::as_str);
                    found == Some(command) || (found.is_some() && found == legacy.as_deref())
                })
            })
    })
}

fn write_settings(path: &Path, settings: &Value) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut text = serde_json::to_string_pretty(settings).context("serialising settings")?;
    text.push('\n');

    let tmp = dir.join(format!(".settings.json.sealmesh.{}", std::process::id()));
    std::fs::write(&tmp, &text).with_context(|| format!("writing {}", tmp.display()))?;
    if let Err(error) = std::fs::rename(&tmp, path) {
        std::fs::remove_file(&tmp).ok();
        return Err(error).with_context(|| format!("renaming into {}", path.display()));
    }
    Ok(())
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
        for (event, _) in SUBSCRIPTIONS {
            assert!(hooks.contains_key(*event), "missing {event}");
        }
        assert_eq!(
            after["hooks"]["PreToolUse"][0]["matcher"],
            json!("AskUserQuestion")
        );
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
        assert_eq!(
            after["hooks"]["PreToolUse"][1]["matcher"],
            json!("AskUserQuestion")
        );
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
        let events: Vec<&str> = SUBSCRIPTIONS.iter().map(|(e, _)| *e).collect();
        for required in ["UserPromptSubmit", "Stop", "PreToolUse", "SessionEnd"] {
            assert!(events.contains(&required), "not subscribed to {required}");
        }
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
            "/usr/local/bin/sealmesh claude-hook"
        ));
        assert!(
            !already_installed(&entries, "/opt/other/sealmesh claude-hook"),
            "a different binary's old entry counted as this one's"
        );
    }
}
