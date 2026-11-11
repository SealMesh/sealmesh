use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use sealmesh::{
    audit, banner, chat,
    config::Config,
    daemon, declare, doctor, hook, install_hook,
    room::{room_name, word},
    who,
};

/// Printed above `sealmesh --help`, and nowhere else. `before_help` is not
/// propagated to subcommands, so it stays out of `sealmesh who --help` and
/// off every error path. Flush left and with no trailing newline: clap trims
/// the first line's indent, and adds its own blank line before `about`.
const BANNER: &str = "\
█▀ █▀▀ ▄▀█ █   █▀▄▀█ █▀▀ █▀ █ █
▄█ ██▄ █▀█ █▄▄ █ ▀ █ ██▄ ▄█ █▀█
───────────────────────────────
read-only · answered live · no agent data stored";

#[derive(Parser)]
#[command(
    name = "sealmesh",
    about = "See what the team's agents are working on",
    before_help = BANNER,
    version
)]
struct Cli {
    /// Read config from PATH instead of `~/.config/sealmesh/config.toml`.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Optional: bare `sealmesh` prints the welcome screen and exits 0.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Every agent on every connected machine, answered live.
    Who,
    /// Answer for this machine.
    Daemon {
        #[arg(long)]
        dry_run: bool,
    },
    /// Every job this machine saw, and why each was answered with or hidden.
    Doctor,
    /// Who has asked this machine about its agents, and when.
    Audit,
    /// Record an agent's state from outside the collector.
    Declare {
        #[arg(long, value_name = "ID")]
        id: String,
        #[arg(long, value_name = "PATH")]
        worktree: Option<PathBuf>,
        #[arg(long, value_name = "STATE")]
        state: Option<String>,
        #[arg(long, value_name = "REF")]
        branch: Option<String>,
        #[arg(long, value_name = "N")]
        pr: Option<u32>,
        #[arg(long, value_name = "DONE/TOTAL")]
        tasks: Option<String>,
        #[arg(long, value_name = "SESSION_ID")]
        parent: Option<String>,
        #[arg(long, value_name = "TOOL")]
        tool: Option<String>,
        #[arg(long, conflicts_with_all = ["worktree", "state", "branch", "pr", "tasks", "parent", "tool"])]
        end: bool,
    },
    /// Translate a Claude Code hook payload into a declared record.
    ///
    /// `alias = "hook"` is not deprecation politeness. An installed hook lives
    /// in the user's `settings.json` and keeps calling whatever name was
    /// written there; without the alias, clap would reject the old name with a
    /// non-zero exit, and a `UserPromptSubmit` hook that exits non-zero blocks
    /// the person typing. A rename here would wedge the editor of anyone who
    /// upgraded before re-running install.
    #[command(name = "claude-hook", alias = "hook")]
    ClaudeHook,
    /// Install the Claude Code hook into this machine's `settings.json`.
    #[command(name = "install-claude-hook", alias = "install-hook")]
    InstallClaudeHook {
        #[arg(long)]
        dry_run: bool,
        #[arg(long, value_name = "PATH")]
        settings: Option<PathBuf>,
    },
    /// Read-only: list live rooms, or tail one. Never posts — a human reads
    /// rooms here and writes into them nowhere.
    Chat {
        /// A ticket (`XYZ-62`), a repo (`owner/name`), or `project` for the
        /// current checkout's project room. Omit to list rooms — only ones
        /// with members right now; a room with stored history and nobody in
        /// it is not listed, and reading it still works by naming it
        /// directly.
        #[arg(value_name = "SCOPE")]
        scope: Option<String>,
        /// Read the room's stored history after this sequence number first,
        /// paginated, then tail it live. Needs a scope; meaningless when
        /// listing.
        #[arg(long, value_name = "SEQ", requires = "scope")]
        since: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let Some(command) = cli.command else {
        return banner::run(cli.config.as_deref());
    };
    match command {
        Command::Who => {
            let config = Config::load(cli.config.as_deref())?;
            who::run(&config).await
        }
        Command::Daemon { dry_run } => {
            let config = Config::load(cli.config.as_deref())?;
            if dry_run {
                daemon::dry_run(&config)
            } else {
                daemon::run(&config).await
            }
        }
        Command::Doctor => {
            let config = Config::load(cli.config.as_deref())?;
            doctor::run(&config).await
        }
        Command::Audit => show_audit(),
        Command::Declare {
            id,
            worktree,
            state,
            branch,
            pr,
            tasks,
            parent,
            tool,
            end,
        } => declare::run(&declare::Args {
            id,
            worktree,
            state,
            branch,
            pr,
            tasks,
            parent,
            tool,
            end,
        }),
        Command::ClaudeHook => hook::run().await,
        Command::InstallClaudeHook { dry_run, settings } => {
            install_hook::run(&install_hook::Args { dry_run, settings })
        }
        Command::Chat { scope, since } => chat::run(scope, since).await,
    }
}

fn show_audit() -> Result<()> {
    let Some(path) = audit::path() else {
        println!("no home directory, so there is no audit log");
        return Ok(());
    };
    let records = audit::read_all(&path)?;
    if records.is_empty() {
        println!("nothing recorded yet ({})", path.display());
        return Ok(());
    }
    println!(
        "Names are the router's attribution and are not verified by this machine — \
         end-to-end signing by the reader would make them so, and does not exist yet. \
         Times, counts, and refusals are this machine's own.\n"
    );
    for record in records {
        match record {
            audit::Record::Query(entry) => {
                let answered = match (entry.refused, entry.agents) {
                    (true, _) => "refused — over this machine's rate limit".to_owned(),
                    (false, Some(n)) => format!("{n} agents"),
                    (false, None) => "not sharing".to_owned(),
                };
                println!("{}  {} (claimed)  {answered}", entry.at, entry.asked_by);
            }
            audit::Record::Room(room) => println!("{}", describe_room(&room)),
            audit::Record::Guardrail(guardrail) => println!("{}", guardrail.describe()),
        }
    }
    Ok(())
}

fn describe_room(record: &audit::RoomRecord) -> String {
    use audit::RoomRecord;
    match record {
        RoomRecord::PostRefused { at, room, category } => format!(
            "{at}  room {}  a post from this machine was refused: {}",
            room_name(room),
            word(category)
        ),
        RoomRecord::BackfillRefused { at, room, category } => format!(
            "{at}  room {}  a history read was refused: {}",
            room_name(room),
            word(category)
        ),
        RoomRecord::Received {
            at,
            since,
            room,
            from,
            messages,
        } => format!(
            "{at}  room {}  {messages} messages from {from} (claimed) since {since}",
            room_name(room)
        ),
        RoomRecord::ReceivedElsewhere {
            at,
            since,
            messages,
        } => format!("{at}  {messages} more room messages since {since}, not itemised"),
        RoomRecord::RefusalsNotItemised {
            at,
            since,
            refusals,
        } => format!("{at}  {refusals} more refusals since {since}, not itemised"),
        RoomRecord::AgentPost {
            at,
            agent,
            room,
            seq,
            refused,
        } => match (seq, refused) {
            (_, Some(refusal)) => format!(
                "{at}  room {}  agent {agent} (agents.sock) post refused: {}",
                room_name(room),
                refusal.word()
            ),
            (Some(seq), None) => format!(
                "{at}  room {}  agent {agent} (agents.sock) posted seq {}",
                room_name(room),
                seq.0
            ),
            (None, None) => format!(
                "{at}  room {}  agent {agent} (agents.sock) posted",
                room_name(room)
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use clap::{CommandFactory as _, error::ErrorKind};

    use super::*;

    /// Text only the welcome screen prints.
    fn has_welcome(text: &str) -> bool {
        text.contains("see every agent, live") || text.contains('●')
    }

    #[test]
    fn the_cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn bare_sealmesh_parses_to_no_command() {
        let cli = Cli::try_parse_from(["sealmesh"]).unwrap();
        assert!(cli.command.is_none());
        let cli = Cli::try_parse_from(["sealmesh", "--config", "x.toml"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.config, Some(PathBuf::from("x.toml")));
    }

    #[test]
    fn help_and_version_do_not_print_the_welcome_screen() {
        let help = Cli::try_parse_from(["sealmesh", "--help"])
            .err()
            .expect("--help exits through clap");
        assert_eq!(help.kind(), ErrorKind::DisplayHelp);
        let help = help.render().to_string();
        assert!(help.contains(BANNER), "the --help header is unchanged");
        assert!(!has_welcome(&help), "{help}");

        let version = Cli::try_parse_from(["sealmesh", "--version"])
            .err()
            .expect("--version exits through clap");
        assert_eq!(version.kind(), ErrorKind::DisplayVersion);
        let version = version.render().to_string();
        assert_eq!(version, format!("sealmesh {}\n", env!("CARGO_PKG_VERSION")));

        let sub_help = Cli::try_parse_from(["sealmesh", "who", "--help"])
            .err()
            .expect("who --help exits through clap");
        assert!(!has_welcome(&sub_help.render().to_string()));
    }

    #[test]
    fn a_subcommand_still_runs_as_itself() {
        let cli = Cli::try_parse_from(["sealmesh", "who"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Who)));
        let cli = Cli::try_parse_from(["sealmesh", "doctor"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Doctor)));
        let cli = Cli::try_parse_from(["sealmesh", "hook"]).unwrap();
        assert!(matches!(cli.command, Some(Command::ClaudeHook)));
        assert!(Cli::try_parse_from(["sealmesh", "nope"]).is_err());
    }
}
