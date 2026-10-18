use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use sealmesh::{
    audit, chat,
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
▄▀█ █▀▀ █▀▀ █▄ █ ▀█▀ █ █ █ ▄▀█ ▀█▀ █▀▀ █ █
█▀█ █▄█ ██▄ █ ▀█  █  ▀▄▀▄▀ █▀█  █  █▄▄ █▀█
──────────────────────────────────────────
read-only · answered live · nothing kept";

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

    #[command(subcommand)]
    command: Command,
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
    match cli.command {
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
    }
}
