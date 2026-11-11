//! The welcome screen bare `sealmesh` prints: the seal balancing its mesh,
//! the wordmark, the version, and three lines about this machine.
//!
//! Everything here is local and quick. The status lines read the config file,
//! knock on the daemon's local socket, and stat the guardrail policy; nothing
//! touches the network, so the screen draws in a few milliseconds whether or
//! not a router is reachable.
//!
//! The layout follows the terminal. Wide (72 columns and up) puts the
//! wordmark beside the seal, narrower stacks it underneath, and anything
//! narrower still, piped, or `TERM=dumb` gets the compact text with no art and
//! no escape codes. Art is never wrapped mid-line. `NO_COLOR` keeps the art
//! and drops the escape codes; color is only bold and dim.

use std::{
    ffi::OsStr,
    io::{IsTerminal as _, Write as _},
    path::Path,
};

use anyhow::Result;

use crate::{
    config::{Config, Mode},
    policy, render, room,
};

/// The seal, in profile with its nose up, balancing a ball that is three
/// linked nodes. Trailing spaces are never part of a row.
const SEAL: [&str; 12] = [
    "                    ●───●",
    "                     ╲ ╱",
    "                      ●",
    "                ▄███▄",
    "               ▄███▀██",
    "               ████▀",
    "             ▄█████",
    "           ▄███████",
    "       ▄▄█████████▀",
    "▄▄▄▄▄█████████████",
    " █████████████████▄▄",
    "▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀",
];

/// "SEALMESH", drawn here rather than taken from a figlet font.
const WORDMARK: [&str; 5] = [
    "▄▀▀▀ █▀▀▀ ▄▀▀▄ █    █▄ ▄█ █▀▀▀ ▄▀▀▀ █  █",
    "█    █    █  █ █    █ ▀ █ █    █    █  █",
    " ▀▀▄ █▀▀  █▀▀█ █    █   █ █▀▀   ▀▀▄ █▀▀█",
    "   █ █    █  █ █    █   █ █       █ █  █",
    "▄▄▄▀ █▄▄▄ █  █ █▄▄▄ █   █ █▄▄▄ ▄▄▄▀ █  █",
];

/// The seal row the wordmark's first row sits beside, so its baseline meets
/// the seal's.
const WORDMARK_ROW: usize = 6;

/// Spaces between the seal's widest row and the wordmark.
const GUTTER: usize = 3;

/// The narrowest terminal that gets the wordmark beside the seal.
pub const WIDE_MIN_COLUMNS: usize = 72;

/// What a terminal is assumed to be when its width can't be read.
const DEFAULT_COLUMNS: usize = 80;

const TAGLINE: &str = "see every agent, live";
const HINT: &str = "sealmesh who · sealmesh doctor · sealmesh --help";

/// How much of the screen to draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Wordmark beside the seal.
    Wide,
    /// Wordmark under the seal.
    Stacked,
    /// No art: the version line, the status lines and the hint.
    Compact,
}

/// The three lines about this machine, already worded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub mode: String,
    pub daemon: String,
    pub guardrail: String,
}

impl Status {
    /// Reads this machine's state: the config at `explicit` (or the user
    /// config), the daemon's local socket, and the guardrail policy. Never
    /// fails; a missing home, config or socket is a line, not an error.
    pub fn read(explicit: Option<&Path>) -> Self {
        Self {
            mode: mode_line(explicit),
            daemon: daemon_line(room::socket::default_path().as_deref()),
            guardrail: policy::status_line(&policy::load(
                Path::new(policy::POLICY_PATH),
                policy::TRUSTED_OWNER,
            )),
        }
    }
}

/// Prints the welcome screen for this terminal.
pub fn run(config: Option<&Path>) -> Result<()> {
    let stdout = std::io::stdout();
    let (layout, color) = choose(
        stdout.is_terminal(),
        std::env::var_os("NO_COLOR").as_deref(),
        std::env::var_os("TERM").as_deref(),
        columns(),
    );
    let text = render(
        layout,
        color,
        env!("CARGO_PKG_VERSION"),
        &Status::read(config),
    );
    match stdout.lock().write_all(text.as_bytes()) {
        // `sealmesh | head -1` is not an error worth a message.
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => Ok(other?),
    }
}

/// Picks the layout and whether to color it. Pure, so every combination is
/// testable without a terminal.
pub fn choose(
    is_terminal: bool,
    no_color: Option<&OsStr>,
    term: Option<&OsStr>,
    columns: Option<usize>,
) -> (Layout, bool) {
    if !is_terminal || term == Some(OsStr::new("dumb")) {
        return (Layout::Compact, false);
    }
    let color = render::styling_allowed_in(no_color, term);
    let columns = columns.unwrap_or(DEFAULT_COLUMNS);
    let layout = if columns >= WIDE_MIN_COLUMNS.max(wide_width()) {
        Layout::Wide
    } else if columns >= stacked_width() {
        Layout::Stacked
    } else {
        Layout::Compact
    };
    (layout, color)
}

/// The whole screen, newline-terminated.
pub fn render(layout: Layout, color: bool, version: &str, status: &Status) -> String {
    let paint = Paint(color);
    let mut lines: Vec<String> = Vec::new();
    match layout {
        Layout::Wide => {
            let seal_width = seal_width();
            for (row, seal) in SEAL.iter().enumerate() {
                let word = row
                    .checked_sub(WORDMARK_ROW)
                    .and_then(|index| WORDMARK.get(index));
                lines.push(match word {
                    Some(word) => {
                        let pad = seal_width - width(seal) + GUTTER;
                        format!("{seal}{}{}", " ".repeat(pad), paint.bold(word))
                    }
                    None => (*seal).to_owned(),
                });
            }
        }
        Layout::Stacked => {
            lines.extend(SEAL.iter().map(|seal| (*seal).to_owned()));
            lines.push(String::new());
            lines.extend(WORDMARK.iter().map(|word| paint.bold(word)));
        }
        Layout::Compact => {}
    }
    let art = layout != Layout::Compact;
    if art {
        lines.push(String::new());
    }
    lines.push(format!(
        "Sealmesh v{version}{}",
        paint.dim(&format!(" · {TAGLINE}"))
    ));
    if art {
        lines.push(String::new());
    }
    for (label, value) in [
        ("mode", &status.mode),
        ("daemon", &status.daemon),
        ("guardrail", &status.guardrail),
    ] {
        lines.push(format!("{}{value}", paint.dim(&format!("{label:<11}"))));
    }
    if art {
        lines.push(String::new());
    }
    lines.push(paint.dim(HINT));

    let mut out = lines.join("\n");
    out.push('\n');
    out
}

/// The terminal's width: `COLUMNS` when it is set to a number, else what the
/// terminal on stdout reports, else nothing.
fn columns() -> Option<usize> {
    let from_env = std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|&n| n > 0);
    from_env.or_else(|| {
        rustix::termios::tcgetwinsize(std::io::stdout())
            .ok()
            .map(|size| usize::from(size.ws_col))
            .filter(|&n| n > 0)
    })
}

fn mode_line(explicit: Option<&Path>) -> String {
    let path = match explicit {
        Some(path) => Some(path.to_path_buf()),
        None => Config::user_path(),
    };
    if !path.is_some_and(|path| path.is_file()) {
        return "no config yet — run `sealmesh doctor`".to_owned();
    }
    match Config::load(explicit) {
        Ok(config) => mode_text(config.mode()).to_owned(),
        Err(_) => "the config doesn't load — run `sealmesh doctor`".to_owned(),
    }
}

fn mode_text(mode: Mode) -> &'static str {
    match mode {
        Mode::Std => "std (router on this machine)",
        Mode::Otw => "otw (router on your tailnet)",
        Mode::Cloud => "cloud (remote router, GitHub identity)",
    }
}

/// Whether the daemon answers on its local socket. A Unix-socket connect
/// either succeeds or is refused at once, so this never waits.
fn daemon_line(socket: Option<&Path>) -> String {
    match socket {
        None => "unknown (no home directory)".to_owned(),
        Some(path) if std::os::unix::net::UnixStream::connect(path).is_ok() => "running".to_owned(),
        Some(_) => "not running (start it with `sealmesh daemon`)".to_owned(),
    }
}

fn width(line: &str) -> usize {
    line.chars().count()
}

fn seal_width() -> usize {
    SEAL.iter().map(|line| width(line)).max().unwrap_or(0)
}

fn wordmark_width() -> usize {
    WORDMARK.iter().map(|line| width(line)).max().unwrap_or(0)
}

fn wide_width() -> usize {
    seal_width() + GUTTER + wordmark_width()
}

fn stacked_width() -> usize {
    seal_width().max(wordmark_width())
}

/// Bold and dim, or nothing.
struct Paint(bool);

impl Paint {
    fn bold(&self, text: &str) -> String {
        self.sgr("1", text)
    }

    fn dim(&self, text: &str) -> String {
        self.sgr("2", text)
    }

    fn sgr(&self, code: &str, text: &str) -> String {
        if self.0 {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_owned()
        }
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

    use super::*;

    fn status() -> Status {
        Status {
            mode: "std (router on this machine)".to_owned(),
            daemon: "not running (start it with `sealmesh daemon`)".to_owned(),
            guardrail: "off (no policy file)".to_owned(),
        }
    }

    fn os(value: &str) -> Option<&OsStr> {
        Some(OsStr::new(value))
    }

    /// Drops every `ESC [ ... m` sequence.
    fn strip_sgr(text: &str) -> String {
        let mut out = String::new();
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c in chars.by_ref() {
                    if c == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    const WIDE: &str = "                    ●───●
                     ╲ ╱
                      ●
                ▄███▄
               ▄███▀██
               ████▀
             ▄█████         ▄▀▀▀ █▀▀▀ ▄▀▀▄ █    █▄ ▄█ █▀▀▀ ▄▀▀▀ █  █
           ▄███████         █    █    █  █ █    █ ▀ █ █    █    █  █
       ▄▄█████████▀          ▀▀▄ █▀▀  █▀▀█ █    █   █ █▀▀   ▀▀▄ █▀▀█
▄▄▄▄▄█████████████             █ █    █  █ █    █   █ █       █ █  █
 █████████████████▄▄        ▄▄▄▀ █▄▄▄ █  █ █▄▄▄ █   █ █▄▄▄ ▄▄▄▀ █  █
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

Sealmesh v0.2.0 · see every agent, live

mode       std (router on this machine)
daemon     not running (start it with `sealmesh daemon`)
guardrail  off (no policy file)

sealmesh who · sealmesh doctor · sealmesh --help
";

    const STACKED: &str = "                    ●───●
                     ╲ ╱
                      ●
                ▄███▄
               ▄███▀██
               ████▀
             ▄█████
           ▄███████
       ▄▄█████████▀
▄▄▄▄▄█████████████
 █████████████████▄▄
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

▄▀▀▀ █▀▀▀ ▄▀▀▄ █    █▄ ▄█ █▀▀▀ ▄▀▀▀ █  █
█    █    █  █ █    █ ▀ █ █    █    █  █
 ▀▀▄ █▀▀  █▀▀█ █    █   █ █▀▀   ▀▀▄ █▀▀█
   █ █    █  █ █    █   █ █       █ █  █
▄▄▄▀ █▄▄▄ █  █ █▄▄▄ █   █ █▄▄▄ ▄▄▄▀ █  █

Sealmesh v0.2.0 · see every agent, live

mode       std (router on this machine)
daemon     not running (start it with `sealmesh daemon`)
guardrail  off (no policy file)

sealmesh who · sealmesh doctor · sealmesh --help
";

    const COMPACT: &str = "Sealmesh v0.2.0 · see every agent, live
mode       std (router on this machine)
daemon     not running (start it with `sealmesh daemon`)
guardrail  off (no policy file)
sealmesh who · sealmesh doctor · sealmesh --help
";

    #[test]
    fn the_wide_banner_without_color_is_exactly_this() {
        assert_eq!(render(Layout::Wide, false, "0.2.0", &status()), WIDE);
    }

    #[test]
    fn a_narrow_terminal_stacks_the_wordmark_under_the_seal() {
        assert_eq!(render(Layout::Stacked, false, "0.2.0", &status()), STACKED);
    }

    #[test]
    fn the_compact_output_has_no_art_and_no_escape_codes() {
        assert_eq!(render(Layout::Compact, false, "0.2.0", &status()), COMPACT);
    }

    #[test]
    fn color_is_only_bold_and_dim_around_the_same_text() {
        for layout in [Layout::Wide, Layout::Stacked, Layout::Compact] {
            let plain = render(layout, false, "0.2.0", &status());
            let colored = render(layout, true, "0.2.0", &status());
            assert_ne!(plain, colored, "{layout:?} has no styling at all");
            assert_eq!(strip_sgr(&colored), plain, "{layout:?}");
            for sequence in colored.split('\x1b').skip(1) {
                assert!(
                    ["[1m", "[2m", "[0m"]
                        .iter()
                        .any(|code| sequence.starts_with(code)),
                    "{layout:?} uses something other than bold or dim: {sequence:?}"
                );
            }
        }
        let wide = render(Layout::Wide, true, "0.2.0", &status());
        assert!(wide.contains(&format!("\x1b[1m{}\x1b[0m", WORDMARK[0])));
        assert!(wide.contains("\x1b[2m · see every agent, live\x1b[0m"));
    }

    #[test]
    fn piped_or_dumb_output_is_compact_and_plain() {
        assert_eq!(
            choose(false, None, os("xterm-256color"), Some(200)),
            (Layout::Compact, false)
        );
        assert_eq!(
            choose(true, None, os("dumb"), Some(200)),
            (Layout::Compact, false)
        );
        assert_eq!(choose(false, None, None, None), (Layout::Compact, false));
    }

    #[test]
    fn no_color_keeps_the_art_and_strips_every_escape_code() {
        let (layout, color) = choose(true, os("1"), os("xterm-256color"), Some(100));
        assert_eq!((layout, color), (Layout::Wide, false));
        let text = render(layout, color, "0.2.0", &status());
        assert!(!text.contains('\x1b'));
        assert!(text.contains(SEAL[0]) && text.contains(WORDMARK[0]));
        // An empty NO_COLOR is unset, by the convention.
        assert_eq!(
            choose(true, os(""), os("xterm"), Some(100)),
            (Layout::Wide, true)
        );
    }

    #[test]
    fn the_layout_follows_the_width_and_never_overflows_it() {
        let term = os("xterm-256color");
        assert_eq!(choose(true, None, term, Some(72)).0, Layout::Wide);
        assert_eq!(choose(true, None, term, Some(71)).0, Layout::Stacked);
        assert_eq!(
            choose(true, None, term, Some(stacked_width())).0,
            Layout::Stacked
        );
        assert_eq!(
            choose(true, None, term, Some(stacked_width() - 1)).0,
            Layout::Compact
        );
        assert_eq!(
            choose(true, None, term, None).0,
            Layout::Wide,
            "unknown width"
        );

        for columns in 1..=200 {
            let (layout, _) = choose(true, None, term, Some(columns));
            if layout == Layout::Compact {
                continue;
            }
            let text = render(layout, false, "0.2.0", &status());
            for line in text.lines().filter(|line| line.contains(['█', '●'])) {
                assert!(width(line) <= columns, "{columns} columns: {line:?}");
            }
        }
    }

    #[test]
    fn the_art_has_no_trailing_spaces_and_fits_its_budget() {
        for line in SEAL.iter().chain(WORDMARK.iter()) {
            assert_eq!(line.trim_end(), *line);
        }
        assert!(wide_width() <= WIDE_MIN_COLUMNS, "{}", wide_width());
        assert!((8..=12).contains(&SEAL.len()));
        assert!(WORDMARK_ROW + WORDMARK.len() <= SEAL.len());
    }

    #[test]
    fn a_missing_config_or_socket_is_a_line_not_an_error() {
        let dir = std::env::temp_dir().join(format!("sealmesh-banner-{}", std::process::id()));
        let missing = dir.join("nope.toml");
        assert_eq!(
            mode_line(Some(&missing)),
            "no config yet — run `sealmesh doctor`"
        );
        assert_eq!(
            daemon_line(Some(&dir.join("daemon.sock"))),
            "not running (start it with `sealmesh daemon`)"
        );
        assert_eq!(daemon_line(None), "unknown (no home directory)");
        let status = Status::read(Some(&missing));
        assert!(!status.guardrail.is_empty());
    }

    #[test]
    fn the_mode_comes_from_the_config_and_a_broken_one_points_at_doctor() {
        let dir = std::env::temp_dir().join(format!("sealmesh-banner-mode-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("std.toml");
        std::fs::write(&good, "[mesh]\nmode = \"std\"\n").unwrap();
        let bad = dir.join("bad.toml");
        std::fs::write(&bad, "[mesh]\nmode = \"lan\"\n").unwrap();
        assert_eq!(mode_line(Some(&good)), "std (router on this machine)");
        assert_eq!(
            mode_line(Some(&bad)),
            "the config doesn't load — run `sealmesh doctor`"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_listening_daemon_socket_reads_as_running() {
        let dir = std::env::temp_dir().join(format!("sm-banner-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        assert_eq!(daemon_line(Some(&path)), "running");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
