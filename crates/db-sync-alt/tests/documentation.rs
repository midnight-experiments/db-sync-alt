//! The documentation says what the binary does. These tests compare
//! `docs/configuration.md` with the `--help` of every command, the exit
//! statuses of `docs/operations.md` with the ones the binary prints, and the
//! `/status` fields and `/health` reason codes of that page with the status
//! document.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
};

use dsa_service::status::{self, Serving, Settings, State, Status};
use dsa_store::service_view::ReaderCheck;
use serde_json::Value;

/// Commands a page may describe before the binary has them. Each is compared
/// with its `--help` as soon as the binary has it.
const PENDING: &[&str] = &["run"];
/// A hidden command whose `--help` covers flag groups of a pending command.
const HIDDEN: &[&str] = &["follow"];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The `--help` of `command`, or `None` when the binary has no such command.
/// No `DSA_` variable of the test's environment reaches the binary.
fn help(command: Option<&str>) -> Option<String> {
    let mut process = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    process.args(command).arg("--help");
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
        process.env_remove(name);
    }
    let output = process.output().expect("the binary runs");
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).expect("help is UTF-8"))
}

/// The lines of a block that starts with the line `title` and ends at the
/// next line that is not indented.
fn block<'a>(text: &'a str, title: &str) -> Vec<&'a str> {
    text.lines()
        .skip_while(|line| *line != title)
        .skip(1)
        .take_while(|line| line.is_empty() || line.starts_with(' '))
        .collect()
}

/// A flag as `--help` prints it, or as a table of the documentation states it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Flag {
    variable: Option<String>,
    default: Option<String>,
}

fn bracket(text: &str, key: &str) -> Option<String> {
    let start = text.find(key)? + key.len();
    let end = text[start..].find(']')? + start;
    Some(text[start..end].to_owned())
}

/// The flags of one `--help`, without `--help` and `--version`.
fn help_flags(help: &str) -> BTreeMap<String, Flag> {
    let mut items: Vec<(String, String)> = Vec::new();
    for line in block(help, "Options:") {
        let trimmed = line.trim();
        if trimmed.starts_with('-') {
            let name = trimmed
                .split([' ', ','])
                .find(|word| word.starts_with("--"))
                .expect("an option line names a long flag");
            items.push((name.to_owned(), trimmed.to_owned()));
        } else if let Some((_, text)) = items.last_mut() {
            text.push(' ');
            text.push_str(trimmed);
        }
    }
    items
        .into_iter()
        .filter(|(name, _)| name != "--help" && name != "--version")
        .map(|(name, text)| {
            let variable = bracket(&text, "[env: ").map(|value| {
                value
                    .chars()
                    .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                    .collect()
            });
            let default = bracket(&text, "[default: ");
            (name, Flag { variable, default })
        })
        .collect()
}

/// The commands `db-sync-alt --help` lists, without `help`.
fn listed_commands(help: &str) -> Vec<String> {
    block(help, "Commands:")
        .iter()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_owned)
        .collect()
}

/// GitHub's anchor of a heading.
fn anchor(title: &str) -> String {
    title
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
        .map(|c| if c == ' ' { '-' } else { c })
        .collect()
}

fn cells(line: &str) -> Vec<String> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_owned())
        .collect()
}

fn unquoted(cell: &str) -> String {
    cell.replace('`', "")
}

/// Text without backticks and with single spaces, for comparing defaults and
/// sentences.
fn plain(text: &str) -> String {
    unquoted(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// One `## ` section of a page: its title, its text and its table rows.
struct Section {
    title: String,
    text: String,
}

fn sections(page: &str, level: &str) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    for line in page.lines() {
        if let Some(title) = line.strip_prefix(level) {
            sections.push(Section {
                title: title.trim().to_owned(),
                text: String::new(),
            });
        } else if let Some(section) = sections.last_mut() {
            section.text.push_str(line);
            section.text.push('\n');
        }
    }
    sections
}

fn section<'a>(sections: &'a [Section], title: &str) -> &'a Section {
    sections
        .iter()
        .find(|section| section.title == title)
        .unwrap_or_else(|| panic!("no section {title:?}"))
}

/// The flag rows of a section's tables.
fn flag_rows(section: &Section) -> BTreeMap<String, (Flag, String)> {
    let mut rows = BTreeMap::new();
    for line in section
        .text
        .lines()
        .filter(|line| line.starts_with("| `--"))
    {
        let cells = cells(line);
        assert!(cells.len() >= 4, "a flag row has four cells: {line}");
        let variable = match cells[1].as_str() {
            "—" => None,
            cell => Some(unquoted(cell)),
        };
        let flag = Flag {
            variable,
            default: Some(cells[2].clone()),
        };
        let name = unquoted(&cells[0]);
        assert!(
            rows.insert(name.clone(), (flag, cells[3].clone()))
                .is_none(),
            "{name} twice in {:?}",
            section.title
        );
    }
    rows
}

/// Compares one documented flag with `--help`: the same variable, and the
/// default `--help` prints within the documented one.
fn compare_flag(
    context: &str,
    name: &str,
    documented: &Flag,
    printed: &Flag,
    errors: &mut Vec<String>,
) {
    if documented.variable != printed.variable {
        errors.push(format!(
            "{context} {name}: the page says variable {:?}, --help {:?}",
            documented.variable, printed.variable
        ));
    }
    if let Some(default) = &printed.default {
        let page = plain(documented.default.as_deref().unwrap_or_default());
        if !page.contains(&plain(default)) {
            errors.push(format!(
                "{context} {name}: the page says default {page:?}, --help {default:?}"
            ));
        }
    }
}

#[test]
fn the_configuration_page_states_every_flag_of_every_command() {
    let top = help(None).expect("db-sync-alt --help");
    let listed = listed_commands(&top);
    assert!(listed.len() >= 10, "{listed:?}");
    let page = read("docs/configuration.md");
    let sections = sections(&page, "## ");
    let groups: BTreeMap<String, &Section> = sections
        .iter()
        .filter(|section| section.title.ends_with(" flags"))
        .map(|section| (anchor(&section.title), section))
        .collect();
    let commands: BTreeMap<String, &Section> = sections
        .iter()
        .filter(|section| section.title.starts_with('`') && section.title.ends_with('`'))
        .map(|section| (section.title.trim_matches('`').to_owned(), section))
        .collect();
    let linked = |section: &Section| -> Vec<String> {
        groups
            .keys()
            .filter(|group| section.text.contains(&format!("](#{group})")))
            .cloned()
            .collect()
    };
    let mut errors = Vec::new();
    for command in &listed {
        if !commands.contains_key(command) {
            errors.push(format!("`{command}` has no section"));
        }
    }
    let mut helps: BTreeMap<String, BTreeMap<String, Flag>> = BTreeMap::new();
    for (command, section) in &commands {
        let Some(text) = help(Some(command)) else {
            if !PENDING.contains(&command.as_str()) {
                errors.push(format!(
                    "`{command}` is documented but the binary has no such command"
                ));
            }
            continue;
        };
        let printed = help_flags(&text);
        let mut documented = flag_rows(section);
        for group in linked(section) {
            for (name, row) in flag_rows(groups[&group]) {
                if documented.insert(name.clone(), row).is_some() {
                    errors.push(format!("`{command}` {name}: documented twice"));
                }
            }
        }
        let page_names: BTreeSet<String> = documented.keys().cloned().collect();
        let help_names: BTreeSet<String> = printed.keys().cloned().collect();
        for missing in help_names.difference(&page_names) {
            errors.push(format!("`{command}` {missing}: in --help, not on the page"));
        }
        for extra in page_names.difference(&help_names) {
            errors.push(format!("`{command}` {extra}: on the page, not in --help"));
        }
        for (name, printed) in &printed {
            if let Some((flag, _)) = documented.get(name) {
                compare_flag(&format!("`{command}`"), name, flag, printed, &mut errors);
            }
        }
        helps.insert(command.clone(), printed);
    }
    // A group whose commands are all pending is compared with the hidden
    // commands that take the same flags, and otherwise waits for its command.
    for (group, section) in &groups {
        let users: Vec<&String> = commands
            .iter()
            .filter(|(_, command)| linked(command).contains(group))
            .map(|(name, _)| name)
            .collect();
        if users.is_empty() {
            errors.push(format!("#{group}: no command takes these flags"));
            continue;
        }
        if users.iter().any(|user| helps.contains_key(*user)) {
            continue;
        }
        let hidden: Vec<BTreeMap<String, Flag>> = HIDDEN
            .iter()
            .filter_map(|command| help(Some(command)))
            .map(|text| help_flags(&text))
            .collect();
        for (name, (flag, _)) in flag_rows(section) {
            match hidden.iter().find_map(|flags| flags.get(&name)) {
                Some(printed) => {
                    compare_flag(&format!("#{group}"), &name, &flag, printed, &mut errors)
                }
                None if users.iter().all(|user| PENDING.contains(&user.as_str())) => {}
                None => errors.push(format!("#{group} {name}: in no --help")),
            }
        }
    }
    assert!(
        errors.is_empty(),
        "docs/configuration.md:\n{}",
        errors.join("\n")
    );
}

/// The statuses of an `Exit status:` block of `--help`, with single spaces.
fn printed_statuses(help: &str) -> BTreeMap<u8, String> {
    let mut statuses: BTreeMap<u8, String> = BTreeMap::new();
    let mut last = None;
    for line in block(help, "Exit status:") {
        let trimmed = line.trim();
        let (first, rest) = trimmed.split_once(' ').unwrap_or((trimmed, ""));
        if let Ok(status) = first.parse::<u8>() {
            statuses.insert(status, plain(rest));
            last = Some(status);
        } else if let Some(status) = last {
            let text = statuses.get_mut(&status).expect("the status above");
            text.push(' ');
            text.push_str(&plain(trimmed));
        }
    }
    statuses
}

/// The rows of the first table of a section, by their first cell.
fn table(section: &Section) -> BTreeMap<String, Vec<String>> {
    section
        .text
        .lines()
        .skip_while(|line| !line.starts_with('|'))
        .take_while(|line| line.starts_with('|'))
        .skip(2)
        .map(|line| {
            let cells = cells(line);
            (unquoted(&cells[0]), cells[1..].to_vec())
        })
        .collect()
}

fn documented_statuses(section: &Section) -> BTreeMap<u8, String> {
    table(section)
        .into_iter()
        .map(|(status, cells)| {
            let status = status.parse().expect("a status number");
            (status, plain(&cells[0]))
        })
        .collect()
}

#[test]
fn the_operations_page_lists_the_exit_statuses_the_binary_prints() {
    let page = read("docs/operations.md");
    let top = help(None).expect("db-sync-alt --help");
    let printed = printed_statuses(&top);
    assert!(printed.contains_key(&75), "{printed:?}");
    let sections3 = sections(&page, "### ");
    let sections2 = sections(&page, "## ");
    let exit = section(&sections2, "Exit status");
    assert_eq!(documented_statuses(exit), printed, "## Exit status");
    let run = documented_statuses(section(&sections3, "Exit status of `run`"));
    assert!(!run.is_empty());
    if let Some(text) = help(Some("run")) {
        assert_eq!(run, printed_statuses(&text), "### Exit status of `run`");
    }
}

/// The document without a database or a follower: every field present.
fn bare() -> Status {
    status::compose(None, None, &Settings::new("preprod"), 0)
}

fn reader(passed: bool) -> ReaderCheck {
    ReaderCheck {
        role: "reader".to_owned(),
        passed,
        error: None,
        violations: Vec::new(),
        warnings: Vec::new(),
    }
}

/// Every reason `health` can give, found by varying one condition at a time
/// around a healthy document.
fn health_reasons() -> BTreeSet<&'static str> {
    let mut healthy = bare();
    healthy.serving = Serving::Ready;
    healthy.state = State::Following;
    healthy.lag.blocks = Some(0);
    healthy.freshness.provider_observation_age_seconds = Some(0);
    healthy.database.observation_age_seconds = Some(0);
    healthy.database.reader_check = Some(reader(true));
    assert_eq!(status::health(&healthy), (true, Vec::new()));
    let mut variants = Vec::new();
    for serving in [
        Serving::Ready,
        Serving::Unready,
        Serving::Revoked,
        Serving::Unknown,
    ] {
        let mut variant = healthy.clone();
        variant.serving = serving;
        variants.push(variant);
    }
    for state in [
        State::Restoring,
        State::CatchingUp,
        State::Following,
        State::Waiting,
        State::Stopped,
    ] {
        let mut variant = healthy.clone();
        variant.state = state;
        variants.push(variant);
    }
    for value in [None, Some(u64::MAX)] {
        let mut variant = healthy.clone();
        variant.lag.blocks = value;
        variants.push(variant);
        let mut variant = healthy.clone();
        variant.freshness.provider_observation_age_seconds = value;
        variants.push(variant);
        let mut variant = healthy.clone();
        variant.database.observation_age_seconds = value;
        variants.push(variant);
    }
    for check in [None, Some(reader(false))] {
        let mut variant = healthy.clone();
        variant.database.reader_check = check;
        variants.push(variant);
    }
    variants
        .iter()
        .flat_map(|variant| status::health(variant).1)
        .collect()
}

/// The string literals of the functions of `status.rs` that name reasons, so
/// that a condition the variations above do not reach is not missed either.
fn reasons_in_source() -> BTreeSet<String> {
    let source = read("crates/dsa-service/src/status.rs");
    let mut found = BTreeSet::new();
    // Each function up to its closing brace: a method of `impl State` is
    // indented by four spaces, `health` is not.
    for (start, end) in [
        ("fn reason_code(", "\n    }\n"),
        ("pub fn health(", "\n}\n"),
    ] {
        let body = &source[source.find(start).expect(start)..];
        let body = &body[..body.find(end).unwrap_or(body.len())];
        for literal in body.split('"').skip(1).step_by(2) {
            if !literal.is_empty() && literal.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                found.insert(literal.to_owned());
            }
        }
    }
    found
}

#[test]
fn the_operations_page_explains_every_health_reason_and_status_field() {
    let page = read("docs/operations.md");
    let sections = sections(&page, "## ");
    let documented_reasons: BTreeSet<String> =
        table(section(&sections, "/health")).into_keys().collect();
    let reasons: BTreeSet<String> = health_reasons().into_iter().map(str::to_owned).collect();
    assert!(reasons.len() >= 14, "{reasons:?}");
    // A code the variations never produce is a condition they miss; the one
    // exception is the reason of the state `following`, which is never a
    // reason to fail.
    let following = format!(
        "state_{}",
        serde_json::to_value(State::Following).expect("a state")
    )
    .replace('"', "");
    let unreached: Vec<String> = reasons_in_source()
        .into_iter()
        .filter(|code| !reasons.contains(code) && *code != following)
        .collect();
    assert!(
        unreached.is_empty(),
        "reasons no variation produces: {unreached:?}"
    );
    assert_eq!(
        documented_reasons, reasons,
        "the reason codes of ## /health in docs/operations.md"
    );

    let fields = table(section(&sections, "/status"));
    let document = serde_json::to_value(bare()).expect("the status document serialises");
    let mut errors = Vec::new();
    for key in document.as_object().expect("an object").keys() {
        if !fields.contains_key(key) {
            errors.push(format!("{key}: no row"));
        }
    }
    for key in fields.keys() {
        if document.get(key).is_none() {
            errors.push(format!(
                "{key}: a row for a field the document does not have"
            ));
        }
    }
    // The fields inside the objects, from the full document of a following
    // service, must be named in their object's row.
    let full: Value = serde_json::from_str(&read("crates/dsa-service/tests/status/following.json"))
        .expect("the golden document");
    for object in [
        "freshness",
        "lag",
        "tip_lag",
        "progress",
        "last_update",
        "last_error",
        "retention",
        "provider",
        "epochs",
        "integrity",
        "database",
        "identity",
        "first_observations",
        "health",
    ] {
        let row = fields
            .get(object)
            .map(|cells| cells.join(" "))
            .unwrap_or_default();
        for key in full[object].as_object().expect(object).keys() {
            if !row.contains(&format!("`{key}`")) {
                errors.push(format!("{object}.{key}: not named in the row of {object}"));
            }
        }
    }
    assert!(
        errors.is_empty(),
        "## /status in docs/operations.md:\n{}",
        errors.join("\n")
    );
}

/// Commands the binary has outside its documented interface, for development
/// and maintenance: `--help` does not list them and no page names them.
const HIDDEN_COMMANDS: &[&str] = &["convert", "follow"];
/// Flags of listed commands that `--help` does not show, with their command.
const HIDDEN_FLAGS: &[(&str, &str)] = &[("compare", "--a-capture")];

/// The pages that document the interface: the README, `docs/`, the
/// deployment README, the changelog and the security policy.
fn pages() -> Vec<(String, String)> {
    let mut names: Vec<String> = std::fs::read_dir(root().join("docs"))
        .expect("docs/")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .into_string()
                .expect("UTF-8")
        })
        .filter(|name| name.ends_with(".md"))
        .map(|name| format!("docs/{name}"))
        .collect();
    names.sort();
    names.extend(
        [
            "README.md",
            "deploy/README.md",
            "CHANGELOG.md",
            "SECURITY.md",
        ]
        .map(str::to_owned),
    );
    names
        .into_iter()
        .map(|name| {
            let text = read(&name);
            (name, text)
        })
        .collect()
}

/// The `--help` of the binary and of every command it lists, by name.
fn visible_helps() -> Vec<(String, String)> {
    let top = help(None).expect("db-sync-alt --help");
    let mut helps = vec![("db-sync-alt --help".to_owned(), top.clone())];
    for command in listed_commands(&top) {
        let text = help(Some(&command)).expect("a listed command has --help");
        helps.push((format!("db-sync-alt {command} --help"), text));
    }
    helps
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// The positions where `needle` occurs in `text` as a whole name: not inside a
/// longer name on either side.
fn whole(text: &str, needle: &str) -> Vec<usize> {
    text.match_indices(needle)
        .filter(|(at, _)| {
            let before = text[..*at].chars().next_back();
            let after = text[at + needle.len()..].chars().next();
            !before.is_some_and(is_name_char) && !after.is_some_and(is_name_char)
        })
        .map(|(at, _)| at)
        .collect()
}

/// Whether `text` names the command `name`: in a code span of its own or at
/// the start of one, after `db-sync-alt`, or followed by a flag or a colon.
fn names_command(text: &str, name: &str) -> bool {
    text.contains(&format!("`{name}`"))
        || text.contains(&format!("`{name} "))
        || !whole(text, &format!("db-sync-alt {name}")).is_empty()
        || whole(text, name).into_iter().any(|at| {
            let rest = &text[at + name.len()..];
            rest.starts_with(" --") || rest.starts_with(':')
        })
}

/// The long flags of a `--help`, without `--help` and `--version`.
fn flag_names(text: &str) -> BTreeSet<String> {
    help_flags(text).into_keys().collect()
}

#[test]
fn hidden_commands_and_flags_are_hidden_and_no_page_names_them() {
    // The list is complete: every `hide = true` of the command line is in it.
    let mut declared = 0;
    for entry in std::fs::read_dir(root().join("crates/db-sync-alt/src")).expect("src/") {
        let path = entry.expect("an entry").path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            declared += std::fs::read_to_string(&path)
                .expect("a source file")
                .matches("hide = true")
                .count();
        }
    }
    let mut errors = Vec::new();
    if declared != HIDDEN_COMMANDS.len() + HIDDEN_FLAGS.len() {
        errors.push(format!(
            "crates/db-sync-alt/src has {declared} `hide = true`, this test lists {}",
            HIDDEN_COMMANDS.len() + HIDDEN_FLAGS.len()
        ));
    }
    let top = help(None).expect("db-sync-alt --help");
    let listed = listed_commands(&top);
    let mut visible_flags = BTreeSet::new();
    for command in &listed {
        visible_flags.extend(flag_names(
            &help(Some(command)).expect("a listed command has --help"),
        ));
    }
    let mut hidden_flags: BTreeSet<String> = BTreeSet::new();
    for command in HIDDEN_COMMANDS {
        if listed.iter().any(|name| name == command) {
            errors.push(format!("`{command}` is listed by db-sync-alt --help"));
        }
        match help(Some(command)) {
            Some(text) => hidden_flags.extend(flag_names(&text)),
            None => errors.push(format!("`{command} --help` does not work")),
        }
    }
    let mut hidden_flags: BTreeSet<String> =
        hidden_flags.difference(&visible_flags).cloned().collect();
    for (command, flag) in HIDDEN_FLAGS {
        let text = help(Some(command)).expect("a listed command has --help");
        if flag_names(&text).contains(*flag) {
            errors.push(format!("`{command}` {flag} is shown by --help"));
        }
        let accepted = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([command, flag, "value", "--help"])
            .output()
            .expect("the binary runs")
            .status
            .success();
        if !accepted {
            errors.push(format!("`{command}` does not accept {flag}"));
        }
        hidden_flags.insert((*flag).to_owned());
    }
    // The flags of `convert` that no listed command has, with `--a-capture`
    // and `follow --stop-at-target`: fewer means a hidden command's flags
    // became visible.
    if hidden_flags.len() < 10 {
        errors.push(format!("only these flags are hidden: {hidden_flags:?}"));
    }
    for (name, text) in pages().into_iter().chain(visible_helps()) {
        for command in HIDDEN_COMMANDS {
            if names_command(&text, command) {
                errors.push(format!("{name} names the hidden command `{command}`"));
            }
        }
        for flag in &hidden_flags {
            if !whole(&text, flag).is_empty() {
                errors.push(format!("{name} names the hidden flag {flag}"));
            }
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// An earlier db-sync-alt version in `text` (0.1.x to 0.4.x, also with a `v`),
/// not part of a longer number or address such as `0.0.0.0:18080`.
fn earlier_version(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    for (start, _) in text.match_indices('0') {
        let at = if start > 0 && bytes[start - 1] == b'v' {
            start - 1
        } else {
            start
        };
        let before = text[..at].chars().next_back();
        if before.is_some_and(|c| c.is_alphanumeric() || c == '.' || c == '_') {
            continue;
        }
        let rest = &bytes[start..];
        if rest.len() < 5 || rest[1] != b'.' || !(b'0'..=b'4').contains(&rest[2]) || rest[3] != b'.'
        {
            continue;
        }
        let last = if rest[4] == b'x' {
            5
        } else {
            4 + rest[4..].iter().take_while(|b| b.is_ascii_digit()).count()
        };
        if last == 4 {
            continue;
        }
        let after = &rest[last..];
        let longer = after
            .first()
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
            || (after.first() == Some(&b'.') && after.get(1).is_some_and(u8::is_ascii_digit));
        if !longer {
            return Some(text[at..start + last].to_owned());
        }
    }
    None
}

/// Words that describe a change from an earlier version instead of the
/// software as it is.
const HISTORY_WORDS: &[&str] = &[
    "upgrade",
    "upgrading",
    "migrate",
    "migration",
    "no longer",
    "is gone",
    "are gone",
    "earlier version",
    "earlier release",
    "previous version",
    "previous release",
];

/// `.github/scripts/check_public_source.py` applies the same rule to every
/// prose file of the repository; this test applies it to what the binary
/// prints.
#[test]
fn no_help_text_names_an_earlier_version_or_an_upgrade() {
    let mut errors = Vec::new();
    for (name, text) in visible_helps() {
        if let Some(version) = earlier_version(&text) {
            errors.push(format!("{name}: {version}"));
        }
        let lower = text.to_lowercase();
        for word in HISTORY_WORDS {
            if !whole(&lower, word).is_empty() {
                errors.push(format!("{name}: {word:?}"));
            }
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn the_version_rule_finds_earlier_versions_and_nothing_else() {
    for (text, found) in [
        ("from 0.4.x on", Some("0.4.x")),
        ("tag v0.4.9.", Some("v0.4.9")),
        ("0.1.1", Some("0.1.1")),
        ("listen on 0.0.0.0:18080", None),
        ("127.0.0.1", None),
        ("took 0.19 s and 0.4 s", None),
        ("version 0.5.0", None),
        ("node 2.1.101, 10.4.1", None),
    ] {
        assert_eq!(earlier_version(text).as_deref(), found, "{text}");
    }
}
