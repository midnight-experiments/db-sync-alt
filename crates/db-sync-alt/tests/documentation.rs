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
            errors.push(format!(
                "`{command}` is documented but the binary has no such command"
            ));
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
    // Every group is taken by a command whose `--help` was compared above.
    for group in groups.keys() {
        let compared = commands
            .iter()
            .any(|(name, command)| linked(command).contains(group) && helps.contains_key(name));
        if !compared {
            errors.push(format!("#{group}: no command takes these flags"));
        }
    }
    assert!(
        errors.is_empty(),
        "docs/configuration.md:\n{}",
        errors.join("\n")
    );
}

/// `run` with `arguments` and `environment` and no other `DSA_` variable: its
/// exit status and standard error.
fn run_refusal(arguments: &[&str], environment: &[(&str, &str)]) -> (Option<i32>, String) {
    let mut process = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    process.args([
        "run",
        "--database",
        "host=127.0.0.1 port=1 user=nobody dbname=nowhere sslmode=disable",
        "--network",
        "preprod",
        "--provider-key-file",
        "/nonexistent/key",
        "--reader",
        "reader",
    ]);
    process.args(arguments);
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
        process.env_remove(name);
    }
    process.envs(environment.iter().copied());
    let output = process.output().expect("the binary runs");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn run_refuses_the_overall_timeout_as_the_page_says() {
    let page = read("docs/configuration.md");
    let sections = sections(&page, "## ");
    let run = plain(&section(&sections, "`run`").text);
    assert!(
        run.contains("It refuses --overall-timeout-seconds and DSA_OVERALL_TIMEOUT_SECONDS with exit status 2"),
        "## `run` in docs/configuration.md: {run}"
    );
    let refused = r#"{"error":"overall_timeout_not_supported","flag":"--overall-timeout-seconds"}"#;
    for (arguments, environment) in [
        (&["--overall-timeout-seconds", "60"][..], &[][..]),
        (&[][..], &[("DSA_OVERALL_TIMEOUT_SECONDS", "60")][..]),
    ] {
        let (code, stderr) = run_refusal(arguments, environment);
        assert_eq!(code, Some(2), "{arguments:?} {environment:?}: {stderr}");
        assert!(
            stderr.lines().any(|line| line == refused),
            "{arguments:?} {environment:?}: {stderr}"
        );
    }
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
    let text = help(Some("run")).expect("db-sync-alt run --help");
    assert_eq!(run, printed_statuses(&text), "### Exit status of `run`");
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
    // A provider head made long before the document.
    let mut variant = healthy.clone();
    variant.generated_at = 100_000;
    variant.provider_head = Some(dsa_store::service_view::Point {
        height: 1,
        hash: "00".repeat(32),
        timestamp: 0,
    });
    variants.push(variant);
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
