//! Closed extraction of the vendored node's SQL literals and native bind types.
//!
//! Every SQL-looking literal of a vendored file is either catalogued or listed
//! in `sql-outside-catalogue.json` with its reason; anything else is an error.
//! `.github/scripts/check_reference_source.py` applies the same rule to the
//! node files that are not vendored.
use dsa_store::catalogue::{Bridge, Catalogue, Class, OutsideCatalogue, Source, Statement};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub struct Extraction {
    pub catalogue: Catalogue,
    pub bridge: Bridge,
}

const KEYWORDS: [&str; 19] = [
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "COPY",
    "GRANT", "REVOKE", "ANALYZE", "VACUUM", "EXPLAIN", "LOCK", "REINDEX", "REFRESH", "SET",
];
const STRUCTURE: [&str; 5] = ["FROM", "INTO", "TABLE", "INDEX", "WHERE"];
/// The prefixes of the catalogued statements.
const CATALOGUED: [&str; 4] = ["SELECT", "WITH ", "CREATE INDEX", "ALTER TABLE"];
const BRIDGE_FILE: &str = "partner-chains/toolkit/data-sources/db-sync/src/db_model.rs";

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// A literal whose first word is an SQL statement keyword in SQL spelling: all
/// capitals, or all lower case followed by an SQL structure word.
pub fn sql_looking(value: &str) -> bool {
    let Some(first) = value.split_whitespace().next() else {
        return false;
    };
    let first = first.trim_end_matches(';');
    if !KEYWORDS.contains(&first.to_uppercase().as_str()) {
        return false;
    }
    if first == first.to_uppercase() {
        return true;
    }
    if first != first.to_lowercase() {
        return false;
    }
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let token = value[start..i].to_ascii_uppercase();
            if STRUCTURE.contains(&token.as_str())
                || (token == "AS" && value[i..].trim_start().starts_with('('))
            {
                return true;
            }
        } else {
            i += 1;
        }
    }
    false
}

/// (offset, value) of every string literal of a Rust source, outside comments.
pub fn literals(source: &str) -> Result<Vec<(usize, String)>, String> {
    let bytes = source.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < bytes.len() {
        if source[i..].starts_with("//") {
            i += source[i..].find('\n').unwrap_or(bytes.len() - i);
            continue;
        }
        if source[i..].starts_with("/*") {
            let mut depth = 1;
            i += 2;
            while depth > 0 {
                if i >= bytes.len() {
                    return Err("unterminated comment".to_owned());
                }
                if source[i..].starts_with("/*") {
                    depth += 1;
                    i += 2;
                } else if source[i..].starts_with("*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += source[i..].chars().next().unwrap().len_utf8();
                }
            }
            continue;
        }
        let start = i;
        if bytes[i] == b'r' {
            i += 1;
            while i < bytes.len() && bytes[i] == b'#' {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'"' {
                let end = format!("\"{}", "#".repeat(i - start - 1));
                i += 1;
                let n = source[i..].find(&end).ok_or("unterminated raw literal")?;
                out.push((start, source[i..i + n].to_owned()));
                i += n + end.len();
                continue;
            }
            i = start;
        }
        if bytes[i] == b'"' {
            i += 1;
            let mut value = String::new();
            while i < bytes.len() && bytes[i] != b'"' {
                let c = source[i..].chars().next().unwrap();
                i += c.len_utf8();
                if c == '\\' {
                    let e = source[i..].chars().next().ok_or("unterminated literal")?;
                    i += e.len_utf8();
                    if e == '\n' || e == '\r' {
                        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                            i += 1;
                        }
                        continue;
                    }
                    value.push(match e {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        _ => return Err(format!("unsupported Rust string escape \\{e}")),
                    });
                } else {
                    value.push(c);
                }
            }
            if i >= bytes.len() {
                return Err("unterminated literal".to_owned());
            }
            i += 1;
            out.push((start, value));
        } else {
            i += source[i..].chars().next().unwrap().len_utf8();
        }
    }
    Ok(out)
}

fn types(name: &str, count: usize) -> Result<Vec<String>, String> {
    let types: Vec<&str> = match name {
        "get_registrations" => [vec!["text", "int8"], vec!["int4"; 6], vec!["int8"; 6]].concat(),
        "get_deregistrations" => [vec!["text"], vec!["int4"; 6], vec!["int8"; 4]].concat(),
        "get_asset_creates" => [vec!["int8"], vec!["int4"; 6], vec!["int8"; 6]].concat(),
        "get_asset_spends" => [vec!["int8"], vec!["int4"; 6], vec!["int8"; 4]].concat(),
        "get_token_utxo_for_epoch" if count == 2 => vec!["int8", "int4"],
        "get_token_utxo_for_epoch" => vec!["bytea", "bytea", "int4"],
        "get_block_by_hash" | "get_epoch_for_block_hash" | "get_block_info_for_tx_hash" => {
            vec!["bytea"]
        }
        "resolve_ident" => vec!["bytea", "bytea"],
        "get_blocks_by_numbers" => vec!["int4"; 2],
        "get_highest_block" => vec!["int4", "int8", "int8", "timestamp", "timestamp"],
        "get_governance_body_utxo" => vec!["text", "bytea", "int4"],
        "get_utxos_for_address_tx_in_enabled" | "get_utxos_for_address_tx_in_consumed" => {
            vec!["text", "int4"]
        }
        "index_exists" => vec!["text"],
        "get_low_bounds"
        | "get_high_bounds"
        | "get_max_tx_size"
        | "get_latest_block_for_epoch"
        | "get_latest_stable_epoch"
        | "get_stake_distribution"
        | "get_epoch_nonce" => vec!["int4"],
        _ if count == 0 => vec![],
        _ => return Err(format!("unclassified native parameters: {name}")),
    };
    if types.len() != count {
        return Err(format!("parameter count of {name}"));
    }
    Ok(types.into_iter().map(str::to_owned).collect())
}

fn placeholders(sql: &str) -> usize {
    let mut max = 0;
    let b = sql.as_bytes();
    for i in 0..b.len() {
        if b[i] == b'$' {
            let digits = sql[i + 1..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            if let Ok(n) = digits.parse::<usize>() {
                max = max.max(n);
            }
        }
    }
    max
}

fn files(root: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    for e in fs::read_dir(root).map_err(|e| e.to_string())? {
        let p = e.map_err(|e| e.to_string())?.path();
        if p.is_dir() {
            files(&p, out)?;
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
    Ok(())
}

fn line(source: &str, offset: usize) -> usize {
    source[..offset].bytes().filter(|b| *b == b'\n').count() + 1
}

pub fn extract(root: &Path, outside: &OutsideCatalogue) -> Result<Extraction, String> {
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("source-manifest.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let mut paths = Vec::new();
    files(root, &mut paths)?;
    paths.sort();
    let mut hashes = BTreeMap::new();
    let mut entries: BTreeMap<String, Statement> = BTreeMap::new();
    let listed: BTreeSet<(&str, usize, &str)> = outside
        .statements
        .iter()
        .map(|s| (s.file.as_str(), s.line, s.sha256.as_str()))
        .collect();
    let mut seen = BTreeSet::new();
    let mut bridge = None;
    for path in paths {
        let file = path
            .strip_prefix(root)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");
        let source = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let digest = hash(source.as_bytes());
        if manifest["files"][&file]["sha256"].as_str() != Some(digest.as_str()) {
            return Err(format!("reference manifest drift: {file}"));
        }
        hashes.insert(file.clone(), digest);
        for (offset, text) in literals(&source).map_err(|e| format!("{file}: {e}"))? {
            let sql = text.trim().trim_end_matches(';').to_owned();
            let upper = sql.to_uppercase();
            if !CATALOGUED.iter().any(|p| upper.starts_with(p)) {
                if sql_looking(&text) {
                    let at = line(&source, offset);
                    let sha256 = hash(sql.as_bytes());
                    if !listed.contains(&(file.as_str(), at, sha256.as_str())) {
                        return Err(format!("uncatalogued SQL literal: {file}:{at}"));
                    }
                    seen.insert((file.clone(), at, sha256));
                }
                continue;
            }
            let name = source[..offset]
                .rsplit_once("fn ")
                .map(|(_, p)| {
                    p.chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect::<String>()
                })
                .unwrap_or_else(|| "unknown".to_owned());
            let location = Source {
                file: file.clone(),
                line: line(&source, offset),
                function: name.clone(),
            };
            let key = sql
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase();
            if let Some(e) = entries.get_mut(&key) {
                e.sources.push(location);
                continue;
            }
            let kind = if sql.contains('{') || name == "get_bridge_txs" {
                "component"
            } else if upper.starts_with("SELECT") || upper.starts_with("WITH ") {
                "read"
            } else {
                "ddl"
            };
            let group = if name == "get_low_bounds" || name == "get_high_bounds" {
                "bounds"
            } else if name == "index_exists"
                || name == "from_connection"
                || name == "create_index_if_not_exists"
            {
                "startup"
            } else if [
                "get_latest_block_for_epoch",
                "get_stake_distribution",
                "get_utxos_for_address_tx_in_enabled",
                "get_utxos_for_address_tx_in_consumed",
            ]
            .contains(&name.as_str())
            {
                "rpc"
            } else {
                "node"
            };
            let sha256 = hash(sql.as_bytes());
            let parameters = if kind == "read" {
                types(&name, placeholders(&sql))?
            } else {
                Vec::new()
            };
            let id = format!("{}:{}", name, &sha256[..10]);
            entries.insert(
                key,
                Statement {
                    id,
                    name,
                    sql,
                    sha256,
                    kind: kind.to_owned(),
                    group: group.to_owned(),
                    parameters,
                    sources: vec![location],
                },
            );
        }
        if file == BRIDGE_FILE {
            bridge = Some(assemble_bridge(&source)?);
        }
    }
    for statement in &outside.statements {
        let key = (
            statement.file.clone(),
            statement.line,
            statement.sha256.clone(),
        );
        let vendored = manifest["files"].get(&statement.file).is_some();
        if vendored && !seen.contains(&key) {
            return Err(format!(
                "stale SQL exclusion: {}:{}",
                statement.file, statement.line
            ));
        }
        if vendored != (statement.class == Class::Assembled) {
            return Err(format!(
                "wrong exclusion class: {}:{}",
                statement.file, statement.line
            ));
        }
        if (statement.class == Class::Unsupported) == statement.subcommands.is_empty() {
            return Err(format!(
                "unsupported statements name their subcommands: {}:{}",
                statement.file, statement.line
            ));
        }
    }
    let mut entries = entries.into_values().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    let mut candidate_addresses = BTreeMap::new();
    for net in ["mainnet", "preprod", "preview"] {
        let file = format!("res/{net}/registered-candidates-addresses.json");
        let bytes = fs::read(root.join(&file)).map_err(|e| e.to_string())?;
        let digest = hash(&bytes);
        if manifest["files"][&file]["sha256"].as_str() != Some(digest.as_str()) {
            return Err(format!("reference manifest drift: {file}"));
        }
        hashes.insert(file, digest);
        let args: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        candidate_addresses.insert(
            net.to_owned(),
            args["committee_candidates_address"]
                .as_str()
                .ok_or("committee_candidates_address")?
                .to_owned(),
        );
    }
    Ok(Extraction {
        catalogue: Catalogue {
            node_commit: manifest["commit"].as_str().ok_or("commit")?.to_owned(),
            files: hashes,
            entries,
            candidate_addresses,
        },
        bridge: bridge.ok_or("bridge source missing")?,
    })
}

fn first_literal(text: &str) -> Result<String, String> {
    Ok(literals(text)?
        .into_iter()
        .next()
        .ok_or("expected a literal")?
        .1)
}

fn after<'a>(text: &'a str, marker: &str) -> Result<&'a str, String> {
    Ok(text
        .split_once(marker)
        .ok_or_else(|| format!("missing {marker}"))?
        .1)
}

/// The node's `get_bridge_txs` QueryBuilder text for enabled inputs, a block
/// checkpoint and no row limit, with `{}` where the checkpoint block goes.
pub fn assemble_bridge(source: &str) -> Result<Bridge, String> {
    let start = source
        .find("fn get_bridge_txs(")
        .ok_or("missing get_bridge_txs")?;
    let body = &source[start..];
    let mut substitutions = BTreeMap::new();
    for name in ["bridge_inputs_subquery", "reserve_inputs_subquery"] {
        let block = after(body, &format!("let {name} = match tx_in_configuration"))?;
        substitutions.insert(
            name,
            first_literal(after(block, "TxInConfiguration::Enabled =>")?)?,
        );
    }
    for name in ["bridge_outputs_subquery", "reserve_outputs_subquery"] {
        substitutions.insert(name, first_literal(after(body, &format!("let {name} ="))?)?);
    }
    let checkpoint = first_literal(after(body, "ResolvedBridgeDataCheckpoint::Block")?)?;
    substitutions.insert("checkpoint_limit", checkpoint);
    substitutions.insert("max_rows", "null".to_owned());
    let template = after(body, "let mut query_builder = QueryBuilder::new")?;
    let template_offset = source.len() - template.len();
    let mut sql = first_literal(template)?;
    for (name, value) in substitutions {
        sql = sql.replace(&format!("{{{name}}}"), &value);
    }
    if sql.matches("{}").count() != 1 || sql.replacen("{}", "", 1).contains(['{', '}']) {
        return Err("bridge statement placeholders".to_owned());
    }
    let literal = template_offset + template.find('"').ok_or("bridge template literal")?;
    Ok(Bridge {
        source: Source {
            file: BRIDGE_FILE.to_owned(),
            line: line(source, literal),
            function: "get_bridge_txs".to_owned(),
        },
        sql,
    })
}
