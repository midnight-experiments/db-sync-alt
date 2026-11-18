//! Closed extraction of the vendored node's SQL literals and native bind types.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::Path};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Source {
    pub file: String,
    pub line: usize,
    pub function: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Statement {
    pub id: String,
    pub name: String,
    pub sql: String,
    pub sha256: String,
    pub kind: String,
    pub group: String,
    pub parameters: Vec<String>,
    pub sources: Vec<Source>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Catalogue {
    pub node_commit: String,
    pub files: BTreeMap<String, String>,
    pub entries: Vec<Statement>,
}
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn literals(source: &str) -> Vec<(usize, String)> {
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
                assert!(i < bytes.len(), "unterminated comment");
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
                let n = source[i..].find(&end).expect("unterminated raw literal");
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
                    let e = source[i..].chars().next().unwrap();
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
                        _ => panic!("unsupported Rust string escape"),
                    });
                } else {
                    value.push(c);
                }
            }
            assert!(i < bytes.len());
            i += 1;
            out.push((start, value));
        } else {
            i += source[i..].chars().next().unwrap().len_utf8();
        }
    }
    out
}
fn types(name: &str, count: usize) -> Vec<String> {
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
        _ => panic!("unclassified native parameters: {name}"),
    };
    assert_eq!(types.len(), count, "{name}");
    types.into_iter().map(str::to_owned).collect()
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
fn files(root: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in fs::read_dir(root).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            files(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}
pub fn extract(root: &Path) -> Catalogue {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("source-manifest.json")).unwrap()).unwrap();
    let mut paths = Vec::new();
    files(root, &mut paths);
    paths.sort();
    let mut hashes = BTreeMap::new();
    let mut entries: BTreeMap<String, Statement> = BTreeMap::new();
    for path in paths {
        let file = path
            .strip_prefix(root)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");
        let source = fs::read_to_string(&path).unwrap();
        let digest = hash(source.as_bytes());
        assert_eq!(
            manifest["files"][&file]["sha256"].as_str(),
            Some(digest.as_str()),
            "reference manifest drift: {file}"
        );
        hashes.insert(file.clone(), digest);
        for (offset, text) in literals(&source) {
            let sql = text.trim().trim_end_matches(';').to_owned();
            let upper = sql.to_uppercase();
            if !["SELECT", "WITH ", "CREATE INDEX", "ALTER TABLE"]
                .iter()
                .any(|p| upper.starts_with(p))
            {
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
                line: source[..offset].bytes().filter(|b| *b == b'\n').count() + 1,
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
                types(&name, placeholders(&sql))
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
    }
    let mut entries = entries.into_values().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    Catalogue {
        node_commit: manifest["commit"].as_str().unwrap().to_owned(),
        files: hashes,
        entries,
    }
}
