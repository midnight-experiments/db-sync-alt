//! All SQL literals are extracted at build time from pinned node 2.1.101 files.
//! The committed inventory also includes DDL and non-emitted bridge components;
//! readers execute only complete SELECTs and the assembled enabled bridge query.
#[allow(dead_code)]
#[path = "../codegen/catalogue.rs"]
mod extraction;
pub use extraction::{Catalogue, Source, Statement};
pub const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/node-catalogue.json"));
pub fn catalogue() -> Catalogue {
    serde_json::from_str(GENERATED).expect("built catalogue")
}
#[cfg(test)]
mod tests {
    #[test]
    fn committed_catalogue_matches_extraction() {
        let generated: serde_json::Value = serde_json::from_str(super::GENERATED).unwrap();
        let committed: serde_json::Value =
            serde_json::from_str(include_str!("../catalogue.json")).unwrap();
        assert_eq!(generated, committed);
        assert_eq!(
            super::catalogue()
                .entries
                .iter()
                .filter(|e| e.kind == "read")
                .count(),
            29
        );
    }
    #[test]
    fn scanner_skips_comments() {
        let values = super::extraction::literals(
            "// \"no\"\n/* \"no\" /* nested */ */ r##\"SELECT \"yes\"\"##",
        )
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
        assert_eq!(values, vec!["SELECT \"yes\"".to_owned()]);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Checkpoint {
    Block(i32),
    Tx(i32, i32),
    ReserveTransfer(i32, i32),
}
/// Exact enabled-input QueryBuilder assembly, copied from the verified A3
/// extractor. Interpolation accepts only typed integers and closed arm names.
pub fn bridge_sql(checkpoint: Checkpoint, limit: Option<u32>) -> String {
    use std::collections::BTreeMap;
    let source = include_str!(
        "../../../reference/partner-chains/toolkit/data-sources/db-sync/src/db_model.rs"
    );
    let body = source.split_once("fn get_bridge_txs(").unwrap().1;
    let literal = |s: &str| extraction::literals(s).into_iter().next().unwrap().1;
    let mut substitutions = BTreeMap::new();
    for name in ["bridge_inputs_subquery", "reserve_inputs_subquery"] {
        let block = body
            .split_once(&format!("let {name} = match tx_in_configuration"))
            .unwrap()
            .1;
        substitutions.insert(
            name,
            literal(block.split_once("TxInConfiguration::Enabled =>").unwrap().1),
        );
    }
    for name in ["bridge_outputs_subquery", "reserve_outputs_subquery"] {
        substitutions.insert(
            name,
            literal(body.split_once(&format!("let {name} =")).unwrap().1),
        );
    }
    let (arm, values) = match checkpoint {
        Checkpoint::Block(n) => ("ResolvedBridgeDataCheckpoint::Block", vec![n]),
        Checkpoint::Tx(n, i) => ("ResolvedBridgeDataCheckpoint::Tx {", vec![n, i]),
        Checkpoint::ReserveTransfer(n, i) => (
            "ResolvedBridgeDataCheckpoint::TxReserveTransfer",
            vec![n, i],
        ),
    };
    let mut predicate = literal(body.split_once(arm).unwrap().1);
    for n in values {
        predicate = predicate.replacen("{}", &n.to_string(), 1);
    }
    substitutions.insert("checkpoint_limit", predicate);
    substitutions.insert(
        "max_rows",
        limit.map_or_else(|| "null".to_owned(), |n| n.to_string()),
    );
    let mut sql = literal(
        body.split_once("let mut query_builder = QueryBuilder::new")
            .unwrap()
            .1,
    );
    for (name, value) in substitutions {
        sql = sql.replace(&format!("{{{name}}}"), &value);
    }
    assert!(!sql.contains(['{', '}']));
    sql
}
