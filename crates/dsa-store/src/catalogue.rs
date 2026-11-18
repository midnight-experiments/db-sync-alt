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
