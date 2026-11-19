//! Operator-visible scope of the pinned Midnight node contract.

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeCapabilities {
    pub registered_candidates: RegisteredCandidates,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RegisteredCandidates {
    pub node_release: &'static str,
    pub enabled_in_block_production_and_import: bool,
    pub operator_rpc_cli: &'static str,
    pub unsupported_scope_includes_ariadne_registrations: bool,
    pub future_support_requires_full_address_coverage: bool,
    pub future_support_requires_new_keep_rule_version: bool,
}

impl NodeCapabilities {
    pub fn pinned() -> Self {
        Self {
            registered_candidates: RegisteredCandidates {
                node_release: crate::profiles::NODE_RELEASE,
                enabled_in_block_production_and_import: false,
                operator_rpc_cli: "unsupported",
                unsupported_scope_includes_ariadne_registrations: true,
                future_support_requires_full_address_coverage: true,
                future_support_requires_new_keep_rule_version: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registered_candidates_are_explicitly_unsupported() {
        let capabilities = NodeCapabilities::pinned();
        assert_eq!(capabilities.registered_candidates.node_release, "2.1.101");
        assert!(
            !capabilities
                .registered_candidates
                .enabled_in_block_production_and_import
        );
        assert_eq!(
            capabilities.registered_candidates.operator_rpc_cli,
            "unsupported"
        );
        assert!(
            capabilities
                .registered_candidates
                .future_support_requires_full_address_coverage
        );
        assert!(
            capabilities
                .registered_candidates
                .future_support_requires_new_keep_rule_version
        );
    }
}
