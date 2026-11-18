//! Token retention shared by conversion, following and independent rebuild.

use std::collections::BTreeSet;

use crate::profiles::Profile;

pub const RULE_VERSION: u32 = 2;

#[derive(Clone, Debug)]
pub struct AssetRule {
    exact_units: BTreeSet<String>,
    governance_policies: BTreeSet<String>,
}

impl AssetRule {
    pub fn from_profile(profile: &Profile) -> Self {
        Self {
            exact_units: ["cnight", "auth", "permissioned_candidates", "bridge"]
                .iter()
                .map(|role| profile.assets[*role].unit())
                .collect(),
            governance_policies: ["council", "technical_committee"]
                .iter()
                .map(|role| profile.assets[*role].policy.clone())
                .collect(),
        }
    }

    pub fn allows(&self, policy: &str, name: &str) -> bool {
        self.governance_policies.contains(policy)
            || self.exact_units.contains(&format!("{policy}{name}"))
    }

    pub fn exact_units(&self) -> &BTreeSet<String> {
        &self.exact_units
    }

    pub fn governance_policies(&self) -> &BTreeSet<String> {
        &self.governance_policies
    }
}
