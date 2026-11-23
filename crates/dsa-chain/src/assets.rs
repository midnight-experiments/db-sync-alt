//! Token retention shared by conversion, following and independent rebuild.

use std::collections::BTreeSet;

use crate::profiles::{AssetRole, Profile};

pub const RULE_VERSION: u32 = 2;

/// Fixed descriptor/supply guarantee; transaction-asset eligibility remains
/// AssetRule's four exact units plus the two governance policies.
pub const FIXED_UNIT_ROLES: [AssetRole; 6] = [
    AssetRole::Cnight,
    AssetRole::Auth,
    AssetRole::PermissionedCandidates,
    AssetRole::Bridge,
    AssetRole::Council,
    AssetRole::TechnicalCommittee,
];
pub fn fixed_units(profile: &Profile) -> BTreeSet<String> {
    FIXED_UNIT_ROLES
        .iter()
        .map(|role| profile.assets.get(*role).unit())
        .collect()
}

#[derive(Clone, Debug)]
pub struct AssetRule {
    exact_units: BTreeSet<String>,
    governance_policies: BTreeSet<String>,
}

impl AssetRule {
    pub fn from_profile(profile: &Profile) -> Self {
        Self {
            exact_units: [
                &profile.assets.cnight,
                &profile.assets.auth,
                &profile.assets.permissioned_candidates,
                &profile.assets.bridge,
            ]
            .iter()
            .map(|asset| asset.unit())
            .collect(),
            governance_policies: profile
                .governance()
                .iter()
                .map(|body| body.asset.policy.clone())
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
