//! Profiles generated from the pinned Midnight node configuration files.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const NODE_RELEASE: &str = "2.1.101";
pub const NODE_COMMIT: &str = "86774edf8cc76345a5c3a4221f3fe80c094b1c33";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub format: u32,
    pub network: String,
    pub node_release: String,
    pub cardano: Cardano,
    pub start: StartPosition,
    pub addresses: BTreeMap<String, String>,
    /// Named input assets plus `bridge`, the token in shipped genesis storage.
    /// Genesis values do not assert a live chain's current runtime configuration.
    pub assets: BTreeMap<String, Asset>,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cardano {
    pub security_parameter: u32,
    pub active_slots_coefficient: f64,
    pub block_stability_margin: u32,
    pub first_epoch_number: u32,
    pub first_epoch_timestamp_millis: u64,
    pub epoch_duration_millis: u64,
    pub first_slot_number: u64,
    pub slot_duration_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartPosition {
    pub block_number: u32,
    pub block_hash: String,
    pub block_timestamp_millis: u64,
    pub tx_index_in_block: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    /// Lowercase hexadecimal policy ID.
    pub policy: String,
    /// Lowercase hexadecimal asset name (the UTF-8 bytes in node config).
    pub name: String,
}

impl Asset {
    pub fn unit(&self) -> String {
        format!("{}{}", self.policy, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub repository: String,
    pub commit: String,
    /// Consumed node file paths and the SHA-256 of their original bytes.
    pub files: BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("unsupported_network: {0}")]
    UnsupportedNetwork(String),
    #[error("invalid_profile: {0}")]
    Invalid(#[from] serde_json::Error),
}

impl Profile {
    /// Only networks supported by the released service are selectable.
    pub fn load(network: &str) -> Result<Self, ProfileError> {
        let json = match network {
            "mainnet" => include_str!("../profiles/mainnet.json"),
            "preprod" => include_str!("../profiles/preprod.json"),
            _ => return Err(ProfileError::UnsupportedNetwork(network.to_owned())),
        };
        Ok(serde_json::from_str(json)?)
    }

    /// Hash the complete deterministic profile representation, including provenance.
    pub fn hash(&self) -> Result<String, serde_json::Error> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_profiles_are_bound_to_node() {
        for (network, start, index) in [("mainnet", 13_164_005, 1), ("preprod", 4_500_197, 4)] {
            let profile = Profile::load(network).unwrap();
            assert_eq!(profile.network, network);
            assert_eq!(profile.node_release, NODE_RELEASE);
            assert_eq!(profile.provenance.commit, NODE_COMMIT);
            assert_eq!(profile.cardano.security_parameter, 2160);
            assert_eq!(profile.cardano.active_slots_coefficient, 0.05);
            assert_eq!(profile.cardano.block_stability_margin, 30);
            assert_eq!(profile.start.block_number, start);
            assert_eq!(profile.start.tx_index_in_block, index);
            assert_eq!(profile.addresses.len(), 5);
            assert_eq!(profile.assets.len(), 8);
            assert_eq!(profile.provenance.files.len(), 10);
            assert_eq!(profile.hash().unwrap().len(), 64);
            let mut changed = profile.clone();
            changed.cardano.block_stability_margin += 1;
            assert_ne!(profile.hash().unwrap(), changed.hash().unwrap());
        }
    }

    #[test]
    fn preprod_keeps_bridge_genesis_and_config_input_units_distinct() {
        let profile = Profile::load("preprod").unwrap();
        assert_eq!(profile.assets["ics"].name, "");
        assert_eq!(profile.assets["reserve"].name, "");
        assert_eq!(profile.assets["bridge"].name, "4e49474854");
        assert_eq!(
            profile.assets["bridge"].policy,
            profile.assets["ics"].policy
        );
        assert_ne!(
            profile.assets["bridge"].unit(),
            profile.assets["ics"].unit()
        );
    }

    #[test]
    fn preview_is_generated_but_not_selectable() {
        let preview: Profile =
            serde_json::from_str(include_str!("../profiles/preview.json")).unwrap();
        assert_eq!(preview.cardano.security_parameter, 432);
        assert_eq!(preview.cardano.block_stability_margin, 30);
        assert_eq!(preview.start.block_number, 0);
        for network in ["preview", "", "Mainnet", "other"] {
            assert!(matches!(
                Profile::load(network),
                Err(ProfileError::UnsupportedNetwork(_))
            ));
        }
    }
}
