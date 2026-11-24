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
    pub addresses: Addresses,
    /// Named input assets plus `bridge`, the token in shipped genesis storage.
    /// Genesis values do not assert a live chain's current runtime configuration.
    pub assets: Assets,
    pub provenance: Provenance,
}

/// The Cardano addresses the node reads, by role. Fields are declared in the
/// order of their names, so a profile serialises exactly as its generated JSON
/// and keeps its hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Addresses {
    pub council: String,
    pub ics: String,
    pub mapping: String,
    pub reserve: String,
    pub technical_committee: String,
}

impl Addresses {
    pub fn get(&self, role: AddressRole) -> &str {
        match role {
            AddressRole::Council => &self.council,
            AddressRole::Ics => &self.ics,
            AddressRole::Mapping => &self.mapping,
            AddressRole::Reserve => &self.reserve,
            AddressRole::TechnicalCommittee => &self.technical_committee,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (AddressRole, &str)> {
        AddressRole::ALL
            .into_iter()
            .map(|role| (role, self.get(role)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AddressRole {
    Council,
    Ics,
    Mapping,
    Reserve,
    TechnicalCommittee,
}

impl AddressRole {
    pub const ALL: [Self; 5] = [
        Self::Council,
        Self::Ics,
        Self::Mapping,
        Self::Reserve,
        Self::TechnicalCommittee,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Council => "council",
            Self::Ics => "ics",
            Self::Mapping => "mapping",
            Self::Reserve => "reserve",
            Self::TechnicalCommittee => "technical_committee",
        }
    }
}

/// The tokens the node reads, by role, declared in the order of their names
/// like [`Addresses`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assets {
    pub auth: Asset,
    pub bridge: Asset,
    pub cnight: Asset,
    pub council: Asset,
    pub ics: Asset,
    pub permissioned_candidates: Asset,
    pub reserve: Asset,
    pub technical_committee: Asset,
}

impl Assets {
    pub fn get(&self, role: AssetRole) -> &Asset {
        match role {
            AssetRole::Auth => &self.auth,
            AssetRole::Bridge => &self.bridge,
            AssetRole::Cnight => &self.cnight,
            AssetRole::Council => &self.council,
            AssetRole::Ics => &self.ics,
            AssetRole::PermissionedCandidates => &self.permissioned_candidates,
            AssetRole::Reserve => &self.reserve,
            AssetRole::TechnicalCommittee => &self.technical_committee,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (AssetRole, &Asset)> {
        AssetRole::ALL
            .into_iter()
            .map(|role| (role, self.get(role)))
    }
}

/// Variants are declared in the order of their names, so `Ord` sorts roles as
/// their names sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AssetRole {
    Auth,
    Bridge,
    Cnight,
    Council,
    Ics,
    PermissionedCandidates,
    Reserve,
    TechnicalCommittee,
}

impl AssetRole {
    pub const ALL: [Self; 8] = [
        Self::Auth,
        Self::Bridge,
        Self::Cnight,
        Self::Council,
        Self::Ics,
        Self::PermissionedCandidates,
        Self::Reserve,
        Self::TechnicalCommittee,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Bridge => "bridge",
            Self::Cnight => "cnight",
            Self::Council => "council",
            Self::Ics => "ics",
            Self::PermissionedCandidates => "permissioned_candidates",
            Self::Reserve => "reserve",
            Self::TechnicalCommittee => "technical_committee",
        }
    }
}

/// A governance body: its address and the policy of its token.
#[derive(Debug, Clone, Copy)]
pub struct Governance<'a> {
    pub name: &'static str,
    pub address: &'a str,
    pub asset: &'a Asset,
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

/// The Cardano network magic of a supported network, as a provider's
/// `/genesis` names it.
/// The `multi_asset` IDs of the fixed descriptors of a network, by unit.
/// Mainnet and Preprod carry the IDs their converted databases and published
/// snapshot files hold; another network numbers its units in bytewise order.
/// The table lives beside the profiles, not in them, so that a profile's
/// hash stays the one each database stores.
pub fn fixed_descriptor_ids(profile: &Profile) -> Vec<(String, i64)> {
    use AssetRole::{Auth, Bridge, Cnight, Council, PermissionedCandidates, TechnicalCommittee};
    let order: Vec<String> = match profile.network.as_str() {
        "mainnet" => [
            Cnight,
            PermissionedCandidates,
            Council,
            TechnicalCommittee,
            Auth,
            Bridge,
        ]
        .iter()
        .map(|role| profile.assets.get(*role).unit())
        .collect(),
        "preprod" => [
            TechnicalCommittee,
            Auth,
            Council,
            Cnight,
            Bridge,
            PermissionedCandidates,
        ]
        .iter()
        .map(|role| profile.assets.get(*role).unit())
        .collect(),
        _ => crate::assets::fixed_units(profile).into_iter().collect(),
    };
    let mut ids: Vec<(String, i64)> = Vec::new();
    for unit in order {
        if !ids.iter().any(|(known, _)| *known == unit) {
            let next = ids.len() as i64 + 1;
            ids.push((unit, next));
        }
    }
    ids
}

pub fn network_magic(network: &str) -> Option<u32> {
    match network {
        "mainnet" => Some(764_824_073),
        "preprod" => Some(1),
        _ => None,
    }
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

    /// The council and the technical committee, in that order.
    pub fn governance(&self) -> [Governance<'_>; 2] {
        [
            Governance {
                name: "council",
                address: &self.addresses.council,
                asset: &self.assets.council,
            },
            Governance {
                name: "technical_committee",
                address: &self.addresses.technical_committee,
                asset: &self.assets.technical_committee,
            },
        ]
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
            assert_eq!(profile.provenance.files.len(), 10);
            assert_eq!(profile.hash().unwrap().len(), 64);
            let mut changed = profile.clone();
            changed.cardano.block_stability_margin += 1;
            assert_ne!(profile.hash().unwrap(), changed.hash().unwrap());
        }
    }

    #[test]
    fn each_supported_network_has_its_magic() {
        assert_eq!(network_magic("mainnet"), Some(764_824_073));
        assert_eq!(network_magic("preprod"), Some(1));
        assert_eq!(network_magic("preview"), None);
    }

    /// The profile hash is bound into every database and snapshot file; the
    /// typed roles must serialise exactly as the generated JSON did.
    #[test]
    fn profile_hashes_are_pinned() {
        let preview: Profile =
            serde_json::from_str(include_str!("../profiles/preview.json")).unwrap();
        for (profile, hash) in [
            (
                Profile::load("mainnet").unwrap(),
                "05dd2c46edf6f188f1a9ff45cdd97dcfab937f5ff6352fd71c3adcae3057925a",
            ),
            (
                Profile::load("preprod").unwrap(),
                "d17155cf527bf7e7db83f9815b3a5eb4aa4d554877c75e0d2706781a3e4170d8",
            ),
            (
                preview,
                "83f053464e3edd337877e984fa7aed4e9aff701282093023ff00442530eacb52",
            ),
        ] {
            assert_eq!(profile.hash().unwrap(), hash, "{}", profile.network);
        }
    }

    #[test]
    fn roles_name_every_field_once() {
        let profile = Profile::load("mainnet").unwrap();
        let value = serde_json::to_value(&profile).unwrap();
        let assets = AssetRole::ALL.map(AssetRole::name);
        let addresses = AddressRole::ALL.map(AddressRole::name);
        assert_eq!(
            value["assets"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            assets.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            value["addresses"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            addresses.iter().collect::<Vec<_>>()
        );
        for (role, asset) in profile.assets.iter() {
            assert_eq!(value["assets"][role.name()]["policy"], asset.policy);
        }
        for (role, address) in profile.addresses.iter() {
            assert_eq!(value["addresses"][role.name()], address);
        }
        let mut missing = value.clone();
        missing["assets"].as_object_mut().unwrap().remove("bridge");
        assert!(serde_json::from_value::<Profile>(missing).is_err());
        let mut extra = value;
        extra["addresses"]["other"] = serde_json::json!("addr1");
        assert!(serde_json::from_value::<Profile>(extra).is_err());
    }

    #[test]
    fn preprod_keeps_bridge_genesis_and_config_input_units_distinct() {
        let profile = Profile::load("preprod").unwrap();
        assert_eq!(profile.assets.ics.name, "");
        assert_eq!(profile.assets.reserve.name, "");
        assert_eq!(profile.assets.bridge.name, "4e49474854");
        assert_eq!(profile.assets.bridge.policy, profile.assets.ics.policy);
        assert_ne!(profile.assets.bridge.unit(), profile.assets.ics.unit());
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
