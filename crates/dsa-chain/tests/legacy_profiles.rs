use std::{collections::BTreeSet, fs, path::Path};

use dsa_chain::profiles::Profile;
use serde_json::Value;

fn json(path: &Path, name: &str) -> Value {
    serde_json::from_slice(&fs::read(path.join(format!("{name}.json"))).unwrap()).unwrap()
}

#[test]
fn mainnet_and_preprod_match_legacy_profiles() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../profiles");
    for (network, folder) in [
        ("mainnet", "mainnet-node-1.0.0"),
        ("preprod", "preprod-node-1.0.400"),
    ] {
        let profile = Profile::load(network).unwrap();
        let source = root.join(folder);
        let cn = json(&source, "cnight-config");
        let gov = json(&source, "federated-authority-config");
        let permissioned = json(&source, "permissioned-candidates-config");
        let ics = json(&source, "ics-config");
        let reserve = json(&source, "reserve-config");
        let old = json(&source, "network");
        let text = |value: &Value| value.as_str().unwrap().to_owned();
        let expected_addresses: BTreeSet<_> = [
            text(&cn["addresses"]["mapping_validator_address"]),
            text(&gov["council"]["address"]),
            text(&gov["technical_committee"]["address"]),
            text(&ics["illiquid_circulation_supply_validator_address"]),
            text(&reserve["reserve_validator_address"]),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            profile.addresses.values().cloned().collect::<BTreeSet<_>>(),
            expected_addresses
        );
        let asset_unit = |asset: &Value| {
            text(&asset["policy_id"])
                .trim_start_matches("0x")
                .to_owned()
                + &hex::encode(asset["asset_name"].as_str().unwrap().as_bytes())
        };
        let (_, address) = bech32::decode(
            cn["addresses"]["mapping_validator_address"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let expected_units: BTreeSet<_> = [
            text(&cn["addresses"]["cnight_policy_id"])
                + &hex::encode(
                    cn["addresses"]["cnight_asset_name"]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                ),
            hex::encode(&address[1..]),
            text(&permissioned["permissioned_candidates_policy_id"])
                .trim_start_matches("0x")
                .to_owned(),
            text(&gov["council"]["policy_id"]),
            text(&gov["technical_committee"]["policy_id"]),
            asset_unit(&ics["asset"]),
            asset_unit(&reserve["asset"]),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            profile
                .assets
                .values()
                .map(|asset| asset.unit())
                .collect::<BTreeSet<_>>(),
            expected_units
        );
        assert_eq!(
            u64::from(profile.start.block_number),
            old["midnight"]["start"]["block_number"].as_u64().unwrap()
        );
        assert_eq!(
            u64::from(profile.start.tx_index_in_block),
            old["midnight"]["start"]["tx_index_in_block"]
                .as_u64()
                .unwrap()
        );
        assert_eq!(
            profile.start.block_hash,
            text(&cn["next_cardano_position"]["block_hash"]).trim_start_matches("0x")
        );
        assert_eq!(
            profile.start.block_timestamp_millis,
            cn["next_cardano_position"]["block_timestamp"]
                .as_u64()
                .unwrap()
        );
        assert_eq!(
            u64::from(profile.cardano.security_parameter),
            old["cardano"]["security_parameter"].as_u64().unwrap()
        );
        assert_eq!(
            profile.cardano.active_slots_coefficient,
            old["cardano"]["active_slots_coefficient"].as_f64().unwrap()
        );
        let epochs = &old["cardano"]["epochs"];
        assert_eq!(
            serde_json::json!({
                "first_epoch_number": profile.cardano.first_epoch_number,
                "first_epoch_timestamp_millis": profile.cardano.first_epoch_timestamp_millis,
                "epoch_duration_millis": profile.cardano.epoch_duration_millis,
                "first_slot_number": profile.cardano.first_slot_number,
                "slot_duration_millis": profile.cardano.slot_duration_millis,
            }),
            *epochs
        );
        assert_eq!(profile.cardano.block_stability_margin, 30);
        assert_eq!(old["midnight"]["block_stability_margin"], 10);
    }
}
