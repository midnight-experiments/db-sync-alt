//! The fake provider's listings have the members of the backend's answers:
//! the recorded Preprod answers in `fixtures/listing-answers.json` against
//! the fake's answers over a generated chain, endpoint by endpoint.

use std::collections::BTreeSet;

use dsa_chain::{assets::AssetRule, profiles::Profile};
use dsa_testkit::{
    chain::Chain,
    generate::{self, Roles, Settings},
};
use serde_json::Value;

fn roles(profile: &Profile) -> Roles {
    let rule = AssetRule::from_profile(profile);
    Roles {
        network: 0,
        addresses: profile
            .addresses
            .iter()
            .map(|(role, address)| (role.name().to_owned(), address.to_owned()))
            .collect(),
        units: profile
            .assets
            .iter()
            .map(|(role, asset)| {
                (
                    role.name().to_owned(),
                    (asset.policy.clone(), asset.name.clone()),
                )
            })
            .collect(),
        eligible_units: rule.exact_units().clone(),
        governance_policies: rule.governance_policies().clone(),
    }
}

fn members(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default()
}

fn first(value: &Value) -> &Value {
    value
        .as_array()
        .and_then(|rows| rows.first())
        .unwrap_or(value)
}

#[test]
fn the_fake_renders_listings_with_the_backends_members() {
    let recorded: Value =
        serde_json::from_str(include_str!("fixtures/listing-answers.json")).unwrap();
    let profile = Profile::load("preprod").unwrap();
    let g = generate::generate(
        &roles(&profile),
        &Settings {
            seed: 11,
            first_height: 1,
            blocks: 80,
            transactions: 5,
            large_datums: false,
        },
    );
    let mut chain = Chain::from_generated(&g, "stake_test1");
    let cnight = profile.assets.cnight.unit();
    let spending = g
        .facts
        .iter()
        .find(|(_, facts)| facts.valid && !facts.inputs.is_empty() && !facts.outputs.is_empty())
        .map(|(hash, _)| hash.clone())
        .unwrap();
    let last = g.blocks.last().unwrap();
    let mut fake = |path: &str| -> Value {
        let answer = chain.answer(&format!("/api/v0{path}"));
        assert_eq!(answer.status, 200, "{path}");
        serde_json::from_str(&answer.body).unwrap()
    };
    let pages = "?count=100&page=1&order=asc";
    let cases = [
        (
            "asset_utxos",
            fake(&format!("/assets/{cnight}/utxos{pages}")),
        ),
        (
            "address_utxos",
            fake(&format!(
                "/addresses/{}/utxos{pages}",
                profile.addresses.mapping
            )),
        ),
        (
            "asset_transactions",
            fake(&format!("/assets/{cnight}/transactions{pages}")),
        ),
        ("transaction_utxos", fake(&format!("/txs/{spending}/utxos"))),
        ("transaction", fake(&format!("/txs/{spending}"))),
        ("asset", fake(&format!("/assets/{cnight}"))),
        (
            "policy_assets",
            fake(&format!(
                "/assets/policy/{}{pages}",
                profile.assets.council.policy
            )),
        ),
        (
            "epoch_blocks",
            fake(&format!(
                "/epochs/{}/blocks?count=1&page=1&order=desc",
                last.epoch
            )),
        ),
        ("block", fake(&format!("/blocks/{}", last.hash))),
    ];
    for (name, answer) in cases {
        let real = &recorded[name]["body"];
        assert_eq!(
            members(first(&answer)),
            members(first(real)),
            "{name}: the members of a row"
        );
        assert_eq!(
            answer.is_array(),
            real.is_array(),
            "{name}: a list or an object"
        );
        if name == "transaction_utxos" {
            for part in ["inputs", "outputs"] {
                assert_eq!(
                    members(first(&answer[part])),
                    members(first(&real[part])),
                    "{name}: the members of its {part}"
                );
            }
        }
        if name == "epoch_blocks" {
            assert!(answer[0].is_string() && real[0].is_string());
        }
    }
}
