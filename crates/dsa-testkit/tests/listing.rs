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

/// The member sets of every row of an answer (an object is one row).
fn shapes(value: &Value) -> BTreeSet<BTreeSet<String>> {
    match value.as_array() {
        Some(rows) => rows.iter().map(members).collect(),
        None => BTreeSet::from([members(value)]),
    }
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
            every_role_minted: false,
            flagged_rows: true,
        },
    );
    let mut chain = Chain::from_generated(&g, "stake_test1");
    let cnight = profile.assets.cnight.unit();
    let flagged = g
        .facts
        .iter()
        .find(|(_, facts)| {
            facts.valid && !facts.collateral.is_empty() && facts.collateral_return.is_some()
        })
        .map(|(hash, _)| hash.clone())
        .expect("a valid transaction with collateral");
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
        (
            "transaction_utxos_flagged",
            fake(&format!("/txs/{flagged}/utxos")),
        ),
        (
            "asset_utxos_ranged",
            fake(&format!(
                "/assets/{cnight}/utxos{pages}&from=1&to={}",
                last.height
            )),
        ),
        (
            "address_transactions",
            fake(&format!(
                "/addresses/{}/transactions{pages}",
                profile.addresses.mapping
            )),
        ),
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
        // Every row of the fake's answer and of the recorded one has the
        // same members.
        assert_eq!(shapes(real).len(), 1, "{name}: the recorded rows");
        assert_eq!(
            shapes(&answer),
            shapes(real),
            "{name}: the members of a row"
        );
        assert_eq!(
            answer.is_array(),
            real.is_array(),
            "{name}: a list or an object"
        );
        if name.starts_with("transaction_utxos") {
            for part in ["inputs", "outputs"] {
                assert_eq!(shapes(&real[part]).len(), 1, "{name}: the recorded {part}");
                assert_eq!(
                    shapes(&answer[part]),
                    shapes(&real[part]),
                    "{name}: the members of its {part}"
                );
            }
        }
        if name == "transaction_utxos_flagged" {
            // A valid transaction's collateral input and collateral return
            // are flagged, the return lovelace only and never spent.
            for (row, part) in [
                (&answer, "inputs"),
                (real, "inputs"),
                (&answer, "outputs"),
                (real, "outputs"),
            ] {
                let flagged: Vec<&Value> = row[part]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|row| row["collateral"] == true)
                    .collect();
                assert!(!flagged.is_empty(), "{name}: no flagged row in its {part}");
                for row in flagged {
                    assert_eq!(
                        row["amount"].as_array().map(Vec::len),
                        Some(1),
                        "{name}: {row}"
                    );
                    if part == "outputs" {
                        assert!(row["consumed_by_tx"].is_null(), "{name}: {row}");
                    }
                }
            }
        }
        if name == "epoch_blocks" {
            assert!(answer[0].is_string() && real[0].is_string());
        }
    }
}

/// One answer of the fake, which must be 200.
fn get(chain: &mut Chain, path: &str) -> Value {
    let answer = chain.answer(&format!("/api/v0{path}"));
    assert_eq!(answer.status, 200, "{path}");
    serde_json::from_str(&answer.body).unwrap()
}

/// Every page of a list, oldest first; a 404 is an empty list.
fn every(chain: &mut Chain, path: &str) -> Vec<Value> {
    let mut rows = Vec::new();
    for page in 1.. {
        let answer = chain.answer(&format!("/api/v0{path}?count=100&page={page}&order=asc"));
        if answer.status == 404 {
            break;
        }
        assert_eq!(answer.status, 200, "{path}");
        let answer: Vec<Value> = serde_json::from_str(&answer.body).unwrap();
        let full = answer.len() == 100;
        rows.extend(answer);
        if !full {
            break;
        }
    }
    rows
}

/// Whether a row of `/txs/{hash}/utxos` holds some of `unit`.
fn holds(row: &Value, unit: &str) -> bool {
    row["amount"].as_array().is_some_and(|amounts| {
        amounts
            .iter()
            .any(|amount| amount["unit"] == unit && amount["quantity"] != "0")
    })
}

/// The fake lists what the backend's queries over db-sync's tables list: a
/// unit's transactions are those that create an ordinary output holding it
/// (a complete burn is in its history only), an address's are those with an
/// ordinary input or output there, an invalid transaction shows its
/// collateral inputs as ordinary inputs and its collateral return as its one
/// ordinary output, and a unit's history has one row per mint or burn of a
/// valid transaction, as many as the asset's `mint_or_burn_count`.
#[test]
fn the_fake_lists_as_the_backends_queries() {
    let profile = Profile::load("preprod").unwrap();
    let cnight = profile.assets.cnight.unit();
    let mut seen = (0, 0);
    for seed in 11..60 {
        let g = generate::generate(
            &roles(&profile),
            &Settings {
                seed,
                first_height: 1,
                blocks: 80,
                transactions: 5,
                large_datums: false,
                every_role_minted: false,
                flagged_rows: false,
            },
        );
        let mut chain = Chain::from_generated(&g, "stake_test1");
        let listed: Vec<String> = every(&mut chain, &format!("/assets/{cnight}/transactions"))
            .iter()
            .map(|row| row["tx_hash"].as_str().unwrap().to_owned())
            .collect();
        let history = every(&mut chain, &format!("/assets/{cnight}/history"));
        for row in &history {
            let members: BTreeSet<&str> = row
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(members, BTreeSet::from(["action", "amount", "tx_hash"]));
            let amount: i128 = row["amount"].as_str().unwrap().parse().unwrap();
            assert_eq!(row["action"], if amount < 0 { "burned" } else { "minted" });
        }
        let asset = get(&mut chain, &format!("/assets/{cnight}"));
        assert_eq!(asset["mint_or_burn_count"], history.len());
        let mapping = every(
            &mut chain,
            &format!("/addresses/{}/transactions", profile.addresses.mapping),
        );
        let at_mapping: BTreeSet<&str> = mapping
            .iter()
            .map(|row| row["tx_hash"].as_str().unwrap())
            .collect();
        for block in &g.blocks {
            for built in &block.transactions {
                let Some(facts) = g.facts.get(&built.hash) else {
                    continue;
                };
                let answer = get(&mut chain, &format!("/txs/{}/utxos", built.hash));
                let inputs = answer["inputs"].as_array().unwrap();
                let outputs = answer["outputs"].as_array().unwrap();
                let creates = outputs
                    .iter()
                    .any(|row| row["collateral"] == false && holds(row, &cnight));
                assert_eq!(
                    listed.contains(&built.hash),
                    creates,
                    "{}: listed for cNIGHT exactly when it creates an output holding it",
                    built.hash
                );
                let burnt = history
                    .iter()
                    .any(|row| row["tx_hash"] == built.hash.as_str() && row["action"] == "burned");
                if burnt && !creates {
                    seen.0 += 1;
                }
                let touches = inputs
                    .iter()
                    .chain(outputs)
                    .filter(|row| row["collateral"] == false && row["reference"] != true)
                    .any(|row| row["address"] == profile.addresses.mapping.as_str());
                assert_eq!(at_mapping.contains(built.hash.as_str()), touches);
                if !facts.valid {
                    seen.1 += 1;
                    assert!(
                        inputs
                            .iter()
                            .all(|row| row["collateral"] == false && row["reference"] == false)
                    );
                    assert_eq!(inputs.len(), facts.collateral.len());
                    assert_eq!(
                        outputs.len(),
                        usize::from(facts.collateral_return.is_some())
                    );
                    for row in outputs {
                        assert_eq!(row["collateral"], false);
                        assert_eq!(row["output_index"], facts.outputs.len());
                    }
                    assert!(
                        !history
                            .iter()
                            .any(|row| row["tx_hash"] == built.hash.as_str())
                    );
                }
            }
        }
        if seen.0 > 0 && seen.1 > 0 {
            break;
        }
    }
    assert!(seen.0 > 0, "no complete burn of cNIGHT in the chains read");
    assert!(seen.1 > 0, "no invalid transaction in the chains read");
}
