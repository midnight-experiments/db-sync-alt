//! Native fact models exercise checks without constructing ledger envelopes.

use super::check_facts;

use std::collections::BTreeMap;

use crate::{
    checks::{self, CheckError, Header, SupplyOutput},
    ids,
    profiles::Profile,
    rows::{self, HeldOutput, Supplements},
    transaction::{self, Asset, Outpoint, Output, Transaction},
};

fn asset(profile: &Profile, role: &str, quantity: i128) -> Asset {
    Asset {
        policy: profile.assets[role].policy.clone(),
        name: profile.assets[role].name.clone(),
        quantity,
    }
}
fn output(profile: &Profile, quantity: i128) -> Output {
    Output {
        address_raw: Vec::new(),
        address: Some(profile.addresses["mapping"].clone()),
        coin: 100,
        assets: vec![asset(profile, "cnight", quantity)],
        data_hash: None,
        inline_datum: None,
        classification_safe: true,
    }
}
fn transaction<'a>(
    body: &'a [u8],
    inputs: Vec<Outpoint>,
    outputs: Vec<Option<Output>>,
    mint: Vec<Asset>,
) -> Transaction<'a> {
    Transaction {
        hash: transaction::hash(body),
        valid: true,
        ordinary: inputs,
        collateral: Vec::new(),
        reference: Vec::new(),
        outputs,
        collateral_output: None,
        has_collateral_return: false,
        mint,
        datums: BTreeMap::new(),
        body,
        auxiliary: None,
        auxiliary_hash: None,
    }
}
fn headers(count: usize) -> (Header, Header) {
    let previous = Header {
        height: 9,
        hash: "01".repeat(32),
        previous_hash: None,
        transaction_count: 0,
    };
    let next = Header {
        height: 10,
        hash: "02".repeat(32),
        previous_hash: Some(previous.hash.clone()),
        transaction_count: count,
    };
    (previous, next)
}
fn origin(profile: &Profile) -> (Outpoint, BTreeMap<Outpoint, HeldOutput>) {
    let point = Outpoint {
        hash: "03".repeat(32),
        index: 0,
    };
    (
        point.clone(),
        BTreeMap::from([(
            point,
            HeldOutput {
                transaction_id: ids::transaction_id(1, 0).unwrap(),
                output: output(profile, 100),
            },
        )]),
    )
}

#[test]
fn each_planted_block_fault_names_the_block_or_transaction() {
    let profile = Profile::load("mainnet").unwrap();
    for fault in [
        "missing input",
        "duplicated holding",
        "dropped transaction",
        "changed body",
        "header height",
        "header parent",
        "old target",
    ] {
        let (point, mut held) = origin(&profile);
        let (previous, mut header) = headers(1);
        let mut tx = transaction(
            b"native conservation model",
            vec![point.clone()],
            vec![Some(output(&profile, 100))],
            Vec::new(),
        );
        let hash = tx.hash.clone();
        let mut target = previous.height;
        match fault {
            "missing input" => {
                held.remove(&point);
            }
            "duplicated holding" => {
                let duplicate = held[&point].output.assets[0].clone();
                held.get_mut(&point).unwrap().output.assets.push(duplicate);
            }
            "changed body" => {
                tx.body = b"native conservation modeX";
            }
            "header height" => {
                header.height += 1;
            }
            "header parent" => {
                header.previous_hash = Some("ff".repeat(32));
            }
            "old target" => {
                target = header.height;
            }
            _ => {}
        }
        let transactions = if fault == "dropped transaction" {
            Vec::new()
        } else {
            vec![tx]
        };
        let error = check_facts(
            &profile,
            &previous,
            &header,
            target,
            &transactions,
            &held,
            &Supplements::default(),
        )
        .unwrap_err();
        match fault {
            "missing input" | "duplicated holding" => assert_eq!(
                error,
                CheckError::AssetBalance {
                    height: 10,
                    transaction: hash,
                    unit: profile.assets["cnight"].unit(),
                    inputs: if fault == "missing input" { 0 } else { 200 },
                    mint: 0,
                    outputs: 100,
                }
            ),
            "dropped transaction" => assert_eq!(
                error,
                CheckError::TransactionCount {
                    height: 10,
                    expected: 1,
                    actual: 0
                }
            ),
            "changed body" => assert_eq!(
                error,
                CheckError::BodyHash {
                    height: 10,
                    transaction: hash
                }
            ),
            "header height" => assert_eq!(
                error,
                CheckError::HeaderHeight {
                    height: 11,
                    expected: 10
                }
            ),
            "header parent" => assert_eq!(error, CheckError::HeaderParent { height: 10 }),
            "old target" => assert_eq!(
                error,
                CheckError::NotAboveTarget {
                    height: 10,
                    target: 10
                }
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn supply_detects_an_unbooked_mint_and_excludes_old_spent_carry_records() {
    let profile = Profile::load("mainnet").unwrap();
    let (point, mut held) = origin(&profile);
    let before = checks::aggregate_supply(
        &profile,
        9,
        held.values().map(|row| SupplyOutput {
            output: &row.output,
            spent: false,
        }),
    )
    .unwrap();
    let mut old_carry = output(&profile, 999_999);
    old_carry.address = Some(profile.addresses["council"].clone());
    let (previous, header) = headers(1);
    let tx = transaction(
        b"native minted model",
        vec![point],
        vec![Some(output(&profile, 105))],
        vec![asset(&profile, "cnight", 5)],
    );
    let effects = check_facts(
        &profile,
        &previous,
        &header,
        9,
        std::slice::from_ref(&tx),
        &held,
        &Supplements::default(),
    )
    .unwrap();
    rows::build_block(&profile, 10, &[tx], &mut held, &Supplements::default()).unwrap();
    let actual = checks::aggregate_supply(
        &profile,
        10,
        held.values()
            .map(|row| SupplyOutput {
                output: &row.output,
                spent: false,
            })
            .chain([SupplyOutput {
                output: &old_carry,
                spent: true,
            }]),
    )
    .unwrap();
    let unbooked = checks::apply_mint_delta(&profile, 10, &before, &BTreeMap::new()).unwrap();
    assert!(matches!(
        checks::check_supply(10, &unbooked, &actual).unwrap_err(),
        CheckError::SupplyMismatch {
            height: 10,
            expected: 100,
            actual: 105,
            ..
        }
    ));
    let booked = checks::apply_mint_delta(&profile, 10, &before, &effects.mint_delta).unwrap();
    checks::check_supply(10, &booked, &actual).unwrap();
    assert_eq!(
        actual[&profile.assets["cnight"].unit()],
        105,
        "profile aliases and old spent retained rows cannot add supply"
    );
}

#[test]
fn untracked_ordinary_assets_and_inputs_do_not_trigger_profile_conservation() {
    let profile = Profile::load("preprod").unwrap();
    let mut unrelated = output(&profile, 0);
    unrelated.assets = vec![Asset {
        policy: "ff".repeat(28),
        name: "00".to_owned(),
        quantity: 42,
    }];
    unrelated.address = Some("unrelated address".to_owned());
    let tx = transaction(
        b"native untracked model",
        vec![Outpoint {
            hash: "04".repeat(32),
            index: 0,
        }],
        vec![Some(unrelated)],
        Vec::new(),
    );
    let (previous, header) = headers(1);
    let effects = check_facts(
        &profile,
        &previous,
        &header,
        9,
        &[tx],
        &BTreeMap::new(),
        &Supplements::default(),
    )
    .unwrap();
    assert!(effects.mint_delta.is_empty());
    assert_eq!(effects.transactions_checked, 1);
}

#[test]
fn effective_collateral_and_same_block_spends_share_the_row_builder_rules() {
    let profile = Profile::load("mainnet").unwrap();
    let (point, held) = origin(&profile);
    let first = transaction(
        b"native first",
        vec![point],
        vec![Some(output(&profile, 100))],
        Vec::new(),
    );
    let first_point = Outpoint {
        hash: first.hash.clone(),
        index: 0,
    };
    let mut invalid = transaction(
        b"native invalid projection",
        vec![Outpoint {
            hash: "ee".repeat(32),
            index: 0,
        }],
        vec![Some(output(&profile, 999))],
        vec![asset(&profile, "cnight", 999)],
    );
    invalid.valid = false;
    invalid.collateral = vec![first_point];
    invalid.has_collateral_return = true;
    invalid.collateral_output = Some(output(&profile, 100));
    let transactions = [first, invalid];
    let (previous, header) = headers(2);
    let effects = check_facts(
        &profile,
        &previous,
        &header,
        9,
        &transactions,
        &held,
        &Supplements::default(),
    )
    .unwrap();
    assert!(
        effects.mint_delta.is_empty(),
        "invalid mint and ordinary outputs have no effect"
    );
    let mut actual = held;
    let rows = rows::build_block(
        &profile,
        10,
        &transactions,
        &mut actual,
        &Supplements::default(),
    )
    .unwrap();
    assert_eq!(rows.tx_in.len(), 2);
    assert_eq!(rows.tx_out.last().unwrap().index, 1);
    assert_eq!(actual.len(), 1);
}

fn next_random(random: &mut u64) -> u64 {
    *random = random
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *random >> 17
}

fn clone_fact<'a>(tx: &Transaction<'a>) -> Transaction<'a> {
    Transaction {
        hash: tx.hash.clone(),
        valid: tx.valid,
        ordinary: tx.ordinary.clone(),
        collateral: tx.collateral.clone(),
        reference: tx.reference.clone(),
        outputs: tx.outputs.clone(),
        collateral_output: tx.collateral_output.clone(),
        has_collateral_return: tx.has_collateral_return,
        mint: tx.mint.clone(),
        datums: tx.datums.clone(),
        body: tx.body,
        auxiliary: tx.auxiliary,
        auxiliary_hash: tx.auxiliary_hash.clone(),
    }
}

#[test]
fn generated_valid_chains_preserve_conservation_and_supply_on_both_profiles() {
    let mut blocks = 0;
    let mut transactions_checked = 0;
    let mut coverage = [0usize; 6];
    for network in ["mainnet", "preprod"] {
        let profile = Profile::load(network).unwrap();
        let mut units: BTreeMap<String, Asset> = crate::assets::fixed_units(&profile)
            .into_iter()
            .map(|unit| {
                let entry = Asset {
                    policy: unit[..56].to_owned(),
                    name: unit[56..].to_owned(),
                    quantity: 0,
                };
                (unit, entry)
            })
            .collect();
        let alternate = Asset {
            policy: profile.assets["council"].policy.clone(),
            name: "ff00".to_owned(),
            quantity: 0,
        };
        let foreign = Asset {
            policy: "ff".repeat(28),
            name: "01".to_owned(),
            quantity: 0,
        };
        units.insert(alternate.unit(), alternate);
        units.insert(foreign.unit(), foreign.clone());
        for seed in 0..32u64 {
            let mut held = BTreeMap::new();
            let mut supply: BTreeMap<String, i128> = crate::assets::fixed_units(&profile)
                .into_iter()
                .map(|unit| (unit, 0))
                .collect();
            for root in 0..3u16 {
                let mut value = output(&profile, 0);
                value.assets.clear();
                for (index, (unit, template)) in units.iter().enumerate() {
                    let quantity = 11 + i128::from(root) * 6 + index as i128;
                    value.assets.push(Asset {
                        quantity,
                        ..template.clone()
                    });
                    if let Some(total) = supply.get_mut(unit) {
                        *total += quantity;
                    }
                }
                held.insert(
                    Outpoint {
                        hash: format!("{:064x}", seed + 100),
                        index: root,
                    },
                    HeldOutput {
                        transaction_id: 1,
                        output: value,
                    },
                );
            }
            // An absolute independently specified baseline: 11+17+23 + 3*i.
            for (index, unit) in units.keys().enumerate() {
                if let Some(total) = supply.get(unit) {
                    assert_eq!(*total, 51 + 3 * index as i128);
                }
            }
            let initial_actual = checks::aggregate_supply(
                &profile,
                9,
                held.values().map(|row| SupplyOutput {
                    output: &row.output,
                    spent: false,
                }),
            )
            .unwrap();
            checks::check_supply(9, &supply, &initial_actual).unwrap();
            let (mut previous, _) = headers(1);
            let mut random = seed + 1;
            let fault_height = 10 + next_random(&mut random) % 32;
            let mut carry = held.keys().next().unwrap().clone();
            for height in 10..42u32 {
                let count = 1 + (next_random(&mut random) % 6) as usize;
                coverage[count - 1] += 1;
                let bodies: Vec<_> = (0..count)
                    .map(|index| format!("native {network} {seed} {height} {index}").into_bytes())
                    .collect();
                let mut expected = held.clone();
                let mut expected_delta: BTreeMap<String, i128> = BTreeMap::new();
                let mut txs = Vec::new();
                for body in &bodies {
                    let mut inputs = vec![carry.clone()];
                    let alternatives: Vec<_> = expected
                        .keys()
                        .filter(|point| **point != carry)
                        .cloned()
                        .collect();
                    if !alternatives.is_empty() {
                        inputs.push(
                            alternatives[(next_random(&mut random) as usize) % alternatives.len()]
                                .clone(),
                        );
                    }
                    let mut balances: BTreeMap<String, i128> =
                        units.keys().cloned().map(|unit| (unit, 0)).collect();
                    for point in &inputs {
                        for value in &expected[point].output.assets {
                            *balances.get_mut(&value.unit()).unwrap() += value.quantity;
                        }
                    }
                    let invalid = next_random(&mut random) % 5 == 0;
                    let mut mint = Vec::new();
                    if !invalid {
                        for (unit, quantity) in &mut balances {
                            if unit == &foreign.unit() {
                                continue;
                            }
                            let draw = next_random(&mut random);
                            let change = if draw % 13 == 0 {
                                -*quantity
                            } else {
                                ((draw % 7) as i128 - 3).max(-*quantity)
                            };
                            *quantity += change;
                            if change != 0 {
                                mint.push(Asset {
                                    quantity: change,
                                    ..units[unit].clone()
                                });
                                if supply.contains_key(unit) {
                                    *expected_delta.entry(unit.clone()).or_default() += change;
                                }
                            }
                        }
                    } else {
                        // This ignored mint is independent of collateral effects.
                        mint.push(asset(&profile, "cnight", 17));
                    }
                    let mut left = output(&profile, 0);
                    left.assets.clear();
                    let mut right = left.clone();
                    for (unit, quantity) in balances {
                        let split = if invalid {
                            quantity
                        } else {
                            i128::from(next_random(&mut random) % ((quantity + 1) as u64))
                        };
                        if split > 0 {
                            left.assets.push(Asset {
                                quantity: split,
                                ..units[&unit].clone()
                            });
                        }
                        if quantity - split > 0 {
                            right.assets.push(Asset {
                                quantity: quantity - split,
                                ..units[&unit].clone()
                            });
                        }
                    }
                    let mut unrelated = output(&profile, 0);
                    unrelated.address = Some("ordinary foreign".to_owned());
                    unrelated.assets = vec![Asset {
                        quantity: 7,
                        ..foreign.clone()
                    }];
                    let mut tx = transaction(
                        body,
                        inputs.clone(),
                        vec![Some(left.clone()), Some(right.clone()), Some(unrelated)],
                        mint,
                    );
                    if invalid {
                        tx.valid = false;
                        tx.collateral = inputs.clone();
                        tx.ordinary = vec![Outpoint {
                            hash: "ee".repeat(32),
                            index: 0,
                        }];
                        tx.outputs = vec![Some(output(&profile, 999)), None];
                        tx.has_collateral_return = true;
                        tx.collateral_output = Some(left.clone());
                    }
                    for point in &inputs {
                        expected.remove(point);
                    }
                    let id = (i64::from(height) << 12) | txs.len() as i64;
                    let first_index = if invalid { 2 } else { 0 };
                    carry = Outpoint {
                        hash: tx.hash.clone(),
                        index: first_index,
                    };
                    expected.insert(
                        carry.clone(),
                        HeldOutput {
                            transaction_id: id,
                            output: left,
                        },
                    );
                    if !invalid {
                        expected.insert(
                            Outpoint {
                                hash: tx.hash.clone(),
                                index: 1,
                            },
                            HeldOutput {
                                transaction_id: id,
                                output: right,
                            },
                        );
                    }
                    txs.push(tx);
                }
                let header = Header {
                    height,
                    hash: transaction::hash(&height.to_be_bytes()),
                    previous_hash: Some(previous.hash.clone()),
                    transaction_count: txs.len(),
                };
                if u64::from(height) == fault_height {
                    let position = (next_random(&mut random) as usize) % txs.len();
                    let mut faulted: Vec<_> = txs.iter().map(clone_fact).collect();
                    match seed % 7 {
                        0 | 1 => {
                            // Fault the trusted starting inventory at this random
                            // chain position; the first tx spends this held item.
                            let point = txs[0].effective_inputs()[0].clone();
                            let mut broken = held.clone();
                            if seed % 7 == 0 {
                                broken.remove(&point);
                            } else {
                                let duplicate = broken[&point]
                                    .output
                                    .assets
                                    .iter()
                                    .find(|asset| {
                                        crate::assets::AssetRule::from_profile(&profile)
                                            .allows(&asset.policy, &asset.name)
                                    })
                                    .unwrap()
                                    .clone();
                                broken
                                    .get_mut(&point)
                                    .unwrap()
                                    .output
                                    .assets
                                    .push(duplicate);
                            }
                            assert!(
                                matches!(check_facts(&profile,&previous,&header,previous.height,&txs,&broken,&Supplements::default()),Err(CheckError::AssetBalance{height:h,transaction:ref hash,..}) if h==height&&hash==&txs[0].hash)
                            );
                        }
                        2 => {
                            faulted.remove(position);
                            assert_eq!(
                                check_facts(
                                    &profile,
                                    &previous,
                                    &header,
                                    previous.height,
                                    &faulted,
                                    &held,
                                    &Supplements::default()
                                )
                                .unwrap_err(),
                                CheckError::TransactionCount {
                                    height,
                                    expected: txs.len(),
                                    actual: txs.len() - 1
                                }
                            );
                        }
                        3 => {
                            faulted[position].body = b"native changed bytes";
                            assert_eq!(
                                check_facts(
                                    &profile,
                                    &previous,
                                    &header,
                                    previous.height,
                                    &faulted,
                                    &held,
                                    &Supplements::default()
                                )
                                .unwrap_err(),
                                CheckError::BodyHash {
                                    height,
                                    transaction: txs[position].hash.clone()
                                }
                            );
                        }
                        4 => {
                            let mut bad = header.clone();
                            bad.height += 1;
                            assert_eq!(
                                check_facts(
                                    &profile,
                                    &previous,
                                    &bad,
                                    previous.height,
                                    &txs,
                                    &held,
                                    &Supplements::default()
                                )
                                .unwrap_err(),
                                CheckError::HeaderHeight {
                                    height: height + 1,
                                    expected: height
                                }
                            );
                        }
                        5 => {
                            let mut bad = header.clone();
                            bad.previous_hash = None;
                            assert_eq!(
                                check_facts(
                                    &profile,
                                    &previous,
                                    &bad,
                                    previous.height,
                                    &txs,
                                    &held,
                                    &Supplements::default()
                                )
                                .unwrap_err(),
                                CheckError::HeaderParent { height }
                            );
                        }
                        _ => {
                            let bad = &mut faulted[position];
                            let effective = if bad.valid {
                                bad.outputs[0].as_mut().unwrap()
                            } else {
                                bad.collateral_output.as_mut().unwrap()
                            };
                            effective.assets.push(asset(&profile, "cnight", 1));
                            assert!(
                                matches!(check_facts(&profile,&previous,&header,previous.height,&faulted,&held,&Supplements::default()),Err(CheckError::AssetBalance{height:h,transaction:ref hash,..}) if h==height&&hash==&txs[position].hash)
                            );
                        }
                    }
                }
                let effects = check_facts(
                    &profile,
                    &previous,
                    &header,
                    previous.height,
                    &txs,
                    &held,
                    &Supplements::default(),
                )
                .unwrap();
                assert_eq!(effects.transactions_checked, count);
                assert_eq!(effects.mint_delta, expected_delta);
                let unbooked =
                    checks::apply_mint_delta(&profile, height, &supply, &BTreeMap::new()).unwrap();
                supply = checks::apply_mint_delta(&profile, height, &supply, &effects.mint_delta)
                    .unwrap();
                rows::build_block(&profile, height, &txs, &mut held, &Supplements::default())
                    .unwrap();
                assert_eq!(
                    held, expected,
                    "independent model vs shared held transition"
                );
                let actual = checks::aggregate_supply(
                    &profile,
                    height,
                    held.values().map(|row| SupplyOutput {
                        output: &row.output,
                        spent: false,
                    }),
                )
                .unwrap();
                checks::check_supply(height, &supply, &actual).unwrap();
                if u64::from(height) == fault_height
                    && expected_delta.values().any(|delta| *delta != 0)
                {
                    assert!(
                        matches!(checks::check_supply(height,&unbooked,&actual),Err(CheckError::SupplyMismatch{height:h,..}) if h==height)
                    );
                }
                assert_eq!(
                    check_facts(
                        &profile,
                        &previous,
                        &header,
                        height,
                        &txs,
                        &held,
                        &Supplements::default()
                    )
                    .unwrap_err(),
                    CheckError::NotAboveTarget {
                        height,
                        target: height
                    }
                );
                previous = header;
                blocks += 1;
                transactions_checked += count;
            }
        }
    }
    assert_eq!(blocks, 2_048);
    assert!(transactions_checked > 6_000);
    assert!(
        coverage.iter().all(|count| *count > 100),
        "all one-to-six transaction block shapes occur: {coverage:?}"
    );
}

#[test]
fn a_failure_in_the_last_transaction_returns_no_effects_or_partial_rows() {
    let profile = Profile::load("preprod").unwrap();
    let (mut point, mut held) = origin(&profile);
    let before = held.clone();
    let bodies: Vec<_> = (0..6)
        .map(|n| format!("native last-failure {n}").into_bytes())
        .collect();
    let mut txs = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        let quantity = if index == 5 { 101 } else { 100 };
        let tx = transaction(
            body,
            vec![point],
            vec![Some(output(&profile, quantity))],
            Vec::new(),
        );
        point = Outpoint {
            hash: tx.hash.clone(),
            index: 0,
        };
        txs.push(tx);
    }
    let (previous, header) = headers(txs.len());
    assert_eq!(
        check_facts(
            &profile,
            &previous,
            &header,
            9,
            &txs,
            &held,
            &Supplements::default()
        )
        .unwrap_err(),
        CheckError::AssetBalance {
            height: 10,
            transaction: txs[5].hash.clone(),
            unit: profile.assets["cnight"].unit(),
            inputs: 100,
            mint: 0,
            outputs: 101
        }
    );
    // A late rendering failure after five row-producing transactions must also
    // discard the block overlay and preserve the retry's entire starting set.
    txs[5].outputs[0].as_mut().unwrap().classification_safe = false;
    assert!(rows::build_block(&profile, 10, &txs, &mut held, &Supplements::default()).is_err());
    assert_eq!(held, before);
    txs[5].outputs[0] = Some(output(&profile, 100));
    let effects = check_facts(
        &profile,
        &previous,
        &header,
        9,
        &txs,
        &held,
        &Supplements::default(),
    )
    .unwrap();
    assert_eq!(effects.transactions_checked, 6);
    assert!(effects.mint_delta.is_empty());
    rows::build_block(&profile, 10, &txs, &mut held, &Supplements::default()).unwrap();
    assert_eq!(held.len(), 1);
    assert!(held.contains_key(&point));
}

#[test]
fn distinct_names_governance_mints_burn_to_zero_and_invalid_collateral_loss() {
    let profile = Profile::load("preprod").unwrap();
    assert_ne!(
        profile.assets["cnight"].unit(),
        profile.assets["bridge"].unit()
    );
    let (point, mut held) = origin(&profile);
    let mut alternate = asset(&profile, "council", 11);
    alternate.name = "ffff".to_owned();
    held.get_mut(&point).unwrap().output.assets = vec![
        asset(&profile, "cnight", 100),
        asset(&profile, "bridge", 7),
        alternate.clone(),
    ];
    let mut next = held[&point].output.clone();
    next.assets[0].quantity = 98;
    next.assets[1].quantity = 10;
    next.assets[2].quantity = 13;
    let mut alt_mint = alternate.clone();
    alt_mint.quantity = 2;
    let tx = transaction(
        b"native independent names",
        vec![point.clone()],
        vec![Some(next.clone())],
        vec![
            asset(&profile, "cnight", -2),
            asset(&profile, "bridge", 3),
            alt_mint,
        ],
    );
    let (previous, header) = headers(1);
    let effects = check_facts(
        &profile,
        &previous,
        &header,
        9,
        &[tx],
        &held,
        &Supplements::default(),
    )
    .unwrap();
    assert_eq!(
        effects.mint_delta,
        BTreeMap::from([
            (profile.assets["cnight"].unit(), -2),
            (profile.assets["bridge"].unit(), 3)
        ]),
        "all governance names conserve, only six fixed full units have supply entries"
    );
    let mut wrong = next.clone();
    wrong.assets[0].quantity = 101;
    wrong.assets[1].quantity = 7;
    let tx = transaction(
        b"native cannot offset names",
        vec![point.clone()],
        vec![Some(wrong)],
        Vec::new(),
    );
    assert!(
        matches!(check_facts(&profile,&previous,&header,9,&[tx],&held,&Supplements::default()),Err(CheckError::AssetBalance{unit,..}) if unit==profile.assets["cnight"].unit())
    );
    let burn = transaction(
        b"native burn all",
        vec![point.clone()],
        vec![Some(Output {
            assets: Vec::new(),
            ..output(&profile, 0)
        })],
        held[&point]
            .output
            .assets
            .iter()
            .map(|value| Asset {
                quantity: -value.quantity,
                ..value.clone()
            })
            .collect(),
    );
    check_facts(
        &profile,
        &previous,
        &header,
        9,
        &[burn],
        &held,
        &Supplements::default(),
    )
    .unwrap();
    let mut invalid = transaction(
        b"native lost collateral token",
        Vec::new(),
        vec![Some(output(&profile, 777))],
        vec![asset(&profile, "cnight", 100)],
    );
    invalid.valid = false;
    invalid.collateral = vec![point];
    invalid.has_collateral_return = true;
    invalid.collateral_output = Some(Output {
        assets: vec![asset(&profile, "bridge", 7), alternate.clone()],
        ..output(&profile, 0)
    });
    assert_eq!(
        check_facts(
            &profile,
            &previous,
            &header,
            9,
            &[invalid],
            &held,
            &Supplements::default()
        )
        .unwrap_err(),
        CheckError::AssetBalance {
            height: 10,
            transaction: transaction::hash(b"native lost collateral token"),
            unit: profile.assets["cnight"].unit(),
            inputs: 100,
            mint: 0,
            outputs: 0
        }
    );
    let mint_without_output = transaction(
        b"native missing mint output",
        Vec::new(),
        Vec::new(),
        vec![asset(&profile, "cnight", 1)],
    );
    assert_eq!(
        check_facts(
            &profile,
            &previous,
            &header,
            9,
            &[mint_without_output],
            &BTreeMap::new(),
            &Supplements::default()
        )
        .unwrap_err(),
        CheckError::AssetBalance {
            height: 10,
            transaction: transaction::hash(b"native missing mint output"),
            unit: profile.assets["cnight"].unit(),
            inputs: 0,
            mint: 1,
            outputs: 0
        }
    );
}

#[test]
fn duplicate_position_supply_units_and_reachable_arithmetic_errors_are_typed() {
    let profile = Profile::load("mainnet").unwrap();
    let (previous, header) = headers(2);
    let tx = transaction(b"native duplicate", Vec::new(), Vec::new(), Vec::new());
    assert_eq!(
        check_facts(
            &profile,
            &previous,
            &header,
            9,
            &[clone_fact(&tx), tx],
            &BTreeMap::new(),
            &Supplements::default()
        )
        .unwrap_err(),
        CheckError::DuplicateTransaction {
            height: 10,
            transaction: transaction::hash(b"native duplicate")
        }
    );
    let mut bad = header.clone();
    bad.height = ids::HEIGHT_LIMIT;
    assert_eq!(
        checks::check_header(&previous, &bad).unwrap_err(),
        CheckError::Position {
            height: ids::HEIGHT_LIMIT
        }
    );
    let empty = checks::aggregate_supply(&profile, 10, []).unwrap();
    assert_eq!(
        checks::apply_mint_delta(&profile, 10, &BTreeMap::new(), &BTreeMap::new()).unwrap_err(),
        CheckError::SupplyUnits { height: 10 }
    );
    assert_eq!(
        checks::apply_mint_delta(
            &profile,
            10,
            &empty,
            &BTreeMap::from([("unknown".to_owned(), 1)])
        )
        .unwrap_err(),
        CheckError::SupplyUnits { height: 10 }
    );
    assert_eq!(
        checks::check_supply(10, &empty, &BTreeMap::new()).unwrap_err(),
        CheckError::SupplyUnits { height: 10 }
    );
    for (start, delta) in [(0, -1), (i128::MAX, 1)] {
        let mut supply = empty.clone();
        let unit = profile.assets["cnight"].unit();
        supply.insert(unit.clone(), start);
        assert_eq!(
            checks::apply_mint_delta(
                &profile,
                10,
                &supply,
                &BTreeMap::from([(unit.clone(), delta)])
            )
            .unwrap_err(),
            CheckError::AssetQuantity {
                height: 10,
                transaction: None,
                unit
            }
        );
    }
    let (previous, header) = headers(1);
    for (quantity, minting) in [(u64::MAX as i128 + 1, false), (i64::MAX as i128 + 1, true)] {
        let outputs = if minting {
            Vec::new()
        } else {
            vec![Some(output(&profile, quantity))]
        };
        let mint = if minting {
            vec![asset(&profile, "cnight", quantity)]
        } else {
            Vec::new()
        };
        let tx = transaction(b"native quantity range", Vec::new(), outputs, mint);
        assert_eq!(
            check_facts(
                &profile,
                &previous,
                &header,
                9,
                &[tx],
                &BTreeMap::new(),
                &Supplements::default()
            )
            .unwrap_err(),
            CheckError::AssetQuantity {
                height: 10,
                transaction: Some(transaction::hash(b"native quantity range")),
                unit: profile.assets["cnight"].unit()
            }
        );
    }
    // i128 accumulation overflow is unreachable under the decoded transaction's
    // bounded input/output count and u64/i64 item domains. Stored supply addition
    // is the reachable i128 boundary above, and must remain checked.
}
