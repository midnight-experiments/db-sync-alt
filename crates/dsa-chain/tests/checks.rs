//! Native fact models exercise checks without constructing ledger envelopes.

use std::collections::BTreeMap;

use dsa_chain::{
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
                tx.body = b"changed native body";
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
        let before = held.clone();
        let error = checks::check_block(
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
            "missing input" | "duplicated holding" => assert!(
                matches!(error, CheckError::AssetBalance { height: 10, transaction, .. } if transaction == hash)
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
        assert_eq!(held, before, "failed validation cannot mutate held state");
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
    let effects = checks::check_block(
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
    let effects = checks::check_block(
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
    let effects = checks::check_block(
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

#[test]
fn generated_valid_chains_preserve_conservation_and_supply_on_both_profiles() {
    let mut blocks = 0;
    for network in ["mainnet", "preprod"] {
        let profile = Profile::load(network).unwrap();
        for seed in 0..32u64 {
            let (mut point, mut held) = origin(&profile);
            held.get_mut(&point)
                .unwrap()
                .output
                .assets
                .push(asset(&profile, "auth", 100));
            let mut supply = checks::aggregate_supply(
                &profile,
                9,
                held.values().map(|row| SupplyOutput {
                    output: &row.output,
                    spent: false,
                }),
            )
            .unwrap();
            let (mut previous, _) = headers(1);
            let mut random = seed + 1;
            for height in 10..42u32 {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let invalid = random % 5 == 0;
                let mut next = held[&point].output.clone();
                let mut mint = Vec::new();
                if !invalid {
                    for asset in &mut next.assets {
                        let change = (random % 5) as i128 - 2;
                        let change = change.max(-asset.quantity);
                        asset.quantity += change;
                        if change != 0 {
                            mint.push(Asset {
                                quantity: change,
                                ..asset.clone()
                            });
                        }
                    }
                }
                let body = [
                    seed.to_be_bytes().as_slice(),
                    height.to_be_bytes().as_slice(),
                    &[u8::from(invalid)],
                ]
                .concat();
                let mut tx = transaction(
                    &body,
                    vec![
                        point.clone(),
                        Outpoint {
                            hash: "ff".repeat(32),
                            index: 0,
                        },
                    ],
                    vec![Some(next.clone())],
                    mint,
                );
                if invalid {
                    tx.valid = false;
                    tx.collateral = vec![point.clone()];
                    tx.collateral_output = Some(next);
                    tx.has_collateral_return = true;
                }
                let header = Header {
                    height,
                    hash: transaction::hash(&height.to_be_bytes()),
                    previous_hash: Some(previous.hash.clone()),
                    transaction_count: 1,
                };
                let effects = checks::check_block(
                    &profile,
                    &previous,
                    &header,
                    previous.height,
                    std::slice::from_ref(&tx),
                    &held,
                    &Supplements::default(),
                )
                .unwrap();
                supply = checks::apply_mint_delta(&profile, height, &supply, &effects.mint_delta)
                    .unwrap();
                point = Outpoint {
                    hash: tx.hash.clone(),
                    index: u16::from(invalid),
                };
                rows::build_block(&profile, height, &[tx], &mut held, &Supplements::default())
                    .unwrap();
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
                previous = header;
                blocks += 1;
            }
        }
    }
    assert_eq!(blocks, 2_048);
}
