//! `process_block` on built transactions: checks, rows, held-output changes,
//! supplements and observation candidates through the public functions.

use std::collections::BTreeMap;

use dsa_chain::{
    assets::fixed_units,
    block::{
        BlockError, BlockOutput, DatumAvailability, DatumRow, Header, Held, Outcome, Selection,
        SupplementRequest, Supplements, check_header, inputs, process_block,
    },
    datum, ids,
    observation::{Shape, Source},
    profiles::Profile,
    transaction::{self, Asset, Outpoint, Transaction},
};
use dsa_testkit::tx::{
    Built, Output, Tx, enterprise_address, hash, int_datum, large_datum, pointer_address,
    recipient_metadatum,
};

fn decode(built: &[Built]) -> Vec<Transaction<'_>> {
    transaction::decode_all(
        built
            .iter()
            .map(|tx| (tx.hash.as_str(), tx.cbor.as_slice())),
    )
    .unwrap()
}

fn header(height: u32, transaction_count: usize) -> Header {
    Header {
        height,
        hash: hash(&height.to_be_bytes()),
        previous_hash: Some(hash(&(height - 1).to_be_bytes())),
        transaction_count,
    }
}

fn run(
    profile: &Profile,
    height: u32,
    built: &[Built],
    held: &BTreeMap<Outpoint, Held>,
    supplements: &Supplements,
) -> Result<Outcome, BlockError> {
    process_block(
        &Selection::new(profile),
        &header(height, built.len()),
        &decode(built),
        held,
        supplements,
    )
}

fn ready(outcome: Result<Outcome, BlockError>) -> BlockOutput {
    match outcome.unwrap() {
        Outcome::Ready(output) => *output,
        Outcome::Needs(needs) => panic!("unexpected needs {needs:?}"),
    }
}

fn needs(outcome: Result<Outcome, BlockError>) -> Vec<SupplementRequest> {
    match outcome.unwrap() {
        Outcome::Needs(needs) => needs,
        Outcome::Ready(_) => panic!("unexpected ready block"),
    }
}

fn apply(held: &mut BTreeMap<Outpoint, Held>, output: &BlockOutput) {
    for (point, entry) in &output.created {
        held.insert(point.clone(), entry.clone());
    }
    for point in &output.spent {
        held.remove(point);
    }
}

fn unit(profile: &Profile, role: &str) -> (String, String) {
    let (_, asset) = profile
        .assets
        .iter()
        .find(|(candidate, _)| candidate.name() == role)
        .expect("profile asset role");
    (asset.policy.clone(), asset.name.clone())
}

fn address_of<'a>(profile: &'a Profile, role: &str) -> &'a str {
    profile
        .addresses
        .iter()
        .find(|(candidate, _)| candidate.name() == role)
        .map(|(_, address)| address)
        .expect("profile address role")
}

fn asset(profile: &Profile, role: &str, quantity: i128) -> Asset {
    let (policy, name) = unit(profile, role);
    Asset {
        policy,
        name,
        quantity,
    }
}

fn to(profile: &Profile, role: &str, coin: u64) -> Output {
    Output::to(address_of(profile, role), coin)
}

fn with(output: Output, asset: &Asset) -> Output {
    output.token(&asset.policy, &asset.name, asset.quantity)
}

fn ordinary() -> Vec<u8> {
    enterprise_address(1, [9; 28])
}

fn point(seed: &str, index: u16) -> Outpoint {
    Outpoint {
        hash: hash(seed.as_bytes()),
        index,
    }
}

fn held(profile: &Profile, role: &str, assets: Vec<Asset>, transaction_id: i64) -> Held {
    Held {
        transaction_id,
        address: address_of(profile, role).to_owned(),
        assets,
        data_hash: None,
    }
}

#[test]
fn each_planted_fault_trips_exactly_its_check() {
    let profile = Profile::load("mainnet").unwrap();
    let origin = point("origin", 0);
    let holding = held(
        &profile,
        "mapping",
        vec![asset(&profile, "cnight", 100)],
        ids::transaction_id(1, 0).unwrap(),
    );
    let tx = |quantity| {
        Tx::new().input(&origin.hash, 0).output(with(
            to(&profile, "mapping", 2_000_000),
            &asset(&profile, "cnight", quantity),
        ))
    };
    let clean = vec![tx(100).build()];
    let start = BTreeMap::from([(origin.clone(), holding.clone())]);
    let output = ready(run(&profile, 10, &clean, &start, &Supplements::default()));
    assert_eq!(output.spent, vec![origin.clone()]);
    let cnight = profile.assets.cnight.unit();
    let balance = |transaction: &Built, inputs, mint, outputs| BlockError::AssetBalance {
        height: 10,
        transaction: transaction.hash.clone(),
        unit: cnight.clone(),
        inputs,
        mint,
        outputs,
    };
    let fault = |held: &BTreeMap<Outpoint, Held>, built: &[Built]| {
        run(&profile, 10, built, held, &Supplements::default()).unwrap_err()
    };
    assert_eq!(
        fault(&BTreeMap::new(), &clean),
        balance(&clean[0], 0, 0, 100),
        "missing input"
    );
    let mut duplicated = start.clone();
    duplicated
        .get_mut(&origin)
        .unwrap()
        .assets
        .push(asset(&profile, "cnight", 100));
    assert_eq!(
        fault(&duplicated, &clean),
        balance(&clean[0], 200, 0, 100),
        "duplicated holding"
    );
    let unbalanced = vec![tx(101).build()];
    assert_eq!(
        fault(&start, &unbalanced),
        balance(&unbalanced[0], 100, 0, 101),
        "created token"
    );
    let minted = vec![
        Tx::new()
            .mint(&unit(&profile, "cnight").0, &unit(&profile, "cnight").1, 1)
            .build(),
    ];
    assert_eq!(
        fault(&BTreeMap::new(), &minted),
        balance(&minted[0], 0, 1, 0),
        "mint without an output"
    );
    let mut overflowing = start.clone();
    overflowing.get_mut(&origin).unwrap().assets[0].quantity = i128::from(u64::MAX) + 1;
    assert_eq!(
        fault(&overflowing, &clean),
        BlockError::AssetQuantity {
            height: 10,
            transaction: clean[0].hash.clone(),
            unit: cnight.clone()
        },
        "held quantity outside the token domain"
    );
    let selection = Selection::new(&profile);
    let decoded = decode(&clean);
    assert_eq!(
        process_block(
            &selection,
            &header(10, 1),
            &[],
            &start,
            &Supplements::default()
        )
        .unwrap_err(),
        BlockError::TransactionCount {
            height: 10,
            expected: 1,
            actual: 0
        },
        "dropped transaction"
    );
    let twice = [decoded[0].clone(), decoded[0].clone()];
    assert_eq!(
        process_block(
            &selection,
            &header(10, 2),
            &twice,
            &start,
            &Supplements::default()
        )
        .unwrap_err(),
        BlockError::DuplicateTransaction {
            height: 10,
            transaction: clean[0].hash.clone()
        },
        "duplicate transaction"
    );
    let mut far = header(10, 1);
    far.height = ids::HEIGHT_LIMIT;
    assert_eq!(
        process_block(&selection, &far, &decoded, &start, &Supplements::default()).unwrap_err(),
        BlockError::Position {
            height: ids::HEIGHT_LIMIT
        }
    );
    let mut changed = clean[0].cbor.clone();
    let fee = changed
        .windows(6)
        .position(|window| window == [0x02, 0x1a, 0x00, 0x02, 0x98, 0x10])
        .unwrap();
    changed[fee + 5] ^= 1;
    let error =
        transaction::decode_all([(clean[0].hash.as_str(), changed.as_slice())]).unwrap_err();
    assert_eq!(
        (error.position, error.source.code),
        (0, "cbor_body_hash_mismatch"),
        "changed body"
    );
    let previous = header(9, 0);
    let next = header(10, 1);
    check_header(&previous, &next).unwrap();
    let mut wrong = next.clone();
    wrong.height = 11;
    assert_eq!(
        check_header(&previous, &wrong).unwrap_err(),
        BlockError::HeaderHeight {
            height: 11,
            expected: 10
        }
    );
    let mut wrong = next.clone();
    wrong.previous_hash = Some("ff".repeat(32));
    assert_eq!(
        check_header(&previous, &wrong).unwrap_err(),
        BlockError::HeaderParent { height: 10 }
    );
    wrong.height = ids::HEIGHT_LIMIT;
    assert_eq!(
        check_header(&previous, &wrong).unwrap_err().code(),
        "unsupported_position"
    );
}

#[test]
fn every_named_supplement_is_returned_in_one_answer() {
    let profile = Profile::load("mainnet").unwrap();
    let recipient = [1, 2, 3];
    let first = int_datum(1);
    let second = int_datum(2);
    let mut built = Vec::new();
    for fee in 0..3 {
        built.push(
            Tx::new()
                .input(&hash(&[fee]), 0)
                .output(to(&profile, "ics", 2_000_000))
                .metadata(6_500_973, recipient_metadatum(&recipient))
                .fee(200_000 + u64::from(fee))
                .build(),
        );
    }
    for datum in [&first, &second] {
        built.push(
            Tx::new()
                .input(&hash(datum), 0)
                .output(to(&profile, "mapping", 2_000_000).datum_hash(&hash(datum)))
                .build(),
        );
    }
    let decoded = decode(&built);
    let selection = Selection::new(&profile);
    let block = header(10, built.len());
    let held = BTreeMap::new();
    let mut supplements = Supplements::default();
    let mut calls = 0;
    let output = loop {
        calls += 1;
        match process_block(&selection, &block, &decoded, &held, &supplements).unwrap() {
            Outcome::Ready(output) => break output,
            Outcome::Needs(requests) => {
                let mut expected: Vec<_> = built[..3]
                    .iter()
                    .map(|tx| SupplementRequest::Metadata(tx.hash.clone()))
                    .chain(
                        [&first, &second]
                            .iter()
                            .map(|datum| SupplementRequest::Datum(hash(datum))),
                    )
                    .collect();
                expected.sort();
                assert_eq!(requests, expected, "one answer names all five");
                for request in requests {
                    match request {
                        SupplementRequest::Metadata(tx) => {
                            supplements
                                .metadata
                                .insert(tx, Some("[\"0x010203\"]".to_owned()));
                        }
                        SupplementRequest::Datum(datum_hash) => {
                            let raw = if datum_hash == hash(&first) {
                                first.clone()
                            } else {
                                second.clone()
                            };
                            supplements
                                .datums
                                .insert(datum_hash, DatumAvailability::Present(raw));
                        }
                        SupplementRequest::Output { .. } => unreachable!(),
                    }
                }
            }
        }
    };
    assert_eq!(calls, 2, "the block was decoded once and processed twice");
    assert_eq!(output.rows.tx_metadata.len(), 3);
    assert_eq!(output.rows.datum.len(), 2);
    let undecodable = vec![
        Tx::new()
            .input(&hash(b"x"), 0)
            .output(Output::new(Vec::new(), 1))
            .output(to(&profile, "mapping", 2).datum_hash(&hash(&first)))
            .build(),
    ];
    assert_eq!(
        needs(run(
            &profile,
            10,
            &undecodable,
            &held,
            &Supplements::default()
        )),
        vec![
            SupplementRequest::Output {
                transaction: undecodable[0].hash.clone(),
                index: 0
            },
            SupplementRequest::Datum(hash(&first)),
        ]
    );
}

#[test]
fn a_failed_block_changes_nothing_and_a_retry_equals_a_clean_run() {
    let profile = Profile::load("mainnet").unwrap();
    let mapping = |coin| to(&profile, "mapping", coin);
    for failure in ["output", "datum", "metadata"] {
        let origin = point("origin", 0);
        let untouched = point("untouched", 7);
        let before = BTreeMap::from([
            (origin.clone(), held(&profile, "mapping", Vec::new(), 1)),
            (untouched.clone(), held(&profile, "mapping", Vec::new(), 2)),
        ]);
        let first = Tx::new()
            .input(&origin.hash, 0)
            .output(mapping(100))
            .build();
        let later = match failure {
            "output" => Output::new(Vec::new(), 100),
            "datum" => mapping(100).datum_hash(&hash(&[0])),
            _ => to(&profile, "ics", 100),
        };
        let second = Tx::new().input(&first.hash, 0).output(later).build();
        let built = vec![first.clone(), second.clone()];
        let mut supplements = Supplements::default();
        if failure == "datum" {
            supplements
                .datums
                .insert(hash(&[0]), DatumAvailability::Present(vec![1]));
        }
        let attempt = run(&profile, 10, &built, &before, &supplements);
        match failure {
            "datum" => assert_eq!(attempt.unwrap_err().code(), "provider_datum_hash_mismatch"),
            "output" => assert_eq!(
                needs(attempt),
                vec![SupplementRequest::Output {
                    transaction: second.hash.clone(),
                    index: 0
                }]
            ),
            _ => assert_eq!(
                needs(attempt),
                vec![SupplementRequest::Metadata(second.hash.clone())]
            ),
        }
        match failure {
            "output" => {
                let resolved = transaction::Output {
                    address_raw: transaction::address_bytes(&profile.addresses.mapping).unwrap(),
                    address: Some(profile.addresses.mapping.clone()),
                    coin: 100,
                    assets: Vec::new(),
                    data_hash: None,
                    inline_datum: None,
                };
                supplements
                    .outputs
                    .insert(second.hash.clone(), BTreeMap::from([(0, resolved)]));
            }
            "datum" => {
                supplements
                    .datums
                    .insert(hash(&[0]), DatumAvailability::Present(vec![0]));
            }
            _ => {
                supplements.metadata.insert(second.hash.clone(), None);
            }
        }
        let retried = ready(run(&profile, 10, &built, &before, &supplements));
        let clean = ready(run(&profile, 10, &built, &before.clone(), &supplements));
        assert_eq!(retried, clean);
        assert_eq!(retried.rows.tx_in.len(), 2, "same-block spend");
        assert_eq!(retried.spent, vec![origin.clone()]);
        assert_eq!(
            retried
                .created
                .iter()
                .map(|(point, _)| point.hash.clone())
                .collect::<Vec<_>>(),
            vec![second.hash.clone()],
            "an output created and spent in the block is neither created nor spent"
        );
        let mut after = before.clone();
        apply(&mut after, &retried);
        assert_eq!(after.get(&untouched), before.get(&untouched));
    }
}

#[test]
fn datums_are_read_for_reader_addresses_and_the_permissioned_unit_only() {
    let profile = Profile::load("mainnet").unwrap();
    let datum_hash = hash(&[0]);
    let governance = Asset {
        name: "ff".to_owned(),
        ..asset(&profile, "council", 1)
    };
    let cases = [
        (Output::new(ordinary(), 1), governance.clone(), false),
        (to(&profile, "council", 1), governance, true),
        (
            Output::new(ordinary(), 1),
            asset(&profile, "permissioned_candidates", 1),
            true,
        ),
        (
            Output::new(ordinary(), 1),
            asset(&profile, "cnight", 1),
            false,
        ),
    ];
    for (index, (output, token, required)) in cases.into_iter().enumerate() {
        let built = vec![
            Tx::new()
                .input(&hash(&[index as u8]), 0)
                .output(with(output, &token).datum_hash(&datum_hash))
                .mint(&token.policy, &token.name, token.quantity)
                .build(),
        ];
        let mut supplements = Supplements::default();
        if required {
            assert_eq!(
                needs(run(&profile, 10, &built, &BTreeMap::new(), &supplements)),
                vec![SupplementRequest::Datum(datum_hash.clone())]
            );
            supplements
                .datums
                .insert(datum_hash.clone(), DatumAvailability::Present(vec![0]));
        }
        let output = ready(run(&profile, 10, &built, &BTreeMap::new(), &supplements));
        assert_eq!(output.rows.tx_out[0].data_hash.as_ref(), Some(&datum_hash));
        assert_eq!(output.rows.ma_tx_out.len(), 1);
        assert_eq!(output.rows.datum.len(), usize::from(required));
    }
}

#[test]
fn version_two_filters_every_address_and_keeps_full_list_ordinals() {
    let mut profile = Profile::load("mainnet").unwrap();
    profile.assets.cnight.policy = "40".repeat(28);
    profile.assets.auth.policy = "80".repeat(28);
    profile.assets.permissioned_candidates.policy = "c0".repeat(28);
    profile.assets.bridge.policy = "e0".repeat(28);
    profile.assets.council.policy = "60".repeat(28);
    profile.assets.technical_committee.policy = "a0".repeat(28);
    let foreign = |policy: &str| Asset {
        policy: policy.repeat(28),
        name: String::new(),
        quantity: 1,
    };
    let tokens = vec![
        foreign("20"),
        asset(&profile, "cnight", 1),
        foreign("50"),
        Asset {
            policy: "60".repeat(28),
            name: "ff".to_owned(),
            quantity: 1,
        },
        asset(&profile, "auth", 1),
        foreign("90"),
        asset(&profile, "permissioned_candidates", 1),
        asset(&profile, "bridge", 1),
        foreign("f0"),
    ];
    let address = |role: &str| Output::to(address_of(&profile, role), 100);
    let outputs = [
        address("ics"),
        address("reserve"),
        address("mapping"),
        Output::new(ordinary(), 100),
        address("ics"),
        Output::new(ordinary(), 100),
    ];
    let built: Vec<_> = outputs
        .into_iter()
        .enumerate()
        .map(|(index, mut output)| {
            let carried: &[Asset] = if index < 4 { &tokens } else { &tokens[..1] };
            let mut tx = Tx::new().input(&hash(&[index as u8]), 0);
            for token in carried {
                output = with(output, token);
                tx = tx.mint(&token.policy, &token.name, token.quantity);
            }
            tx.output(output).build()
        })
        .collect();
    let mut supplements = Supplements::default();
    for tx in &built {
        supplements.metadata.insert(tx.hash.clone(), None);
    }
    let output = ready(run(&profile, 10, &built, &BTreeMap::new(), &supplements));
    assert_eq!(
        output.rows.tx_out.len(),
        5,
        "address-relevant outputs stay without tokens"
    );
    assert_eq!(output.rows.ma_tx_out.len(), 20);
    for (transaction, chunk) in output.rows.ma_tx_out.as_chunks::<5>().0.iter().enumerate() {
        for (row, ordinal) in chunk.iter().zip([1, 3, 4, 6, 7]) {
            assert_eq!(
                row.id,
                (10i64 << 36) | ((transaction as i64) << 24) | ordinal,
                "asset IDs keep the ordinal in the complete sorted list"
            );
        }
    }
    assert!(!output.rows.ma_tx_out.iter().any(|row| {
        ["20", "50", "90", "f0"]
            .iter()
            .any(|p| row.policy == p.repeat(28))
    }));
}

#[test]
fn requested_datums_distinguish_unknown_absent_stored_and_later_revealed() {
    let profile = Profile::load("mainnet").unwrap();
    let raw = vec![0];
    let datum_hash = hash(&raw);
    let creation = |seed: &str| {
        Tx::new()
            .input(&hash(seed.as_bytes()), 0)
            .output(to(&profile, "mapping", 100).datum_hash(&datum_hash))
            .build()
    };
    let created = vec![creation("first")];
    let mut held = BTreeMap::new();
    assert_eq!(
        needs(run(&profile, 10, &created, &held, &Supplements::default())),
        vec![SupplementRequest::Datum(datum_hash.clone())]
    );
    let mut absent = Supplements::default();
    absent
        .datums
        .insert(datum_hash.clone(), DatumAvailability::NotFound);
    let output = ready(run(&profile, 10, &created, &held, &absent));
    assert!(output.rows.datum.is_empty());
    assert_eq!(output.rows.tx_out[0].data_hash.as_ref(), Some(&datum_hash));
    apply(&mut held, &output);
    let spent = Outpoint {
        hash: created[0].hash.clone(),
        index: 0,
    };
    assert_eq!(held[&spent].data_hash.as_ref(), Some(&datum_hash));
    let spend = Tx::new().input(&spent.hash, 0);
    assert_eq!(
        needs(run(
            &profile,
            11,
            &[spend.clone().build()],
            &held,
            &Supplements::default()
        )),
        vec![SupplementRequest::Datum(datum_hash.clone())],
        "an earlier absence does not suppress the later lookup"
    );
    let revealed = ready(run(
        &profile,
        11,
        &[spend.witness_datum(raw.clone()).build()],
        &held,
        &Supplements::default(),
    ));
    assert_eq!(
        revealed.rows.datum,
        vec![DatumRow {
            hash: datum_hash.clone(),
            value: datum::render(&raw).unwrap()
        }]
    );
    assert_eq!(revealed.spent, vec![spent]);
    for (available, rows) in [
        (DatumAvailability::Present(raw.clone()), 1),
        (DatumAvailability::Stored, 0),
    ] {
        let mut supplements = Supplements::default();
        supplements.datums.insert(datum_hash.clone(), available);
        let output = ready(run(
            &profile,
            12,
            &[creation("again")],
            &BTreeMap::new(),
            &supplements,
        ));
        assert_eq!(output.rows.datum.len(), rows);
    }
    let inline = vec![
        Tx::new()
            .input(&hash(b"inline"), 0)
            .output(to(&profile, "mapping", 100).inline_datum(raw))
            .build(),
    ];
    assert_eq!(
        ready(run(
            &profile,
            13,
            &inline,
            &BTreeMap::new(),
            &Supplements::default()
        ))
        .rows
        .datum
        .len(),
        1
    );
    let unrelated = vec![
        Tx::new()
            .input(&hash(b"unrelated"), 0)
            .output(Output::new(ordinary(), 100).datum_hash(&datum_hash))
            .build(),
    ];
    assert!(
        ready(run(
            &profile,
            14,
            &unrelated,
            &BTreeMap::new(),
            &Supplements::default()
        ))
        .rows
        .tx
        .is_empty()
    );
}

#[test]
fn input_ordinals_count_inputs_that_are_not_held() {
    let profile = Profile::load("mainnet").unwrap();
    let points: Vec<_> = ["01", "02", "03"]
        .into_iter()
        .map(|byte| Outpoint {
            hash: byte.repeat(32),
            index: 0,
        })
        .collect();
    let mut tx = Tx::new();
    for point in points.iter().rev() {
        tx = tx.input(&point.hash, point.index);
    }
    let held = BTreeMap::from([(points[1].clone(), held(&profile, "mapping", Vec::new(), 17))]);
    let output = ready(run(
        &profile,
        10,
        &[tx.build()],
        &held,
        &Supplements::default(),
    ));
    assert_eq!(
        output.rows.tx_in[0].id, 167_772_161,
        "height * 2^24 + transaction * 2^12 + rank among all inputs"
    );
}

#[test]
fn a_phase_two_invalid_transaction_spends_collateral_and_creates_its_return() {
    let profile = Profile::load("mainnet").unwrap();
    let ordinary_point = Outpoint {
        hash: "01".repeat(32),
        index: 1,
    };
    let collateral = Outpoint {
        hash: "02".repeat(32),
        index: 2,
    };
    let token = asset(&profile, "cnight", 1);
    let held = BTreeMap::from([
        (
            ordinary_point.clone(),
            held(&profile, "mapping", vec![token.clone()], 1),
        ),
        (
            collateral.clone(),
            held(&profile, "mapping", vec![token.clone()], 2),
        ),
    ]);
    let built = vec![
        Tx::new()
            .input(&ordinary_point.hash, ordinary_point.index)
            .collateral(&collateral.hash, collateral.index)
            .output(Output::new(Vec::new(), 1))
            .output(Output::new(Vec::new(), 1))
            .output(Output::new(Vec::new(), 1))
            .collateral_return(with(to(&profile, "mapping", 100), &token))
            .mint(&token.policy, &token.name, 999)
            .invalid()
            .build(),
    ];
    let output = ready(run(&profile, 10, &built, &held, &Supplements::default()));
    let rows = &output.rows;
    assert_eq!(rows.tx.len(), 1);
    assert!(!rows.tx[0].valid_contract);
    assert_eq!(rows.tx_in.len(), 1);
    assert_eq!(
        (rows.tx_in[0].tx_out_id, rows.tx_in[0].tx_out_index),
        (2, 2)
    );
    assert_eq!(rows.tx_out.len(), 1);
    assert_eq!(rows.tx_out[0].index, 3);
    assert_eq!(rows.tx_out[0].id, ids::output_id(10, 0, 3).unwrap());
    assert!(rows.tx_metadata.is_empty());
    assert!(
        output.mint_delta.is_empty(),
        "an invalid mint has no effect"
    );
    assert_eq!(output.spent, vec![collateral]);
    assert_eq!(output.created[0].0.index, 3);
}

#[test]
fn fixed_units_have_supply_entries_and_names_never_offset() {
    let profile = Profile::load("preprod").unwrap();
    assert_ne!(profile.assets.cnight.unit(), profile.assets.bridge.unit());
    let origin = point("origin", 0);
    let alternate = Asset {
        name: "ffff".to_owned(),
        ..asset(&profile, "council", 11)
    };
    let tokens = vec![
        asset(&profile, "cnight", 100),
        asset(&profile, "bridge", 7),
        alternate.clone(),
    ];
    let start = BTreeMap::from([(origin.clone(), held(&profile, "mapping", tokens.clone(), 1))]);
    let spend = |quantities: [i128; 3], mint: &[(&Asset, i128)]| {
        let mut output = to(&profile, "mapping", 100);
        for (token, quantity) in tokens.iter().zip(quantities) {
            if quantity > 0 {
                output = output.token(&token.policy, &token.name, quantity);
            }
        }
        let mut tx = Tx::new().input(&origin.hash, 0).output(output);
        for (token, quantity) in mint {
            tx = tx.mint(&token.policy, &token.name, *quantity);
        }
        vec![tx.build()]
    };
    let changed = spend(
        [98, 10, 13],
        &[(&tokens[0], -2), (&tokens[1], 3), (&alternate, 2)],
    );
    let output = ready(run(&profile, 10, &changed, &start, &Supplements::default()));
    assert_eq!(
        output.mint_delta,
        BTreeMap::from([
            (profile.assets.cnight.unit(), -2),
            (profile.assets.bridge.unit(), 3)
        ]),
        "governance names conserve; only fixed units have supply entries"
    );
    let offset = spend([101, 6, 11], &[]);
    assert!(matches!(
        run(&profile, 10, &offset, &start, &Supplements::default()),
        Err(BlockError::AssetBalance { unit, .. }) if unit == profile.assets.cnight.unit()
    ));
    let sibling = Asset {
        name: "01".to_owned(),
        ..asset(&profile, "cnight", 500)
    };
    let mut unchanged = to(&profile, "mapping", 100);
    for token in tokens.iter().chain([&sibling]) {
        unchanged = with(unchanged, token);
    }
    let foreign_sibling = vec![
        Tx::new()
            .input(&origin.hash, 0)
            .output(unchanged)
            .mint(&sibling.policy, &sibling.name, 500)
            .build(),
    ];
    assert!(
        ready(run(
            &profile,
            10,
            &foreign_sibling,
            &start,
            &Supplements::default()
        ))
        .mint_delta
        .is_empty()
    );
    let burn = spend(
        [0, 0, 0],
        &[(&tokens[0], -100), (&tokens[1], -7), (&alternate, -11)],
    );
    ready(run(&profile, 10, &burn, &start, &Supplements::default()));
    let lost = vec![
        Tx::new()
            .collateral(&origin.hash, 0)
            .output(with(
                to(&profile, "mapping", 1),
                &asset(&profile, "cnight", 777),
            ))
            .collateral_return(with(
                with(to(&profile, "mapping", 1), &tokens[1]),
                &alternate,
            ))
            .mint(&tokens[0].policy, &tokens[0].name, 100)
            .invalid()
            .build(),
    ];
    assert_eq!(
        run(&profile, 10, &lost, &start, &Supplements::default()).unwrap_err(),
        BlockError::AssetBalance {
            height: 10,
            transaction: lost[0].hash.clone(),
            unit: profile.assets.cnight.unit(),
            inputs: 100,
            mint: 0,
            outputs: 0
        }
    );
    let unrelated = vec![
        Tx::new()
            .input(&hash(b"unheld"), 0)
            .output(Output::new(ordinary(), 1).token(&"ff".repeat(28), "00", 42))
            .build(),
    ];
    let output = ready(run(
        &profile,
        10,
        &unrelated,
        &BTreeMap::new(),
        &Supplements::default(),
    ));
    assert!(output.mint_delta.is_empty() && output.rows.tx.is_empty());
}

#[test]
fn a_same_block_spend_and_collateral_follow_one_rule() {
    let profile = Profile::load("mainnet").unwrap();
    let origin = point("origin", 0);
    let token = asset(&profile, "cnight", 100);
    let start = BTreeMap::from([(
        origin.clone(),
        held(&profile, "mapping", vec![token.clone()], 1),
    )]);
    let first = Tx::new()
        .input(&origin.hash, 0)
        .output(with(to(&profile, "mapping", 100), &token))
        .build();
    let invalid = Tx::new()
        .input(&hash(b"never"), 0)
        .collateral(&first.hash, 0)
        .output(with(
            to(&profile, "mapping", 1),
            &asset(&profile, "cnight", 999),
        ))
        .collateral_return(with(to(&profile, "mapping", 1), &token))
        .mint(&token.policy, &token.name, 999)
        .invalid()
        .build();
    let built = vec![first.clone(), invalid.clone()];
    assert_eq!(inputs(&decode(&built)), vec![origin.clone()]);
    let output = ready(run(&profile, 10, &built, &start, &Supplements::default()));
    assert!(output.mint_delta.is_empty());
    assert_eq!(output.rows.tx_in.len(), 2);
    assert_eq!(output.rows.tx_out.last().unwrap().index, 1);
    assert_eq!(output.spent, vec![origin]);
    assert_eq!(output.created.len(), 1);
    assert_eq!(output.created[0].0.hash, invalid.hash);
}

#[test]
fn more_transactions_than_positional_ids_allow_are_refused() {
    let profile = Profile::load("mainnet").unwrap();
    let built: Vec<_> = (0..=u64::from(ids::INDEX_LIMIT))
        .map(|fee| Tx::new().fee(fee).build())
        .collect();
    let decoded = decode(&built);
    assert_eq!(
        process_block(
            &Selection::new(&profile),
            &header(10, decoded.len()),
            &decoded,
            &BTreeMap::new(),
            &Supplements::default()
        )
        .unwrap_err(),
        BlockError::Position { height: 10 }
    );
}

#[test]
fn stored_rare_shapes_become_observation_candidates() {
    let profile = Profile::load("mainnet").unwrap();
    let cnight = asset(&profile, "cnight", 5);
    let large = large_datum(10_001);
    let small = int_datum(3);
    let provided = large_datum(12_000);
    let pointer = pointer_address(1, [4; 28], (1, 2, 3));
    let built = vec![
        Tx::new()
            .input(&hash(b"a"), 0)
            .output(Output::new(enterprise_address(1, [5; 28]), 1))
            .output(with(Output::new(pointer, 2_000_000), &cnight))
            .mint(&cnight.policy, &cnight.name, 5)
            .build(),
        Tx::new()
            .input(&hash(b"b"), 0)
            .output(to(&profile, "mapping", 2_000_000).inline_datum(large.clone()))
            .output(to(&profile, "mapping", 2_000_000).inline_datum(small))
            .output(to(&profile, "mapping", 2_000_000).datum_hash(&hash(&provided)))
            .build(),
        Tx::new()
            .input(&hash(b"c"), 0)
            .output(to(&profile, "ics", 2_000_000))
            .metadata(6_500_973, recipient_metadatum(&[9]))
            .build(),
    ];
    let mut supplements = Supplements::default();
    supplements.datums.insert(
        hash(&provided),
        DatumAvailability::Present(provided.clone()),
    );
    supplements
        .metadata
        .insert(built[2].hash.clone(), Some("[\"0x09\"]".to_owned()));
    let output = ready(run(&profile, 10, &built, &BTreeMap::new(), &supplements));
    let shapes: Vec<_> = output
        .observations
        .iter()
        .map(|candidate| {
            (
                candidate.shape,
                candidate.transaction.clone(),
                candidate.output_index,
                candidate.raw_datum_bytes,
                candidate.sources.clone(),
            )
        })
        .collect();
    let mut datums = vec![
        (
            Shape::LargeDatum,
            Some(built[1].hash.clone()),
            Some(0),
            Some(large.len()),
            vec![Source::BlockPage(1)],
        ),
        (
            Shape::LargeDatum,
            Some(built[1].hash.clone()),
            Some(2),
            Some(provided.len()),
            vec![Source::Datum(hash(&provided))],
        ),
    ];
    datums.sort_by_key(|candidate| candidate.4.clone());
    let mut found_datums: Vec<_> = shapes
        .iter()
        .filter(|candidate| candidate.0 == Shape::LargeDatum)
        .cloned()
        .collect();
    found_datums.sort_by_key(|candidate| candidate.4.clone());
    assert_eq!(found_datums, datums);
    assert_eq!(
        shapes
            .iter()
            .filter(|candidate| candidate.0 != Shape::LargeDatum)
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            (
                Shape::PointerAddress,
                Some(built[0].hash.clone()),
                Some(1),
                None,
                vec![Source::BlockPage(1)]
            ),
            (
                Shape::BridgeMetadata,
                Some(built[2].hash.clone()),
                None,
                None,
                vec![
                    Source::BlockPage(1),
                    Source::TransactionMetadata(built[2].hash.clone())
                ]
            ),
        ]
    );
    let quiet = vec![
        Tx::new()
            .input(&hash(b"quiet"), 0)
            .output(to(&profile, "mapping", 2_000_000).inline_datum(int_datum(1)))
            .build(),
    ];
    assert!(
        ready(run(
            &profile,
            11,
            &quiet,
            &BTreeMap::new(),
            &Supplements::default()
        ))
        .observations
        .is_empty()
    );
}

fn next_random(random: &mut u64) -> u64 {
    *random = random
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *random >> 17
}

/// Sum each fixed unit over the held outputs.
fn supply(profile: &Profile, held: &BTreeMap<Outpoint, Held>) -> BTreeMap<String, i128> {
    let mut totals: BTreeMap<_, _> = fixed_units(profile).into_iter().map(|u| (u, 0)).collect();
    for entry in held.values() {
        for asset in &entry.assets {
            if let Some(total) = totals.get_mut(&asset.unit()) {
                *total += asset.quantity;
            }
        }
    }
    totals
}

#[test]
fn generated_chains_conserve_tokens_and_track_supply_on_both_profiles() {
    let (mut blocks, mut transactions, mut invalid_count) = (0, 0, 0);
    let (mut burns_to_zero, mut same_block_spends) = (0, 0);
    let mut coverage = [0usize; 6];
    let mut faults = [0usize; 7];
    for network in ["mainnet", "preprod"] {
        let profile = Profile::load(network).unwrap();
        let selection = Selection::new(&profile);
        let mapping = profile.addresses.mapping.clone();
        let mut units: BTreeMap<String, (String, String)> = fixed_units(&profile)
            .into_iter()
            .map(|unit| (unit.clone(), (unit[..56].to_owned(), unit[56..].to_owned())))
            .collect();
        let alternate = (profile.assets.council.policy.clone(), "ff00".to_owned());
        let foreign = ("ff".repeat(28), "01".to_owned());
        units.insert(format!("{}{}", alternate.0, alternate.1), alternate);
        units.insert(format!("{}{}", foreign.0, foreign.1), foreign.clone());
        let foreign_unit = format!("{}{}", foreign.0, foreign.1);
        let eligible = |tokens: &BTreeMap<String, i128>| -> Vec<Asset> {
            tokens
                .iter()
                .filter(|(_, quantity)| **quantity > 0)
                .map(|(unit, quantity)| Asset {
                    policy: units[unit].0.clone(),
                    name: units[unit].1.clone(),
                    quantity: *quantity,
                })
                .filter(|asset| selection.required(asset))
                .collect()
        };
        let output = |tokens: &BTreeMap<String, i128>, coin: u64| {
            let mut output = Output::to(&mapping, coin);
            for (unit, quantity) in tokens.iter().filter(|(_, quantity)| **quantity > 0) {
                output = output.token(&units[unit].0, &units[unit].1, *quantity);
            }
            output
        };
        for seed in 0..32u64 {
            // Every unspent output's complete tokens, independent of the code.
            let mut model: BTreeMap<Outpoint, BTreeMap<String, i128>> = BTreeMap::new();
            let mut held = BTreeMap::new();
            for root in 0..3u16 {
                let tokens: BTreeMap<_, _> = units
                    .keys()
                    .enumerate()
                    .map(|(index, unit)| (unit.clone(), 11 + i128::from(root) * 6 + index as i128))
                    .collect();
                let point = Outpoint {
                    hash: format!("{:064x}", seed + 100),
                    index: root,
                };
                held.insert(
                    point.clone(),
                    Held {
                        transaction_id: 1,
                        address: mapping.clone(),
                        assets: eligible(&tokens),
                        data_hash: None,
                    },
                );
                model.insert(point, tokens);
            }
            let mut expected_supply = supply(&profile, &held);
            for (index, unit) in units.keys().enumerate() {
                if let Some(total) = expected_supply.get(unit) {
                    assert_eq!(*total, 51 + 3 * index as i128, "independent baseline");
                }
            }
            let mut previous = header(9, 0);
            let mut random = seed + 1;
            let fault_height = 10 + next_random(&mut random) % 32;
            let mut carry = model.keys().next().unwrap().clone();
            for height in 10..42u32 {
                let count = 1 + (next_random(&mut random) % 6) as usize;
                coverage[count - 1] += 1;
                let mut expected = model.clone();
                let mut expected_delta: BTreeMap<String, i128> = BTreeMap::new();
                let mut builders = Vec::new();
                let mut hashes: Vec<String> = Vec::new();
                for index in 0..count {
                    let mut spent = vec![carry.clone()];
                    let alternatives: Vec<_> =
                        expected.keys().filter(|p| **p != carry).cloned().collect();
                    if !alternatives.is_empty() {
                        spent.push(
                            alternatives[(next_random(&mut random) as usize) % alternatives.len()]
                                .clone(),
                        );
                    }
                    same_block_spends += spent.iter().filter(|p| hashes.contains(&p.hash)).count();
                    let mut balances: BTreeMap<String, i128> =
                        units.keys().map(|unit| (unit.clone(), 0)).collect();
                    for point in &spent {
                        for (unit, quantity) in &expected[point] {
                            *balances.get_mut(unit).unwrap() += quantity;
                        }
                    }
                    let invalid = next_random(&mut random).is_multiple_of(5);
                    invalid_count += usize::from(invalid);
                    let mut tx = Tx::new().fee(170_000 + (u64::from(height) << 8) + index as u64);
                    if invalid {
                        tx = tx.mint(
                            &profile.assets.cnight.policy,
                            &profile.assets.cnight.name,
                            17,
                        );
                    } else {
                        for (unit, quantity) in &mut balances {
                            if *unit == foreign_unit {
                                continue;
                            }
                            let draw = next_random(&mut random);
                            let change = if draw.is_multiple_of(13) {
                                -*quantity
                            } else {
                                ((draw % 7) as i128 - 3).max(-*quantity)
                            };
                            burns_to_zero += usize::from(*quantity > 0 && *quantity + change == 0);
                            *quantity += change;
                            if change != 0 {
                                tx = tx.mint(&units[unit].0, &units[unit].1, change);
                                if expected_supply.contains_key(unit) {
                                    *expected_delta.entry(unit.clone()).or_default() += change;
                                }
                            }
                        }
                    }
                    let mut left = BTreeMap::new();
                    let mut right = BTreeMap::new();
                    for (unit, quantity) in &balances {
                        let split = if invalid {
                            *quantity
                        } else {
                            i128::from(next_random(&mut random) % ((*quantity + 1) as u64))
                        };
                        left.insert(unit.clone(), split);
                        right.insert(unit.clone(), quantity - split);
                    }
                    let unrelated = Output::new(ordinary(), 3).token(&foreign.0, &foreign.1, 7);
                    if invalid {
                        tx = tx
                            .input(&"ee".repeat(32), 0)
                            .output(output(&BTreeMap::new(), 999))
                            .output(Output::new(Vec::new(), 1))
                            .collateral_return(output(&left, 4))
                            .invalid();
                        for point in &spent {
                            tx = tx.collateral(&point.hash, point.index);
                        }
                    } else {
                        for point in &spent {
                            tx = tx.input(&point.hash, point.index);
                        }
                        tx = tx
                            .output(output(&left, 4))
                            .output(output(&right, 5))
                            .output(unrelated);
                    }
                    let built = tx.build();
                    for point in &spent {
                        expected.remove(point);
                    }
                    carry = Outpoint {
                        hash: built.hash.clone(),
                        index: if invalid { 2 } else { 0 },
                    };
                    expected.insert(carry.clone(), left);
                    if !invalid {
                        expected.insert(
                            Outpoint {
                                hash: built.hash.clone(),
                                index: 1,
                            },
                            right,
                        );
                    }
                    hashes.push(built.hash.clone());
                    builders.push(tx);
                }
                let built: Vec<_> = builders.iter().map(Tx::build).collect();
                let block = Header {
                    height,
                    hash: hash(&height.to_be_bytes()),
                    previous_hash: Some(previous.hash.clone()),
                    transaction_count: built.len(),
                };
                check_header(&previous, &block).unwrap();
                let decoded = decode(&built);
                assert!(
                    inputs(&decoded)
                        .iter()
                        .all(|point| held.contains_key(point)),
                    "one lookup finds every effective input from before the block"
                );
                if u64::from(height) == fault_height {
                    let position = (next_random(&mut random) as usize) % built.len();
                    let kind = (seed % 7) as usize;
                    let process = |txs: &[Transaction<'_>], held: &BTreeMap<Outpoint, Held>| {
                        process_block(&selection, &block, txs, held, &Supplements::default())
                    };
                    match kind {
                        0 | 1 => {
                            // A held input of the first transaction that carries tokens.
                            let first = decoded[0]
                                .effective_inputs()
                                .iter()
                                .find(|point| !held[*point].assets.is_empty())
                                .cloned();
                            if let Some(first) = first {
                                let mut broken = held.clone();
                                if kind == 0 {
                                    broken.remove(&first);
                                } else {
                                    let entry = broken.get_mut(&first).unwrap();
                                    let duplicate = entry.assets[0].clone();
                                    entry.assets.push(duplicate);
                                }
                                assert!(matches!(
                                    process(&decoded, &broken),
                                    Err(BlockError::AssetBalance { height: h, ref transaction, .. })
                                        if h == height && *transaction == built[0].hash
                                ));
                                faults[kind] += 1;
                            }
                        }
                        2 => {
                            let mut fewer = decoded.clone();
                            fewer.remove(position);
                            assert_eq!(
                                process(&fewer, &held).unwrap_err(),
                                BlockError::TransactionCount {
                                    height,
                                    expected: built.len(),
                                    actual: built.len() - 1
                                }
                            );
                            faults[kind] += 1;
                        }
                        3 => {
                            let fee = 170_000u32 + (height << 8) + position as u32;
                            let mut field = vec![0x02, 0x1a];
                            field.extend(fee.to_be_bytes());
                            let mut changed = built[position].cbor.clone();
                            let at = changed
                                .windows(6)
                                .position(|window| window == field.as_slice())
                                .unwrap();
                            changed[at + 5] ^= 1;
                            let error = transaction::decode_all(built.iter().enumerate().map(
                                |(index, tx)| {
                                    let raw = if index == position {
                                        changed.as_slice()
                                    } else {
                                        tx.cbor.as_slice()
                                    };
                                    (tx.hash.as_str(), raw)
                                },
                            ))
                            .unwrap_err();
                            assert_eq!(
                                (error.position, error.source.code),
                                (position, "cbor_body_hash_mismatch")
                            );
                            faults[kind] += 1;
                        }
                        4 => {
                            let mut bad = block.clone();
                            bad.height += 1;
                            assert_eq!(
                                check_header(&previous, &bad).unwrap_err(),
                                BlockError::HeaderHeight {
                                    height: height + 1,
                                    expected: height
                                }
                            );
                            faults[kind] += 1;
                        }
                        5 => {
                            let mut bad = block.clone();
                            bad.previous_hash = None;
                            assert_eq!(
                                check_header(&previous, &bad).unwrap_err(),
                                BlockError::HeaderParent { height }
                            );
                            faults[kind] += 1;
                        }
                        _ => {
                            // A valid transaction whose outputs gain one token.
                            let valid = (0..built.len())
                                .map(|offset| (position + offset) % built.len())
                                .find(|index| decoded[*index].is_valid());
                            if let Some(position) = valid {
                                let mut tampered = builders.clone();
                                let cnight = &profile.assets.cnight;
                                tampered[position] = tampered[position].clone().output(
                                    Output::to(&mapping, 6).token(&cnight.policy, &cnight.name, 1),
                                );
                                let rebuilt: Vec<_> = tampered.iter().map(Tx::build).collect();
                                assert!(matches!(
                                    process(&decode(&rebuilt), &held),
                                    Err(BlockError::AssetBalance { height: h, ref transaction, .. })
                                        if h == height && *transaction == rebuilt[position].hash
                                ));
                                faults[kind] += 1;
                            }
                        }
                    }
                }
                let output = match process_block(
                    &selection,
                    &block,
                    &decoded,
                    &held,
                    &Supplements::default(),
                )
                .unwrap()
                {
                    Outcome::Ready(output) => *output,
                    Outcome::Needs(needs) => panic!("generated block needs {needs:?}"),
                };
                assert_eq!(output.mint_delta, expected_delta);
                apply(&mut held, &output);
                let expected_held: BTreeMap<_, _> = expected
                    .iter()
                    .map(|(point, tokens)| (point.clone(), eligible(tokens)))
                    .collect();
                assert_eq!(
                    held.iter()
                        .map(|(point, entry)| (point.clone(), entry.assets.clone()))
                        .collect::<BTreeMap<_, _>>(),
                    expected_held,
                    "independent model vs applied held changes"
                );
                for (unit, delta) in &expected_delta {
                    *expected_supply.get_mut(unit).unwrap() += delta;
                }
                assert_eq!(supply(&profile, &held), expected_supply);
                model = expected;
                previous = block;
                blocks += 1;
                transactions += count;
            }
        }
    }
    println!(
        "generated_model blocks={blocks} transactions={transactions} counts={coverage:?} invalid={invalid_count} burns_to_zero={burns_to_zero} same_block_spends={same_block_spends} faults={faults:?}"
    );
    assert!(invalid_count > 100 && burns_to_zero > 100 && same_block_spends > 1000);
    assert_eq!(blocks, 2_048);
    assert!(transactions > 6_000);
    assert!(coverage.iter().all(|count| *count > 100), "{coverage:?}");
    assert!(faults.iter().all(|count| *count > 0), "{faults:?}");
}
