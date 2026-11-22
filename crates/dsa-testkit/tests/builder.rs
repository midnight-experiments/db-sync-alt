//! Built transactions are accepted by the product decoder and by the pallas
//! comparison decoder, and both read the same facts.

use dsa_chain::transaction::{self, Asset};
use dsa_testkit::tx::{
    Output, Tx, enterprise_address, hash, int_datum, large_datum, pointer_address,
    recipient_metadatum,
};
use pallas_primitives::conway::MintedDatumOption;
use pallas_traverse::MultiEraTx;

const POLICY: &str = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";
const OTHER: &str = "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0";

fn sorted(mut assets: Vec<Asset>) -> Vec<Asset> {
    assets.sort_by(|a, b| (&a.policy, &a.name).cmp(&(&b.policy, &b.name)));
    assets
}

fn pallas_assets(tx: &MultiEraTx<'_>, output: usize) -> Vec<Asset> {
    sorted(
        tx.outputs()[output]
            .value()
            .assets()
            .iter()
            .flat_map(|policy| policy.assets())
            .map(|asset| Asset {
                policy: asset.policy().to_string(),
                name: hex::encode(asset.name()),
                quantity: asset.any_coin(),
            })
            .collect(),
    )
}

#[test]
fn built_transactions_round_trip_through_both_decoders() {
    let datum = int_datum(42);
    let large = large_datum(10_001);
    let inline = int_datum(-7);
    let enterprise = enterprise_address(1, [3; 28]);
    let pointer = pointer_address(1, [4; 28], (2_498_243, 27, 3));
    let valid = Tx::new()
        .input(&hash(b"first"), 1)
        .input(&hash(b"second"), 0)
        .collateral(&hash(b"collateral"), 2)
        .output(Output::new(enterprise.clone(), 2_000_000))
        .output(
            Output::new(enterprise.clone(), 3_000_000)
                .token(POLICY, "", 5)
                .token(POLICY, "4e49474854", 9)
                .token(OTHER, "01", 1)
                .datum_hash(&hash(&datum)),
        )
        .output(Output::new(pointer.clone(), 1_500_000).inline_datum(inline.clone()))
        .output(Output::new(enterprise.clone(), 1_000_000).legacy())
        .collateral_return(Output::new(enterprise.clone(), 4_000_000))
        .mint(POLICY, "4e49474854", 9)
        .mint(OTHER, "02", -3)
        .witness_datum(datum.clone())
        .witness_datum(large.clone())
        .metadata(6_500_973, recipient_metadatum(&[1, 2, 3]))
        .build();
    let invalid = Tx::new()
        .input(&hash(b"ignored"), 0)
        .collateral(&hash(b"spent collateral"), 0)
        .output(Output::new(enterprise.clone(), 1))
        .collateral_return(Output::new(enterprise.clone(), 5).token(POLICY, "", 1))
        .invalid()
        .build();
    assert!(large.len() > 10_000);
    for built in [&valid, &invalid] {
        let ours = transaction::decode(&built.cbor, &built.hash).unwrap();
        let theirs = MultiEraTx::decode(&built.cbor).unwrap();
        assert_eq!(theirs.hash().to_string(), built.hash);
        assert_eq!(ours.valid, theirs.is_valid());
        let points = |inputs: Vec<pallas_traverse::MultiEraInput<'_>>| {
            let mut points: Vec<_> = inputs
                .iter()
                .map(|input| (input.hash().to_string(), input.index() as u16))
                .collect();
            points.sort();
            points
        };
        let our_points = |inputs: &[transaction::Outpoint]| {
            inputs
                .iter()
                .map(|point| (point.hash.clone(), point.index))
                .collect::<Vec<_>>()
        };
        assert_eq!(our_points(&ours.ordinary), points(theirs.inputs()));
        assert_eq!(our_points(&ours.collateral), points(theirs.collateral()));
        assert_eq!(ours.outputs.len(), theirs.outputs().len());
        for (index, (mine, other)) in ours.outputs.iter().zip(theirs.outputs()).enumerate() {
            let mine = mine.as_ref().unwrap();
            assert_eq!(mine.address_raw, other.address().unwrap().to_vec());
            assert_eq!(
                mine.address,
                transaction::address_text(&other.address().unwrap().to_vec())
            );
            assert_eq!(mine.coin, other.value().coin());
            assert_eq!(mine.assets, pallas_assets(&theirs, index));
            let (data_hash, inline_datum) = match other.datum() {
                None => (None, None),
                Some(MintedDatumOption::Hash(hash)) => (Some(hash.to_string()), None),
                Some(MintedDatumOption::Data(data)) => {
                    let raw = data.0.raw_cbor();
                    (Some(transaction::hash(raw)), Some(raw.to_vec()))
                }
            };
            assert_eq!(mine.data_hash, data_hash);
            assert_eq!(mine.inline_datum, inline_datum);
        }
        assert_eq!(
            ours.has_collateral_return,
            theirs.collateral_return().is_some()
        );
        assert_eq!(
            ours.mint,
            sorted(
                theirs
                    .mints()
                    .iter()
                    .flat_map(|policy| policy.assets())
                    .map(|asset| Asset {
                        policy: asset.policy().to_string(),
                        name: hex::encode(asset.name()),
                        quantity: asset.any_coin(),
                    })
                    .collect()
            )
        );
        let witnessed: std::collections::BTreeMap<_, _> = theirs
            .plutus_data()
            .iter()
            .map(|datum| {
                (
                    transaction::hash(datum.raw_cbor()),
                    datum.raw_cbor().to_vec(),
                )
            })
            .collect();
        assert_eq!(ours.datums, witnessed);
    }
    let ours = transaction::decode(&valid.cbor, &valid.hash).unwrap();
    assert_eq!(ours.outputs[2].as_ref().unwrap().inline_datum, Some(inline));
    assert_eq!(
        ours.outputs[2].as_ref().unwrap().address_raw.first(),
        Some(&0x41),
        "pointer address header"
    );
    assert!(
        ours.outputs[2]
            .as_ref()
            .unwrap()
            .address
            .as_deref()
            .is_some_and(|text| text.starts_with("addr1"))
    );
    assert_eq!(
        dsa_chain::metadata::observe(ours.auxiliary),
        dsa_chain::metadata::Observation::Present(Some(vec![1, 2, 3]))
    );
    let mut tampered = valid.cbor.clone();
    let fee = tampered
        .windows(6)
        .position(|window| window == [0x02, 0x1a, 0x00, 0x02, 0x98, 0x10])
        .expect("fee field of 170,000");
    tampered[fee + 5] ^= 1;
    assert_eq!(
        transaction::decode(&tampered, &valid.hash)
            .unwrap_err()
            .code,
        "cbor_body_hash_mismatch"
    );
}
