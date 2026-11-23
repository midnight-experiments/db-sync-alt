//! Standard CBOR/Plutus conformance for the local reader. These inert values
//! are never wrapped in crafted ledger transactions or sent to other decoders.

use dsa_chain::{cbor::Item, datum};

macro_rules! accepts {
    ($name:ident, $($hex:literal),+ $(,)?) => {
        #[test] fn $name() { $(Item::parse(&hex::decode($hex).unwrap()).unwrap();)+ }
    };
}
macro_rules! refuses {
    ($name:ident, $($hex:literal),+ $(,)?) => {
        #[test] fn $name() { $(assert!(Item::parse(&hex::decode($hex).unwrap()).is_err(), $hex);)+ }
    };
}

accepts!(
    ra_14_a_2_2_nonpreferred_integer_string_container_tag_widths,
    "1800",
    "190000",
    "1a00000000",
    "1b0000000000000000",
    "3800",
    "5800",
    "7800",
    "9800",
    "b800",
    "d80000"
);
refuses!(
    ra_14_a_2_1_reserved_heads_and_truncated_arguments,
    "1c",
    "1d",
    "1e",
    "18",
    "1900",
    "1a0000",
    "1b00000000"
);
accepts!(
    ra_14_a_3_1_full_unsigned_and_negative_domains,
    "1bffffffffffffffff",
    "3bffffffffffffffff"
);
refuses!(ra_14_a_3_1_indefinite_integers, "1f", "3f");
accepts!(
    ra_14_a_3_2_3_definite_bytes_and_utf8,
    "40",
    "43010203",
    "60",
    "62c3bc",
    "64f0908591"
);
refuses!(ra_14_a_3_3_invalid_utf8, "61ff", "62c080", "63eda080");
accepts!(
    ra_14_a_3_4_chunked_strings,
    "5fff",
    "5f4201024103ff",
    "7f61616162ff"
);
refuses!(
    ra_14_a_3_4_wrong_or_indefinite_chunks,
    "5f6161ff",
    "5f5fffff",
    "7f4161ff",
    "5f4101"
);
accepts!(
    ra_14_a_3_5_6_definite_and_indefinite_containers,
    "80",
    "a0",
    "9fff",
    "bfff",
    "bf009f0102ffff"
);
refuses!(
    ra_14_a_3_5_6_truncated_lengths_and_odd_maps,
    "9bffffffffffffffff",
    "a100",
    "bf00ff",
    "9f00"
);
accepts!(
    ra_14_a_3_7_tags_and_raw_content,
    "c000",
    "d9010280",
    "d903e980"
);
refuses!(ra_14_a_3_7_indefinite_tag, "df00");
accepts!(
    ra_14_a_4_8_unconsumed_map_pairs_and_unordered_keys,
    "a201020103",
    "a202000100",
    "bf01020103ff"
);
refuses!(ra_14_a_5_8_no_trailing_bytes, "0000", "f600", "80ff");

#[test]
fn ra_14_b_2_constructor_ranges_and_exact_tag102_u64() {
    for (raw, constructor) in [
        ("d87980", 0u64),
        ("d87f80", 6),
        ("d9050080", 7),
        ("d9057880", 127),
        ("d866821bffffffffffffffff80", u64::MAX),
    ] {
        assert_eq!(
            datum::render(&hex::decode(raw).unwrap()).unwrap().unwrap(),
            format!("{{\"constructor\":{constructor},\"fields\":[]}}")
        );
    }
}

#[test]
fn ra_14_b_2_invalid_plutus_shapes_do_not_become_null() {
    for raw in [
        "60", "f4", "f6", "f93c00", "d87900", "d8668100", "d86480", "d8184100",
    ] {
        assert!(datum::render(&hex::decode(raw).unwrap()).is_err(), "{raw}");
    }
}

#[test]
fn ra_14_b_3_json_depth_boundaries_for_inert_data_values() {
    let mut lists = vec![0x81; 999];
    lists.push(0);
    assert!(datum::render(&lists).unwrap().is_some());
    lists.insert(0, 0x81);
    assert!(datum::render(&lists).unwrap().is_none());
    let mut maps = Vec::new();
    for _ in 0..666 {
        maps.extend_from_slice(&[0xa1, 0x00]);
    }
    maps.push(0);
    assert!(datum::render(&maps).unwrap().is_some());
    maps.splice(0..0, [0xa1, 0]);
    assert!(datum::render(&maps).unwrap().is_none());
}

#[test]
fn ra_14_b_2_bignum_sign_and_chunked_bytes_are_exact() {
    for (raw, json) in [
        ("c34100", "{\"int\":-1}"),
        ("c35f41014100ff", "{\"int\":-257}"),
        ("5f4201024103ff", "{\"bytes\":\"010203\"}"),
    ] {
        assert_eq!(
            datum::render(&hex::decode(raw).unwrap()).unwrap().unwrap(),
            json
        );
    }
}
