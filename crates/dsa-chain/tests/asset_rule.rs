use dsa_chain::{
    assets::{AssetRule, RULE_VERSION, fixed_units},
    profiles::Profile,
};

#[test]
fn configured_full_units_and_all_governance_names_are_retained() {
    assert_eq!(RULE_VERSION, 2);
    for network in ["mainnet", "preprod", "preview"] {
        let profile = if network == "preview" {
            serde_json::from_str(include_str!("../profiles/preview.json")).unwrap()
        } else {
            Profile::load(network).unwrap()
        };
        let rule = AssetRule::from_profile(&profile);
        for asset in [
            &profile.assets.cnight,
            &profile.assets.auth,
            &profile.assets.permissioned_candidates,
            &profile.assets.bridge,
        ] {
            assert!(rule.allows(&asset.policy, &asset.name));
            assert!(!rule.allows(&asset.policy, &format!("{}ff", asset.name)));
        }
        for body in profile.governance() {
            assert!(rule.allows(&body.asset.policy, "ff"));
        }
        assert!(!rule.allows(&"ff".repeat(28), ""));
        assert_eq!(rule.governance_policies().len(), 2);
    }
}

#[test]
fn preprod_empty_cnight_and_named_bridge_are_both_required() {
    let profile = Profile::load("preprod").unwrap();
    let rule = AssetRule::from_profile(&profile);
    assert_eq!(profile.assets.cnight.name, "");
    assert_eq!(profile.assets.bridge.name, "4e49474854");
    assert_eq!(profile.assets.cnight.policy, profile.assets.bridge.policy);
    assert!(rule.exact_units().contains(&profile.assets.cnight.unit()));
    assert!(rule.exact_units().contains(&profile.assets.bridge.unit()));
}

#[test]
fn ics_and_reserve_role_entries_add_no_token_exception() {
    let mut profile = Profile::load("preprod").unwrap();
    profile.assets.ics.name = "01".to_owned();
    profile.assets.reserve.policy = "ff".repeat(28);
    let rule = AssetRule::from_profile(&profile);
    assert!(!rule.allows(&profile.assets.ics.policy, &profile.assets.ics.name));
    assert!(!rule.allows(&profile.assets.reserve.policy, &profile.assets.reserve.name));
}

#[test]
fn fixed_descriptors_use_six_roles_without_alias_exceptions() {
    let mut profile = Profile::load("preprod").unwrap();
    let expected = fixed_units(&profile);
    profile.assets.ics.policy = "ee".repeat(28);
    profile.assets.reserve.name = "ff".to_owned();
    assert_eq!(fixed_units(&profile), expected);
}

#[test]
fn fixed_descriptors_equal_the_independent_pinned_network_sets() {
    let expected = std::collections::BTreeSet::from([
        "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa4e49474854".to_owned(),
        "2c322542e32817f26a75bc49eaaf3ce831b62dafe4040e2f296e339a".to_owned(),
        "73e4aea31b5b51d9b0ca386196fc6a4c422f74c5aea011e4b8bdf4e5".to_owned(),
        "911dee358e934f0ea32af5803586cbeee9721d20ab969f9fdff335ac".to_owned(),
        "f9bfa20ed6136305b654b3613bbe1c9a6f2f058fb61edee49bdf58be".to_owned(),
    ]);
    assert_eq!(expected.len(), 5);
    assert_eq!(fixed_units(&Profile::load("mainnet").unwrap()), expected);
    let expected = std::collections::BTreeSet::from([
        "62905eebf02b600a8c5f5b3d3fc17daefc7add85a92eb004158bae77".to_owned(),
        "7e69087d98fac5869eac14e13dfb6f98228c41e638aa2a59d1f85e9c".to_owned(),
        "bdace6f5897301896f7debbf5b386d6997e4558c12377be3ecb22ce8".to_owned(),
        "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1".to_owned(),
        "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af14e49474854".to_owned(),
        "d962fc619efb2b904e5a8443838500ef2145452af117ab8f9cc3f97f".to_owned(),
    ]);
    assert_eq!(expected.len(), 6);
    assert_eq!(fixed_units(&Profile::load("preprod").unwrap()), expected);
}
