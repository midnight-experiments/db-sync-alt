use dsa_chain::{
    assets::{AssetRule, RULE_VERSION},
    profiles::Profile,
};

#[test]
fn configured_full_units_and_all_governance_names_are_retained() {
    assert_eq!(RULE_VERSION, 2);
    for network in ["mainnet", "preprod", "preview"] {
        let profile = Profile::load(network).unwrap();
        let rule = AssetRule::from_profile(&profile);
        for role in ["cnight", "auth", "permissioned_candidates", "bridge"] {
            let asset = &profile.assets[role];
            assert!(rule.allows(&asset.policy, &asset.name));
            assert!(!rule.allows(&asset.policy, &format!("{}ff", asset.name)));
        }
        for role in ["council", "technical_committee"] {
            assert!(rule.allows(&profile.assets[role].policy, "ff"));
        }
        assert!(!rule.allows(&"ff".repeat(28), ""));
        assert_eq!(rule.governance_policies().len(), 2);
    }
}

#[test]
fn preprod_empty_cnight_and_named_bridge_are_both_required() {
    let profile = Profile::load("preprod").unwrap();
    let rule = AssetRule::from_profile(&profile);
    assert_eq!(profile.assets["cnight"].name, "");
    assert_eq!(profile.assets["bridge"].name, "4e49474854");
    assert_eq!(
        profile.assets["cnight"].policy,
        profile.assets["bridge"].policy
    );
    assert!(
        rule.exact_units()
            .contains(&profile.assets["cnight"].unit())
    );
    assert!(
        rule.exact_units()
            .contains(&profile.assets["bridge"].unit())
    );
}

#[test]
fn ics_and_reserve_role_entries_add_no_token_exception() {
    let mut profile = Profile::load("preprod").unwrap();
    profile.assets.get_mut("ics").unwrap().name = "01".to_owned();
    profile.assets.get_mut("reserve").unwrap().policy = "ff".repeat(28);
    let rule = AssetRule::from_profile(&profile);
    assert!(!rule.allows(&profile.assets["ics"].policy, &profile.assets["ics"].name));
    assert!(!rule.allows(
        &profile.assets["reserve"].policy,
        &profile.assets["reserve"].name
    ));
}
