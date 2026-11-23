//! Generate or check the committed profiles from the pinned Midnight node checkout.

use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Parser;
use dsa_chain::profiles::{
    Addresses, Asset, Assets, Cardano, NODE_COMMIT, NODE_RELEASE, Profile, Provenance,
    StartPosition,
};
use parity_scale_codec::Encode;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Parser)]
struct Arguments {
    /// Midnight node source checkout at the pinned commit.
    #[arg(long)]
    node_source: PathBuf,
    /// Profile directory to write or compare.
    #[arg(long, default_value = "crates/dsa-chain/profiles")]
    output: PathBuf,
    /// Refuse any difference without changing files.
    #[arg(long)]
    check: bool,
}

struct Source<'a> {
    root: &'a Path,
    files: BTreeMap<String, String>,
}

impl Source<'_> {
    fn read(&mut self, name: &str) -> Result<String, Box<dyn Error>> {
        let bytes = fs::read(self.root.join(name))?;
        self.files
            .insert(name.to_owned(), format!("{:x}", Sha256::digest(&bytes)));
        Ok(String::from_utf8(bytes)?)
    }

    fn json(&mut self, name: &str) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&self.read(name)?)?)
    }
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, Box<dyn Error>> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("invalid_node_config: {field}").into())
}

fn integer(value: &Value, field: &str) -> Result<u64, Box<dyn Error>> {
    value[field]
        .as_u64()
        .ok_or_else(|| format!("invalid_node_config: {field}").into())
}

fn hex_field(value: &str, bytes: usize) -> Result<String, Box<dyn Error>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if hex::decode(value)?.len() != bytes {
        return Err("invalid_node_config: hex_length".into());
    }
    Ok(value.to_ascii_lowercase())
}

fn asset(config: &Value) -> Result<Asset, Box<dyn Error>> {
    Ok(Asset {
        policy: hex_field(string(config, "policy_id")?, 28)?,
        name: hex::encode(string(config, "asset_name")?.as_bytes()),
    })
}

fn profile(root: &Path, network: &str) -> Result<Profile, Box<dyn Error>> {
    let mut source = Source {
        root,
        files: BTreeMap::new(),
    };
    let mut settings: toml::Table = toml::from_str(&source.read("res/cfg/default.toml")?)?;
    let overrides: toml::Table = toml::from_str(&source.read(&format!("res/cfg/{network}.toml"))?)?;
    settings.extend(overrides);
    let get = |name: &str| -> Result<u64, Box<dyn Error>> {
        settings
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("invalid_node_config: {name}").into())
    };
    let coefficient = settings
        .get("cardano_active_slots_coeff")
        .and_then(toml::Value::as_float)
        .ok_or("invalid_node_config: cardano_active_slots_coeff")?;
    let cardano = Cardano {
        security_parameter: get("cardano_security_parameter")?.try_into()?,
        active_slots_coefficient: coefficient,
        block_stability_margin: get("block_stability_margin")?.try_into()?,
        first_epoch_number: get("mc__first_epoch_number")?.try_into()?,
        first_epoch_timestamp_millis: get("mc__first_epoch_timestamp_millis")?,
        epoch_duration_millis: get("mc__epoch_duration_millis")?,
        first_slot_number: get("mc__first_slot_number")?,
        slot_duration_millis: get("mc__slot_duration_millis")?,
    };
    let pc = source.json(&format!("res/{network}/pc-chain-config.json"))?;
    for (name, expected) in [
        ("security_parameter", u64::from(cardano.security_parameter)),
        ("first_epoch_number", u64::from(cardano.first_epoch_number)),
        (
            "first_epoch_timestamp_millis",
            cardano.first_epoch_timestamp_millis,
        ),
        ("epoch_duration_millis", cardano.epoch_duration_millis),
        ("first_slot_number", cardano.first_slot_number),
        ("slot_duration_millis", cardano.slot_duration_millis),
    ] {
        if integer(&pc["cardano"], name)? != expected {
            return Err(format!("node_config_mismatch: {name}").into());
        }
    }
    if pc["cardano"]["active_slots_coeff"].as_f64() != Some(coefficient) {
        return Err("node_config_mismatch: active_slots_coeff".into());
    }
    let cn = source.json(&format!("res/{network}/cnight-config.json"))?;
    let governance = source.json(&format!("res/{network}/federated-authority-config.json"))?;
    let permissioned = source.json(&format!(
        "res/{network}/permissioned-candidates-config.json"
    ))?;
    let ics = source.json(&format!("res/{network}/ics-config.json"))?;
    let reserve = source.json(&format!("res/{network}/reserve-config.json"))?;
    let chain_spec = source.json(&format!("res/{network}/chain-spec.json"))?;
    let bridge = &chain_spec["genesis"]["runtimeGenesis"]["config"]["bridge"]["mainChainScripts"];
    let bridge_policy: [u8; 28] = hex::decode(hex_field(string(bridge, "token_policy_id")?, 28)?)?
        .try_into()
        .map_err(|_| "invalid_node_config: bridge_policy")?;
    let bridge_name = hex::decode(string(bridge, "token_asset_name")?.trim_start_matches("0x"))?;
    if bridge_name.len() > 32 {
        return Err("invalid_node_config: bridge_asset_name".into());
    }
    let bridge_address = string(bridge, "illiquid_circulation_supply_validator_address")?;
    if bridge_address != string(&ics, "illiquid_circulation_supply_validator_address")? {
        return Err("node_config_mismatch: bridge_address".into());
    }
    // Shipped raw genesis storage encodes policy, asset name and ICS address.
    // Verify its bytes independently of the chain-spec generator's input files.
    let raw_spec = source.json(&format!("res/{network}/chain-spec-raw.json"))?;
    let storage_key = "0x6bbd34f158a9e1a5e5600b03adee7932707a902fbb84d383419a2159ae15bbfb";
    let raw_bridge = raw_spec["genesis"]["raw"]["top"][storage_key]
        .as_str()
        .ok_or("missing_genesis_bridge_storage")?;
    let expected = (
        bridge_policy,
        bridge_name.clone(),
        bridge_address.as_bytes().to_vec(),
    )
        .encode();
    if hex::decode(raw_bridge.trim_start_matches("0x"))? != expected {
        return Err("node_config_mismatch: genesis_bridge_storage".into());
    }
    let mapping = string(&cn["addresses"], "mapping_validator_address")?;
    let (_, mapping_bytes) = bech32::decode(mapping)?;
    // The mapping validator is a Shelley enterprise script address.
    if mapping_bytes.len() != 29 || mapping_bytes[0] >> 4 != 7 {
        return Err("invalid_node_config: mapping_validator_address".into());
    }
    let addresses = Addresses {
        council: string(&governance["council"], "address")?.to_owned(),
        ics: string(&ics, "illiquid_circulation_supply_validator_address")?.to_owned(),
        mapping: mapping.to_owned(),
        reserve: string(&reserve, "reserve_validator_address")?.to_owned(),
        technical_committee: string(&governance["technical_committee"], "address")?.to_owned(),
    };
    let assets = Assets {
        auth: Asset {
            policy: hex::encode(&mapping_bytes[1..]),
            name: hex::encode(string(&cn["addresses"], "auth_token_asset_name")?.as_bytes()),
        },
        bridge: Asset {
            policy: hex::encode(bridge_policy),
            name: hex::encode(bridge_name),
        },
        cnight: Asset {
            policy: hex_field(string(&cn["addresses"], "cnight_policy_id")?, 28)?,
            name: hex::encode(string(&cn["addresses"], "cnight_asset_name")?.as_bytes()),
        },
        council: Asset {
            policy: hex_field(string(&governance["council"], "policy_id")?, 28)?,
            name: String::new(),
        },
        ics: asset(&ics["asset"])?,
        permissioned_candidates: Asset {
            policy: hex_field(
                string(&permissioned, "permissioned_candidates_policy_id")?,
                28,
            )?,
            name: String::new(),
        },
        reserve: asset(&reserve["asset"])?,
        technical_committee: Asset {
            policy: hex_field(string(&governance["technical_committee"], "policy_id")?, 28)?,
            name: String::new(),
        },
    };
    let position = &cn["next_cardano_position"];
    let start = StartPosition {
        block_number: integer(position, "block_number")?.try_into()?,
        block_hash: hex_field(string(position, "block_hash")?, 32)?,
        block_timestamp_millis: integer(position, "block_timestamp")?,
        tx_index_in_block: integer(position, "tx_index_in_block")?.try_into()?,
    };
    dsa_chain::ids::transaction_id(start.block_number, start.tx_index_in_block)?;
    Ok(Profile {
        format: 1,
        network: network.to_owned(),
        node_release: NODE_RELEASE.to_owned(),
        cardano,
        start,
        addresses,
        assets,
        provenance: Provenance {
            repository: "https://github.com/midnightntwrk/midnight-node".to_owned(),
            commit: NODE_COMMIT.to_owned(),
            files: source.files,
        },
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = Arguments::parse();
    let head = Command::new("git")
        .arg("-C")
        .arg(&arguments.node_source)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !head.status.success() || String::from_utf8(head.stdout)?.trim() != NODE_COMMIT {
        return Err("node_source_commit_mismatch".into());
    }
    if !arguments.check {
        fs::create_dir_all(&arguments.output)?;
    }
    for network in ["mainnet", "preprod", "preview"] {
        let profile = profile(&arguments.node_source, network)?;
        // Reject edits to consumed files even when HEAD still matches.
        for name in profile.provenance.files.keys() {
            let status = Command::new("git")
                .arg("-C")
                .arg(&arguments.node_source)
                .args(["diff", "--quiet", "HEAD", "--", name])
                .status()?;
            if !status.success() {
                return Err(format!("node_source_dirty: {name}").into());
            }
        }
        let rendered = serde_json::to_string_pretty(&profile)? + "\n";
        let output = arguments.output.join(format!("{network}.json"));
        if arguments.check {
            if fs::read_to_string(&output)? != rendered {
                return Err(format!("generated_profile_mismatch: {network}").into());
            }
        } else {
            fs::write(&output, rendered)?;
        }
        println!("{network} {}", profile.hash()?);
    }
    Ok(())
}
