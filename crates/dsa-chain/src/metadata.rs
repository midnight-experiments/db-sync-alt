//! Bridge metadatum normalization and the node's recipient reading, without
//! expanding unconsumed auxiliary CBOR or JSON into recursive value trees.

use num_bigint::BigInt;
use num_traits::FromPrimitive;

use crate::{
    cbor::{Error, Item, error},
    transaction::{Transaction, hash},
};

const LABEL: u64 = 6500973;

/// The provider's rendering is authoritative. Preserve exact integer tokens;
/// integral floating tokens follow the legacy normalization through f64.
pub fn normalize(raw: &str) -> Result<Option<String>, Error> {
    if raw.trim() == "null" {
        return Ok(None);
    }
    let bytes = raw.as_bytes();
    let (mut at, mut depth) = (0usize, 0usize);
    let mut result = String::with_capacity(raw.len());
    while at < bytes.len() {
        match bytes[at] {
            b'"' => {
                let start = at;
                at += 1;
                while at < bytes.len() {
                    if bytes[at] == b'\\' {
                        at = at
                            .checked_add(2)
                            .ok_or_else(|| error("provider_bad_metadata", at))?;
                    } else if bytes[at] == b'"' {
                        at += 1;
                        break;
                    } else {
                        at += 1;
                    }
                }
                let token = raw
                    .get(start..at)
                    .ok_or_else(|| error("provider_bad_metadata", start))?;
                let Ok(value) = serde_json::from_str::<String>(token) else {
                    return Ok(None);
                };
                if value.contains('\0') {
                    return Ok(None);
                }
                result.push_str(token);
            }
            b'{' | b'[' => {
                depth += 1;
                if depth > 2000 {
                    return Ok(None);
                }
                result.push(bytes[at] as char);
                at += 1;
            }
            b'}' | b']' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| error("provider_bad_metadata", at))?;
                result.push(bytes[at] as char);
                at += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = at;
                at += 1;
                while at < bytes.len() && b"0123456789.eE+-".contains(&bytes[at]) {
                    at += 1;
                }
                let token = &raw[start..at];
                if token.bytes().any(|b| b".eE".contains(&b)) {
                    let value: f64 = token
                        .parse()
                        .map_err(|_| error("provider_bad_metadata", start))?;
                    if !value.is_finite() {
                        return Err(error("provider_bad_metadata", start));
                    }
                    if value.fract() == 0.0 {
                        result.push_str(
                            &BigInt::from_f64(value)
                                .ok_or_else(|| error("provider_bad_metadata", start))?
                                .to_string(),
                        );
                    } else {
                        result.push_str(&value.to_string());
                    }
                } else {
                    result.push_str(token);
                }
            }
            b' ' | b'\n' | b'\r' | b'\t' => {
                at += 1;
            }
            _ => {
                result.push(bytes[at] as char);
                at += 1;
            }
        }
    }
    if depth != 0 {
        return Err(error("provider_bad_metadata", at));
    }
    let mut validation = serde_json::Deserializer::from_str(&result);
    validation.disable_recursion_limit();
    <serde::de::IgnoredAny as serde::Deserialize>::deserialize(&mut validation)
        .map_err(|_| error("provider_bad_metadata", 0))?;
    validation
        .end()
        .map_err(|_| error("provider_bad_metadata", 0))?;
    Ok(Some(result))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    Absent,
    Present(Option<Vec<u8>>),
    Unknown,
}

fn recipient(item: Item<'_>) -> Option<Vec<u8>> {
    let elements = item.array().ok()?;
    if elements.len() != 1 {
        return None;
    }
    match elements[0].head().ok()?.major {
        2 => elements[0].bytes().ok(),
        3 => hex::decode(elements[0].text().ok()?.strip_prefix("0x")?).ok(),
        _ => None,
    }
}

pub fn observe(raw: Option<&[u8]>) -> Observation {
    fn read(raw: &[u8]) -> Result<Observation, Error> {
        let mut root = Item::parse(raw)?;
        match root.head()?.major {
            6 => {
                let (tag, inner) = root.tag()?;
                if tag != 259 {
                    return Ok(Observation::Unknown);
                }
                let mut found = None;
                for (key, value) in inner.map()? {
                    if key.unsigned()? == 0 {
                        found = Some(value);
                        break;
                    }
                }
                let Some(value) = found else {
                    return Ok(Observation::Absent);
                };
                root = value;
            }
            4 => {
                let entries = root.array()?;
                let Some(value) = entries.first() else {
                    return Ok(Observation::Unknown);
                };
                root = *value;
            }
            5 => {}
            _ => return Ok(Observation::Unknown),
        }
        let mut values = Vec::new();
        for (key, value) in root.map()? {
            if key.unsigned()? == LABEL {
                values.push(recipient(value));
            }
        }
        values.dedup();
        Ok(match values.as_slice() {
            [] => Observation::Absent,
            [value] => Observation::Present(value.clone()),
            _ => Observation::Unknown,
        })
    }
    match raw {
        None => Observation::Absent,
        Some(raw) => read(raw).unwrap_or(Observation::Unknown),
    }
}

pub fn checked(tx: &Transaction<'_>, provider: Option<&str>) -> Result<Option<String>, Error> {
    if tx.auxiliary.map(hash) != tx.auxiliary_hash {
        return Err(error("provider_metadata_conflict", 0));
    }
    let normalized = provider.map(normalize).transpose()?.flatten();
    let actual = normalized
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .filter(|v| v.len() == 1)
        .and_then(|v| v[0].strip_prefix("0x").and_then(|s| hex::decode(s).ok()));
    match observe(tx.auxiliary) {
        Observation::Absent if provider.is_some() => {
            return Err(error("provider_metadata_conflict", 0));
        }
        Observation::Present(expected) if provider.is_none() || actual != expected => {
            return Err(error("provider_metadata_conflict", 0));
        }
        _ => {}
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ra_14_a_7_standard_auxiliary_forms_and_recipients() {
        for raw in [
            "a11a0063326d81420102",
            "d90103a100a11a0063326d81420102",
            "82a11a0063326d8142010280",
        ] {
            assert_eq!(
                observe(Some(&hex::decode(raw).unwrap())),
                Observation::Present(Some(vec![1, 2]))
            );
        }
        assert_eq!(observe(None), Observation::Absent);
    }
    #[test]
    fn ra_14_c_bridge_metadata_null_unicode_depth_and_integral_numbers() {
        assert_eq!(
            normalize("[1e21,1.0,9007199254740993]").unwrap().unwrap(),
            "[1000000000000000000000,1,9007199254740993]"
        );
        for raw in ["null", "[\"\\u0000\"]", "[\"\\ud800\"]"] {
            assert_eq!(normalize(raw).unwrap(), None);
        }
        assert_eq!(
            normalize("[\"\\\\u0000\"]").unwrap().unwrap(),
            "[\"\\\\u0000\"]"
        );
    }
}
