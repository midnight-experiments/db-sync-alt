//! Iterative Plutus-data rendering with exact integers and ordered map pairs.

use num_bigint::{BigInt, BigUint};

use crate::cbor::{Error, Item, error};

pub const JSON_NESTING_LIMIT: usize = 2000;

enum Task<'a> {
    Value(Item<'a>, usize),
    Text(&'static str),
    Owned(String),
}

/// NULL beyond the db-sync JSON nesting limit. The original datum hash is
/// independent of its rendering and remains available when the value is NULL.
pub fn render(raw: &[u8]) -> Result<Option<String>, Error> {
    let root = Item::parse(raw)?;
    let mut tasks = vec![Task::Value(root, 1)];
    let mut json = String::new();
    while let Some(task) = tasks.pop() {
        match task {
            Task::Text(s) => json.push_str(s),
            Task::Owned(s) => json.push_str(&s),
            Task::Value(item, depth) => {
                if depth > JSON_NESTING_LIMIT {
                    return Ok(None);
                }
                let head = item.head()?;
                if matches!(head.major, 4 | 5) && depth >= JSON_NESTING_LIMIT {
                    return Ok(None);
                }
                match head.major {
                    0 | 1 => json.push_str(&format!("{{\"int\":{}}}", item.integer()?)),
                    2 => {
                        json.push_str(&format!("{{\"bytes\":\"{}\"}}", hex::encode(item.bytes()?)))
                    }
                    4 => {
                        json.push_str("{\"list\":[");
                        tasks.push(Task::Text("]}"));
                        sequence(&mut tasks, item.array()?, depth + 2);
                    }
                    5 => {
                        json.push_str("{\"map\":[");
                        tasks.push(Task::Text("]}"));
                        let pairs = item.map()?;
                        for (i, (key, value)) in pairs.into_iter().enumerate().rev() {
                            tasks.push(Task::Text("}"));
                            tasks.push(Task::Value(value, depth + 3));
                            tasks.push(Task::Text(",\"v\":"));
                            tasks.push(Task::Value(key, depth + 3));
                            tasks.push(Task::Text("{\"k\":"));
                            if i > 0 {
                                tasks.push(Task::Text(","));
                            }
                        }
                    }
                    6 => {
                        let (tag, content) = item.tag()?;
                        if tag == 2 || tag == 3 {
                            let n = BigUint::from_bytes_be(&content.bytes()?);
                            let integer = if tag == 2 {
                                BigInt::from(n)
                            } else {
                                -BigInt::from(n) - 1
                            };
                            json.push_str(&format!("{{\"int\":{integer}}}"));
                        } else {
                            if depth >= JSON_NESTING_LIMIT {
                                return Ok(None);
                            }
                            let (constructor, fields) = match tag {
                                121..=127 => (tag - 121, content),
                                1280..=1400 => (tag - 1280 + 7, content),
                                102 => {
                                    let parts = content.array()?;
                                    if parts.len() != 2 {
                                        return Err(error("cbor_datum_constructor", 0));
                                    }
                                    (parts[0].unsigned()?, parts[1])
                                }
                                _ => return Err(error("cbor_datum_tag", 0)),
                            };
                            json.push_str("{\"constructor\":");
                            tasks.push(Task::Text("]}"));
                            sequence(&mut tasks, fields.array()?, depth + 2);
                            tasks.push(Task::Text(",\"fields\":["));
                            tasks.push(Task::Owned(constructor.to_string()));
                        }
                    }
                    _ => return Err(error("cbor_datum_shape", 0)),
                }
            }
        }
    }
    Ok(Some(json))
}

fn sequence<'a>(tasks: &mut Vec<Task<'a>>, children: Vec<Item<'a>>, depth: usize) {
    for (i, child) in children.into_iter().enumerate().rev() {
        tasks.push(Task::Value(child, depth));
        if i > 0 {
            tasks.push(Task::Text(","));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ra_14_b_exact_integers_and_ordered_repeated_map_pairs() {
        for (raw, json) in [
            ("1bffffffffffffffff", "{\"int\":18446744073709551615}"),
            ("3bffffffffffffffff", "{\"int\":-18446744073709551616}"),
            ("c249010000000000000000", "{\"int\":18446744073709551616}"),
            (
                "a201020103",
                "{\"map\":[{\"k\":{\"int\":1},\"v\":{\"int\":2}},{\"k\":{\"int\":1},\"v\":{\"int\":3}}]}",
            ),
            (
                "d879820141ff",
                "{\"constructor\":0,\"fields\":[{\"int\":1},{\"bytes\":\"ff\"}]}",
            ),
        ] {
            assert_eq!(render(&hex::decode(raw).unwrap()).unwrap().unwrap(), json);
        }
    }
}
