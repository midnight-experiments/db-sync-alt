//! Stream the UTXO fallback while discarding historical input renderings.

use std::{
    cell::Cell,
    io::{self, Read},
    rc::Rc,
};

use bytes::Bytes;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::types::{Hash32, TransactionOutputs};

struct Budget {
    active: Cell<bool>,
    retained: Cell<usize>,
    maximum: usize,
    fault: Cell<Option<&'static str>>,
}

struct Reader {
    receiver: mpsc::Receiver<Bytes>,
    current: Bytes,
    budget: Rc<Budget>,
    maximum_depth: u16,
    depth: u32,
    string: bool,
    escape: bool,
}

impl Read for Reader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.current.is_empty() {
            match self.receiver.blocking_recv() {
                Some(chunk) => self.current = chunk,
                None => return Ok(0),
            }
        }
        let length = output.len().min(self.current.len());
        for (index, byte) in self.current[..length].iter().copied().enumerate() {
            if self.budget.active.get() {
                let count = self.budget.retained.get() + 1;
                if count > self.budget.maximum {
                    self.budget.fault.set(Some("provider_response_size_limit"));
                    return Err(io::Error::other("provider_response_size_limit"));
                }
                self.budget.retained.set(count);
            }
            if self.string {
                if self.escape {
                    self.escape = false;
                } else if byte == b'\\' {
                    self.escape = true;
                } else if byte == b'"' {
                    self.string = false;
                }
            } else {
                match byte {
                    b'"' => self.string = true,
                    b'[' | b'{' => {
                        self.depth += 1;
                        if self.depth > u32::from(self.maximum_depth) {
                            self.budget.fault.set(Some("provider_json_depth_limit"));
                            return Err(io::Error::other("provider_json_depth_limit"));
                        }
                    }
                    b']' | b'}' => self.depth = self.depth.saturating_sub(1),
                    _ => {}
                }
            }
            output[index] = byte;
        }
        self.current = self.current.slice(length..);
        Ok(length)
    }
}

struct Seed(Rc<Budget>);
impl<'de> DeserializeSeed<'de> for Seed {
    type Value = TransactionOutputs;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Seed {
    type Value = TransactionOutputs;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a transaction UTXO object")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut hash: Option<Hash32> = None;
        let mut outputs = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "hash" => {
                    if hash.is_some() {
                        return Err(de::Error::duplicate_field("hash"));
                    }
                    self.0.active.set(true);
                    hash = Some(map.next_value()?);
                    self.0.active.set(false);
                }
                "outputs" => {
                    if outputs.is_some() {
                        return Err(de::Error::duplicate_field("outputs"));
                    }
                    self.0.active.set(true);
                    outputs = Some(map.next_value()?);
                    self.0.active.set(false);
                }
                _ => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        Ok(TransactionOutputs {
            hash: hash.ok_or_else(|| de::Error::missing_field("hash"))?,
            outputs: outputs.ok_or_else(|| de::Error::missing_field("outputs"))?,
        })
    }
}

pub(crate) type DecodeTask = JoinHandle<Result<TransactionOutputs, &'static str>>;

pub(crate) fn start(maximum: usize, maximum_depth: u16) -> (mpsc::Sender<Bytes>, DecodeTask) {
    let (sender, receiver) = mpsc::channel(2);
    let task = tokio::task::spawn_blocking(move || {
        let budget = Rc::new(Budget {
            active: Cell::new(false),
            retained: Cell::new(0),
            maximum,
            fault: Cell::new(None),
        });
        let reader = Reader {
            receiver,
            current: Bytes::new(),
            budget: budget.clone(),
            maximum_depth,
            depth: 0,
            string: false,
            escape: false,
        };
        let mut decoder = serde_json::Deserializer::from_reader(reader);
        let result = Seed(budget.clone())
            .deserialize(&mut decoder)
            .and_then(|row| decoder.end().map(|()| row));
        result.map_err(|_| budget.fault.get().unwrap_or("provider_invalid_response"))
    });
    (sender, task)
}
