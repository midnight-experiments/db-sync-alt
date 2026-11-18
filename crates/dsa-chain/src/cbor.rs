//! Structural CBOR spans. Unconsumed values never become recursive trees.

use std::ops::Range;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code} at byte {offset}")]
pub struct Error {
    pub code: &'static str,
    pub offset: usize,
}

pub(crate) fn error(code: &'static str, offset: usize) -> Error {
    Error { code, offset }
}

#[derive(Clone, Copy, Debug)]
pub struct Item<'a> {
    pub raw: &'a [u8],
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Head {
    pub major: u8,
    pub arg: Option<u64>,
    pub width: u8,
}

pub(crate) struct Cursor<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| error("cbor_truncated", self.pos))?;
        let bytes = self
            .data
            .get(self.pos..end)
            .ok_or_else(|| error("cbor_truncated", self.pos))?;
        self.pos = end;
        Ok(bytes)
    }

    pub fn head(&mut self) -> Result<Head, Error> {
        let byte = self.take(1)?[0];
        let width = byte & 31;
        let arg = match width {
            0..=23 => Some(u64::from(width)),
            24..=27 => {
                let n = 1 << (width - 24);
                Some(
                    self.take(n)?
                        .iter()
                        .fold(0u64, |a, b| (a << 8) | u64::from(*b)),
                )
            }
            31 => None,
            _ => return Err(error("cbor_reserved_encoding", self.pos - 1)),
        };
        Ok(Head {
            major: byte >> 5,
            arg,
            width,
        })
    }

    /// Work and heap are linear in input bytes. Each stack entry is one u64;
    /// there is no recursion or allocation based on an untrusted length alone.
    pub fn skip(&mut self) -> Result<Range<usize>, Error> {
        const ARRAY: u64 = u64::MAX;
        const MAP_KEY: u64 = u64::MAX - 1;
        const MAP_VALUE: u64 = u64::MAX - 2;
        let start = self.pos;
        let mut todo = vec![1u64];
        while let Some(left) = todo.last_mut() {
            if *left == 0 {
                todo.pop();
                continue;
            }
            if *left >= MAP_VALUE {
                if self.data.get(self.pos) == Some(&0xff) {
                    if *left == MAP_VALUE {
                        return Err(error("cbor_map_missing_value", self.pos));
                    }
                    self.pos += 1;
                    todo.pop();
                    continue;
                }
                *left = match *left {
                    MAP_KEY => MAP_VALUE,
                    MAP_VALUE => MAP_KEY,
                    _ => ARRAY,
                };
            } else {
                *left -= 1;
            }
            let at = self.pos;
            let head = self.head()?;
            match head.major {
                0 | 1 if head.arg.is_some() => {}
                2 | 3 => {
                    if let Some(n) = head.arg {
                        let n = usize::try_from(n).map_err(|_| error("cbor_truncated", at))?;
                        let bytes = self.take(n)?;
                        if head.major == 3 && std::str::from_utf8(bytes).is_err() {
                            return Err(error("cbor_invalid_utf8", at));
                        }
                    } else {
                        loop {
                            if self.data.get(self.pos) == Some(&0xff) {
                                self.pos += 1;
                                break;
                            }
                            let chunk = self.head()?;
                            if chunk.major != head.major || chunk.arg.is_none() {
                                return Err(error("cbor_invalid_chunks", self.pos));
                            }
                            let n = usize::try_from(chunk.arg.unwrap())
                                .map_err(|_| error("cbor_truncated", at))?;
                            let bytes = self.take(n)?;
                            if head.major == 3 && std::str::from_utf8(bytes).is_err() {
                                return Err(error("cbor_invalid_utf8", at));
                            }
                        }
                    }
                }
                4 | 5 => {
                    if let Some(n) = head.arg {
                        let n = n
                            .checked_mul(if head.major == 5 { 2 } else { 1 })
                            .ok_or_else(|| error("cbor_truncated", at))?;
                        if n > (self.data.len() - self.pos) as u64 {
                            return Err(error("cbor_truncated", at));
                        }
                        todo.push(n);
                    } else {
                        todo.push(if head.major == 4 { ARRAY } else { MAP_KEY });
                    }
                }
                6 if head.arg.is_some() => todo.push(1),
                7 if head.width < 28 => {}
                _ => return Err(error("cbor_invalid_item", at)),
            }
        }
        Ok(start..self.pos)
    }

    pub fn item(&mut self) -> Result<Item<'a>, Error> {
        let span = self.skip()?;
        Ok(Item {
            raw: &self.data[span],
        })
    }
}

impl<'a> Item<'a> {
    pub fn parse(raw: &'a [u8]) -> Result<Self, Error> {
        let mut cursor = Cursor::new(raw);
        let item = cursor.item()?;
        if cursor.pos != raw.len() {
            return Err(error("cbor_trailing_bytes", cursor.pos));
        }
        Ok(item)
    }

    pub(crate) fn head(self) -> Result<Head, Error> {
        Cursor::new(self.raw).head()
    }

    pub fn unsigned(self) -> Result<u64, Error> {
        let h = self.head()?;
        if h.major == 0 {
            h.arg.ok_or_else(|| error("cbor_integer", 0))
        } else {
            Err(error("cbor_integer", 0))
        }
    }

    pub fn integer(self) -> Result<i128, Error> {
        let h = self.head()?;
        match (h.major, h.arg) {
            (0, Some(n)) => Ok(i128::from(n)),
            (1, Some(n)) => Ok(-1 - i128::from(n)),
            _ => Err(error("cbor_integer", 0)),
        }
    }

    pub fn bytes(self) -> Result<Vec<u8>, Error> {
        let mut c = Cursor::new(self.raw);
        let h = c.head()?;
        if h.major != 2 {
            return Err(error("cbor_bytes", 0));
        }
        if let Some(n) = h.arg {
            let n = usize::try_from(n).map_err(|_| error("cbor_truncated", 0))?;
            return Ok(c.take(n)?.to_vec());
        }
        let mut result = Vec::new();
        while c.data.get(c.pos) != Some(&0xff) {
            let chunk = c.head()?;
            if chunk.major != 2 {
                return Err(error("cbor_invalid_chunks", c.pos));
            }
            let n = chunk
                .arg
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| error("cbor_invalid_chunks", c.pos))?;
            result.extend_from_slice(c.take(n)?);
        }
        Ok(result)
    }

    pub fn array(self) -> Result<Vec<Self>, Error> {
        self.children(4)
    }
    pub fn map(self) -> Result<Vec<(Self, Self)>, Error> {
        let children = self.children(5)?;
        if children.len() % 2 != 0 {
            return Err(error("cbor_map_missing_value", 0));
        }
        Ok(children
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| (p[0], p[1]))
            .collect())
    }

    fn children(self, major: u8) -> Result<Vec<Self>, Error> {
        let mut c = Cursor::new(self.raw);
        let h = c.head()?;
        if h.major != major {
            return Err(error("cbor_container", 0));
        }
        let mut result = Vec::new();
        while c.pos < self.raw.len() && c.data[c.pos] != 0xff {
            result.push(c.item()?);
        }
        Ok(result)
    }

    pub fn tag(self) -> Result<(u64, Self), Error> {
        let mut c = Cursor::new(self.raw);
        let h = c.head()?;
        if h.major != 6 {
            return Err(error("cbor_tag", 0));
        }
        Ok((h.arg.ok_or_else(|| error("cbor_tag", 0))?, c.item()?))
    }

    pub fn is_null(self) -> bool {
        self.raw == [0xf6]
    }
    pub fn boolean(self) -> Result<bool, Error> {
        match self.raw {
            [0xf4] => Ok(false),
            [0xf5] => Ok(true),
            _ => Err(error("cbor_boolean", 0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_cbor_conformance() {
        // RFC 8949 Appendix A, including nonpreferred widths and chunked values.
        for text in [
            "00",
            "1818",
            "1bffffffffffffffff",
            "20",
            "3903e7",
            "f4",
            "f5",
            "f6",
            "f93c00",
            "fa47c35000",
            "fb3ff199999999999a",
            "5f42010243030405ff",
            "7f657374726561646d696e67ff",
            "83010203",
            "9f018202039f0405ffff",
            "a201020304",
            "bf61610161629f0203ffff",
            "c074323031332d30332d32315432303a30343a30305a",
        ] {
            Item::parse(&hex::decode(text).unwrap()).unwrap();
        }
        for text in [
            "",
            "18",
            "ff",
            "1c",
            "bf00ff",
            "5f6161ff",
            "61ff",
            "9bffffffffffffffff",
        ] {
            assert!(Item::parse(&hex::decode(text).unwrap()).is_err(), "{text}");
        }
    }

    #[test]
    fn generic_skip_million_levels_without_recursive_value_or_drop() {
        // Inert CBOR item only: never made into a ledger transaction, persisted,
        // sent over a network, or passed to any third-party decoder.
        let mut raw = vec![0x81; 1_000_000];
        raw.push(0);
        let start = std::time::Instant::now();
        assert_eq!(Item::parse(&raw).unwrap().raw.len(), 1_000_001);
        println!(
            "generic_skip_levels=1000000 elapsed_ms={}",
            start.elapsed().as_millis()
        );
    }
}
