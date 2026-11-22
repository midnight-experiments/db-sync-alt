//! Provider outcomes. Every error carries a static code and says what the
//! caller may do next; it never carries the key, a URL or a response body.

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Kind {
    /// Retry from `until` (Unix seconds) with the caller's own backoff.
    Retry { until: u64 },
    /// The provider refused for a while (402, 418, 429); every request
    /// waits until `until`.
    Wait { until: u64 },
    /// Retrying cannot help; the named setting must change.
    Fatal { field: &'static str },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, thiserror::Error)]
#[error("{code}")]
pub struct Error {
    pub code: &'static str,
    pub kind: Kind,
    /// The provider's HTTP status, when this request received one.
    pub http_status: Option<u16>,
}

impl Error {
    pub(crate) fn configuration(field: &'static str) -> Self {
        Self {
            code: "invalid_provider_configuration",
            kind: Kind::Fatal { field },
            http_status: None,
        }
    }

    pub(crate) fn retry(code: &'static str, until: u64) -> Self {
        Self {
            code,
            kind: Kind::Retry { until },
            http_status: None,
        }
    }

    /// When the caller may try again; `None` for a fatal error.
    pub fn until(&self) -> Option<u64> {
        match self.kind {
            Kind::Retry { until } | Kind::Wait { until } => Some(until),
            Kind::Fatal { .. } => None,
        }
    }
}
