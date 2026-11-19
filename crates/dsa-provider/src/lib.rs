//! Kept-alive HTTPS requests, typed Blockfrost responses and durable attempt
//! reservations. Callers retry according to the returned deadline. Client-wide
//! cooldowns prevent queued requests from bypassing a provider refusal.

mod accounting;
pub mod evidence;
mod outputs;
pub mod pages;
pub mod supplements;
pub mod types;

pub use accounting::{
    AttemptRecorder, Clock, DailyCount, RecordFuture, RecorderError, SystemClock,
};
pub use types::{Endpoint, Reply};

use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use accounting::Counts;
use futures_util::{Stream, StreamExt, stream};
use reqwest::{Url, header::HeaderValue};
use serde::Serialize;
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore},
    time::Instant,
};
use types::Family;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Retry,
    Fatal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, thiserror::Error)]
#[error("{code}")]
pub struct Error {
    pub code: &'static str,
    pub action: Action,
    pub field: Option<&'static str>,
    pub http_status: Option<u16>,
    pub retry_at: Option<u64>,
}

impl Error {
    pub(crate) fn configuration(field: &'static str) -> Self {
        Self {
            code: "invalid_provider_configuration",
            action: Action::Fatal,
            field: Some(field),
            http_status: None,
            retry_at: None,
        }
    }
    pub(crate) fn retry(code: &'static str, at: u64) -> Self {
        Self {
            code,
            action: Action::Retry,
            field: None,
            http_status: None,
            retry_at: Some(at),
        }
    }
}

/// Transport byte ceilings are configurable separately for each response family.
/// CBOR hex doubles binary size. The default CBOR ceiling exceeds the 90,112-byte
/// block-body bound plus 100 reconstructed transaction envelopes and JSON keys.
/// JSON-expanded metadata and historical UTXO answers have separate budgets.
#[derive(Clone, Debug)]
pub struct Limits {
    pub headers: usize,
    pub block_cbor: usize,
    pub genesis: usize,
    pub epochs: usize,
    pub pools: usize,
    pub datum: usize,
    pub metadata: usize,
    pub transaction_outputs: usize,
    pub transaction_output_values: usize,
    pub json_depth: u16,
    pub evidence_memory: usize,
    pub evidence_spool: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            headers: 1024 * 1024,
            block_cbor: 4 * 1024 * 1024,
            genesis: 64 * 1024,
            epochs: 256 * 1024,
            pools: 2 * 1024 * 1024,
            datum: 4 * 1024 * 1024,
            metadata: 8 * 1024 * 1024,
            transaction_outputs: 128 * 1024 * 1024,
            transaction_output_values: 8 * 1024 * 1024,
            json_depth: 2048,
            evidence_memory: 4 * 1024 * 1024,
            evidence_spool: 256 * 1024 * 1024,
        }
    }
}

impl Limits {
    fn maximum(&self, family: Family) -> usize {
        match family {
            Family::Header => self.headers,
            Family::BlockCbor => self.block_cbor,
            Family::Genesis => self.genesis,
            Family::Epoch => self.epochs,
            Family::Pool => self.pools,
            Family::Datum => self.datum,
            Family::Metadata => self.metadata,
            Family::Outputs => self.transaction_outputs,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("provider.limits.evidence_memory", self.evidence_memory),
            ("provider.limits.evidence_spool", self.evidence_spool),
            ("provider.limits.headers", self.headers),
            ("provider.limits.block_cbor", self.block_cbor),
            ("provider.limits.genesis", self.genesis),
            ("provider.limits.epochs", self.epochs),
            ("provider.limits.pools", self.pools),
            ("provider.limits.datum", self.datum),
            ("provider.limits.metadata", self.metadata),
            (
                "provider.limits.transaction_outputs",
                self.transaction_outputs,
            ),
            (
                "provider.limits.transaction_output_values",
                self.transaction_output_values,
            ),
        ] {
            if value == 0 || value > isize::MAX as usize - 65536 {
                return Err(Error::configuration(field));
            }
        }
        if self.json_depth == 0 {
            return Err(Error::configuration("provider.limits.json_depth"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Config {
    pub base_url: String,
    pub key_file: PathBuf,
    pub root_certificate: Option<PathBuf>,
    pub requests_per_second: f64,
    pub max_in_flight: usize,
    pub request_timeout: Duration,
    pub limits: Limits,
}

impl Config {
    pub fn new(base_url: impl Into<String>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            base_url: base_url.into(),
            key_file: key_file.into(),
            root_certificate: None,
            requests_per_second: 8.0,
            max_in_flight: 4,
            request_timeout: Duration::from_secs(60),
            limits: Limits::default(),
        }
    }

    fn validate(&self) -> Result<(Url, Duration), Error> {
        self.limits.validate()?;
        if !self.requests_per_second.is_finite()
            || self.requests_per_second <= 0.0
            || self.requests_per_second > 10.0
        {
            return Err(Error::configuration("provider.requests_per_second"));
        }
        let interval = Duration::try_from_secs_f64(1.0 / self.requests_per_second)
            .map_err(|_| Error::configuration("provider.requests_per_second"))?;
        if Instant::now().checked_add(interval).is_none() {
            return Err(Error::configuration("provider.requests_per_second"));
        }
        if self.max_in_flight == 0 || self.max_in_flight > Semaphore::MAX_PERMITS {
            return Err(Error::configuration("provider.max_in_flight"));
        }
        if self.request_timeout.is_zero()
            || Instant::now().checked_add(self.request_timeout).is_none()
        {
            return Err(Error::configuration("provider.request_timeout"));
        }
        let mut url =
            Url::parse(&self.base_url).map_err(|_| Error::configuration("provider.base_url"))?;
        let loopback = url
            .host_str()
            .and_then(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .is_some_and(|host| host.is_loopback());
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
        {
            return Err(Error::configuration("provider.base_url"));
        }
        let path = format!("{}/", url.path().trim_end_matches('/'));
        url.set_path(&path);
        Ok((url, interval))
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Refusal {
    pub http_status: u16,
    pub code: &'static str,
    pub at: u64,
    pub wait_until: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub utc_day: String,
    pub calls_today: u64,
    pub calls_previous_days: Vec<DailyCount>,
    pub pace_requests_per_second: f64,
    pub requests_in_flight: usize,
    pub last_refusal: Option<Refusal>,
    pub retry_at: Option<u64>,
}

#[derive(Default)]
struct Control {
    failures: u32,
    gate: Option<Error>,
    last_refusal: Option<Refusal>,
}

struct Inner {
    http: reqwest::Client,
    base_url: Url,
    key: String,
    header: HeaderValue,
    config: Config,
    interval: Duration,
    next_send: AsyncMutex<Instant>,
    permits: Semaphore,
    in_flight: AtomicUsize,
    control: Mutex<Control>,
    counts: Mutex<Counts>,
    recorder: Arc<dyn AttemptRecorder>,
    clock: Arc<dyn Clock>,
    evidence_budget: Arc<evidence::Budget>,
}

#[derive(Clone)]
pub struct Provider(Arc<Inner>);

fn read_bounded(path: &Path, maximum: usize, field: &'static str) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(maximum as u64 + 1).read_to_end(&mut bytes))
        .map_err(|_| Error::configuration(field))?;
    if bytes.len() > maximum {
        return Err(Error::configuration(field));
    }
    Ok(bytes)
}

impl Provider {
    pub fn new(
        config: Config,
        recorder: Arc<dyn AttemptRecorder>,
        counts: Vec<DailyCount>,
    ) -> Result<Self, Error> {
        Self::with_clock(config, recorder, counts, Arc::new(SystemClock))
    }

    pub fn with_clock(
        config: Config,
        recorder: Arc<dyn AttemptRecorder>,
        counts: Vec<DailyCount>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let (base_url, interval) = config.validate()?;
        let counts = Counts::new(counts, clock.unix_seconds())?;
        let bytes = read_bounded(&config.key_file, 512, "provider.key_file")?;
        let key = std::str::from_utf8(&bytes)
            .map_err(|_| Error::configuration("provider.key_file"))?
            .trim_end_matches(['\n', '\r'])
            .to_owned();
        if base_url.as_str().contains(&key) && !key.is_empty() {
            return Err(Error::configuration("provider.base_url"));
        }
        if key.is_empty() {
            return Err(Error::configuration("provider.key_file"));
        }
        let mut header =
            HeaderValue::from_str(&key).map_err(|_| Error::configuration("provider.key_file"))?;
        header.set_sensitive(true);
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = &config.root_certificate {
            let bytes = read_bounded(path, 1024 * 1024, "provider.root_certificate")?;
            let certificates = rustls_pemfile::certs(&mut bytes.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Error::configuration("provider.root_certificate"))?;
            if certificates.is_empty() {
                return Err(Error::configuration("provider.root_certificate"));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|_| Error::configuration("provider.root_certificate"))?;
            }
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::configuration("provider.root_certificate"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut builder = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .timeout(config.request_timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(config.max_in_flight)
            .tcp_keepalive(Duration::from_secs(60))
            .user_agent(concat!("db-sync-alt/", env!("CARGO_PKG_VERSION")));
        if base_url.scheme() == "http" {
            builder = builder.no_proxy();
        }
        let http = builder
            .build()
            .map_err(|_| Error::configuration("provider.base_url"))?;
        Ok(Self(Arc::new(Inner {
            evidence_budget: evidence::Budget::new(
                config.limits.evidence_memory,
                config.limits.evidence_spool,
            ),
            http,
            base_url,
            key,
            header,
            interval,
            next_send: AsyncMutex::new(Instant::now()),
            permits: Semaphore::new(config.max_in_flight),
            in_flight: AtomicUsize::new(0),
            control: Mutex::new(Control::default()),
            counts: Mutex::new(counts),
            recorder,
            clock,
            config,
        })))
    }

    pub fn status(&self) -> Result<Status, Error> {
        let (utc_day, calls_today, calls_previous_days) = self
            .0
            .counts
            .lock()
            .unwrap()
            .snapshot(self.0.clock.unix_seconds())?;
        let control = self.0.control.lock().unwrap();
        Ok(Status {
            utc_day,
            calls_today,
            calls_previous_days,
            pace_requests_per_second: self.0.config.requests_per_second,
            requests_in_flight: self.0.in_flight.load(Ordering::SeqCst),
            last_refusal: control.last_refusal.clone(),
            retry_at: control.gate.as_ref().and_then(|error| error.retry_at),
        })
    }

    /// At most max_in_flight answers are buffered, in input order. Independent
    /// calls and cloned clients share the same pacing, semaphore and cooldown.
    pub fn ordered(
        &self,
        endpoints: Vec<Endpoint>,
    ) -> impl Stream<Item = Result<Reply, Error>> + '_ {
        stream::iter(endpoints)
            .map(|endpoint| self.fetch(endpoint))
            .buffered(self.0.config.max_in_flight)
    }

    fn gate(&self) -> Result<(), Error> {
        let now = self.0.clock.unix_seconds();
        let mut control = self.0.control.lock().unwrap();
        if let Some(error) = &control.gate {
            if error.action == Action::Fatal || error.retry_at.is_some_and(|until| until > now) {
                return Err(error.clone());
            }
            control.gate = None;
        }
        Ok(())
    }

    fn failure(&self, code: &'static str, status: Option<u16>, retry_after: Option<u64>) -> Error {
        let now = self.0.clock.unix_seconds();
        let mut control = self.0.control.lock().unwrap();
        control.failures = control.failures.saturating_add(1);
        let delay = match control.failures {
            1 => 1,
            2 => 2,
            3 => 4,
            4 => 8,
            5 => 16,
            _ => 30,
        };
        let until = match status {
            Some(402) => now / 86400 * 86400 + 86400,
            Some(418) => now.saturating_add(retry_after.unwrap_or(3600).clamp(3600, 86400)),
            Some(429) => now.saturating_add(retry_after.unwrap_or(60).clamp(1, 86400)),
            _ => now.saturating_add(delay),
        };
        let mut error = Error::retry(code, until);
        error.http_status = status;
        if matches!(status, Some(401 | 403)) {
            error.action = Action::Fatal;
            error.field = Some("provider.key_file");
            error.retry_at = None;
        }
        if matches!(status, Some(402 | 418 | 429)) {
            control.last_refusal = Some(Refusal {
                http_status: status.unwrap(),
                code,
                at: now,
                wait_until: until,
            });
        }
        let replace = control.gate.as_ref().is_none_or(|old| {
            old.action != Action::Fatal
                && (error.action == Action::Fatal || error.retry_at >= old.retry_at)
        });
        if replace {
            control.gate = Some(error.clone());
        }
        error
    }

    async fn output_response(
        &self,
        mut response: reqwest::Response,
        endpoint: &Endpoint,
        limit: usize,
        mut spool: Option<evidence::Spool>,
    ) -> Result<evidence::CapturedReply, Error> {
        let (sender, decoder) = outputs::start(
            self.0.config.limits.transaction_output_values,
            self.0.config.limits.json_depth,
        );
        let mut received = 0usize;
        let mut transport_error = None;
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if received
                        .checked_add(chunk.len())
                        .is_none_or(|length| length > limit)
                    {
                        transport_error = Some("provider_response_size_limit");
                        break;
                    }
                    received += chunk.len();
                    if let Some(spool) = spool.as_mut() {
                        spool.write(&chunk).map_err(|_| {
                            self.failure("unsafe_or_unwritable_evidence", None, None)
                        })?;
                    }
                    if sender.send(chunk).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    transport_error = Some(if error.is_timeout() {
                        "provider_timeout"
                    } else {
                        "provider_unavailable"
                    });
                    break;
                }
            }
        }
        drop(sender);
        let decoded = decoder
            .await
            .map_err(|_| self.failure("provider_unavailable", None, None))?;
        if let Some(code) = transport_error {
            return Err(self.failure(code, None, None));
        }
        let row = decoded.map_err(|code| self.failure(code, None, None))?;
        endpoint
            .validate_outputs(&row)
            .map_err(|code| self.failure(code, None, None))?;
        let key = &self.0.key;
        let reflected = row.hash.as_str().contains(key)
            || row.outputs.iter().any(|output| {
                output.address.contains(key)
                    || output
                        .data_hash
                        .as_ref()
                        .is_some_and(|hash| hash.as_str().contains(key))
                    || output
                        .inline_datum
                        .as_ref()
                        .is_some_and(|cbor| cbor.as_str().contains(key))
                    || output
                        .consumed_by_tx
                        .as_ref()
                        .is_some_and(|hash| hash.as_str().contains(key))
                    || output.amount.iter().any(|amount| {
                        amount.unit.contains(key) || amount.quantity.0.to_string().contains(key)
                    })
            });
        if reflected {
            return Err(self.failure("unsafe_provider_response", None, None));
        }
        Ok(evidence::CapturedReply {
            endpoint: endpoint.clone(),
            reply: Reply::TransactionOutputs(row),
            evidence: spool
                .map(|spool| spool.finish(endpoint.clone(), self.0.clock.unix_seconds())),
        })
    }

    pub async fn fetch(&self, endpoint: Endpoint) -> Result<Reply, Error> {
        Ok(self.fetch_inner(endpoint, None).await?.reply)
    }
    /// Retain only still-unobserved source families. Uses exactly the same send,
    /// limits and decoder as fetch; never issues an evidence-specific request.
    pub async fn fetch_with_evidence(
        &self,
        endpoint: Endpoint,
        pending: &BTreeSet<dsa_chain::observations::Shape>,
        spool_directory: &Path,
    ) -> Result<evidence::CapturedReply, Error> {
        let directory = evidence::wanted(&endpoint, pending).then_some(spool_directory);
        self.fetch_inner(endpoint, directory).await
    }
    async fn fetch_inner(
        &self,
        endpoint: Endpoint,
        evidence_directory: Option<&Path>,
    ) -> Result<evidence::CapturedReply, Error> {
        let path = endpoint.path()?;
        if path.contains(&self.0.key) {
            return Err(Error::configuration("request.path"));
        }
        self.gate()?;
        let _permit = self
            .0
            .permits
            .acquire()
            .await
            .expect("provider semaphore stays open");
        let mut next = self.0.next_send.lock().await;
        self.gate()?;
        tokio::time::sleep_until(*next).await;
        self.gate()?;
        let at = self.0.clock.unix_seconds();
        let count = self.0.counts.lock().unwrap().next(at)?;
        self.0
            .recorder
            .record(count.clone())
            .await
            .map_err(|_| self.failure("provider_accounting_unavailable", None, None))?;
        self.0.counts.lock().unwrap().committed(at, count.requests);
        self.gate()?;
        *next = Instant::now() + self.0.interval;
        let url = self
            .0
            .base_url
            .join(path.trim_start_matches('/'))
            .expect("typed relative provider path");
        self.0.in_flight.fetch_add(1, Ordering::SeqCst);
        let _flight = InFlight(&self.0.in_flight);
        let request = self
            .0
            .http
            .get(url)
            .header("project_id", self.0.header.clone())
            .header("accept-encoding", "identity");
        drop(next);
        let mut response = request.send().await.map_err(|error| {
            self.failure(
                if error.is_timeout() {
                    "provider_timeout"
                } else {
                    "provider_unavailable"
                },
                None,
                None,
            )
        })?;
        let status = response.status().as_u16();
        if status == 404 && matches!(endpoint, Endpoint::Datum(_)) {
            return Ok(evidence::CapturedReply {
                endpoint,
                reply: Reply::Datum(None),
                evidence: None,
            });
        }
        if status != 200 {
            let after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| retry_after(value, self.0.clock.unix_seconds()));
            let code = match status {
                401 | 403 => "provider_key_rejected",
                402 => "quota_exhausted",
                418 => "provider_banned",
                429 => "provider_rate_limited",
                404 => endpoint.missing_code(),
                500..=599 => "provider_unavailable",
                _ => "provider_request_rejected",
            };
            return Err(self.failure(code, Some(status), after));
        }
        let limit = self.0.config.limits.maximum(endpoint.family());
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(self.failure("provider_response_size_limit", None, None));
        }
        if matches!(endpoint, Endpoint::TransactionOutputs(_)) {
            let spool = evidence_directory
                .map(|directory| {
                    evidence::Spool::new(directory, &self.0.key, &self.0.evidence_budget)
                })
                .transpose()
                .map_err(|_| self.failure("evidence_storage_unavailable", None, None))?;
            let reply = self
                .output_response(response, &endpoint, limit, spool)
                .await?;
            self.0.control.lock().unwrap().failures = 0;
            return Ok(reply);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            self.failure(
                if error.is_timeout() {
                    "provider_timeout"
                } else {
                    "provider_unavailable"
                },
                None,
                None,
            )
        })? {
            if body
                .len()
                .checked_add(chunk.len())
                .is_none_or(|length| length > limit)
            {
                return Err(self.failure("provider_response_size_limit", None, None));
            }
            body.extend_from_slice(&chunk);
        }
        inspect_json(&body, self.0.config.limits.json_depth, &self.0.key)
            .map_err(|code| self.failure(code, None, None))?;
        let reply = endpoint
            .decode(&body)
            .map_err(|code| self.failure(code, None, None))?;
        // A successful in-flight response cannot erase a concurrent refusal.
        self.0.control.lock().unwrap().failures = 0;
        let evidence = evidence_directory
            .map(|directory| {
                evidence::RawResponse::buffered(
                    endpoint.clone(),
                    body,
                    self.0.clock.unix_seconds(),
                    directory,
                    &self.0.key,
                    &self.0.evidence_budget,
                )
            })
            .transpose()
            .map_err(|_| self.failure("evidence_storage_or_capacity", None, None))?;
        Ok(evidence::CapturedReply {
            endpoint,
            reply,
            evidence,
        })
    }
}

struct InFlight<'a>(&'a AtomicUsize);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn retry_after(value: &str, now: u64) -> Option<u64> {
    value.parse::<u64>().ok().or_else(|| {
        httpdate::parse_http_date(value)
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|time| time.as_secs().saturating_sub(now))
    })
}

/// Bound the materialized JSON depth and reject reflected credentials, including
/// JSON-escaped spellings. Known deep metadata stays RawValue. serde's iterative
/// ignored-value reader skips unknown members without constructing their trees.
fn inspect_json(body: &[u8], maximum: u16, key: &str) -> Result<(), &'static str> {
    let mut position = 0;
    let mut depth = 0u32;
    while position < body.len() {
        match body[position] {
            b'"' => {
                let start = position;
                position += 1;
                let mut escaped = false;
                while position < body.len() && body[position] != b'"' {
                    if body[position] == b'\\' {
                        escaped = true;
                        position += 1;
                    }
                    position += 1;
                }
                if position == body.len() {
                    return Err("provider_invalid_response");
                }
                let string = &body[start..=position];
                if escaped {
                    let decoded: String =
                        serde_json::from_slice(string).map_err(|_| "provider_invalid_response")?;
                    if decoded.contains(key) {
                        return Err("unsafe_provider_response");
                    }
                } else if string.windows(key.len()).any(|part| part == key.as_bytes()) {
                    return Err("unsafe_provider_response");
                }
            }
            b'[' | b'{' => {
                depth += 1;
                if depth > u32::from(maximum) {
                    return Err("provider_json_depth_limit");
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        position += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_depth_and_escaped_credentials_are_checked_without_materializing_metadata() {
        assert!(inspect_json(br#"{"unknown":[[[]]]}"#, 3, "synthetic-key").is_err());
        assert_eq!(
            inspect_json(
                br#"{"message":"synthetic-\u006bey"}"#,
                2048,
                "synthetic-key"
            ),
            Err("unsafe_provider_response")
        );
        let raw = format!(
            "[{{\"label\":\"6500973\",\"json_metadata\":{}1{}}}]",
            "[".repeat(2000),
            "]".repeat(2000)
        );
        inspect_json(raw.as_bytes(), 2048, "synthetic-key").unwrap();
        let rows: Vec<types::Metadata> = serde_json::from_str(&raw).unwrap();
        assert_eq!(rows[0].json_metadata.get().len(), 4001);
    }

    #[test]
    fn configuration_checks_precede_key_access() {
        for rate in [0.0, -1.0, f64::NAN, f64::INFINITY, 10.01] {
            let mut config = Config::new("https://example.invalid/api/v0", "/missing-key");
            config.requests_per_second = rate;
            assert_eq!(
                config.validate().unwrap_err().field,
                Some("provider.requests_per_second")
            );
        }
        for url in [
            "http://example.invalid/api/v0",
            "https://example.invalid/?key=secret",
            "https://example.invalid/#fragment",
        ] {
            assert_eq!(
                Config::new(url, "/missing-key")
                    .validate()
                    .unwrap_err()
                    .field,
                Some("provider.base_url")
            );
        }
        let mut credential_url = Url::parse("https://example.invalid").unwrap();
        credential_url.set_username("example-user").unwrap();
        credential_url
            .set_password(Some("example-password"))
            .unwrap();
        assert_eq!(
            Config::new(credential_url.as_str(), "/missing-key")
                .validate()
                .unwrap_err()
                .field,
            Some("provider.base_url")
        );
        for rate in [0.5, 2.0, 8.0, 10.0] {
            let mut config = Config::new("https://example.invalid/api/v0", "/missing-key");
            config.requests_per_second = rate;
            assert!(config.validate().is_ok());
        }
    }
}
