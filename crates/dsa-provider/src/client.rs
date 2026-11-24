//! Paced, kept-alive HTTPS requests. The client never retries by itself. A
//! shared gate holds only refusals that apply to every request (401, 403,
//! 402, 418, 429) and consecutive transport or 5xx failures; what one
//! endpoint answers touches no shared state.

use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use chrono::NaiveDate;
use reqwest::{Url, header::HeaderValue};
use serde::Serialize;
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore},
    time::Instant,
};

use crate::{
    Error, Kind,
    counts::Counts,
    outputs,
    types::{Hash32, TransactionOutputs},
};

/// A `/txs/{hash}/utxos` answer is kept whole up to this many bytes.
pub const OUTPUT_ANSWER_KEPT: usize = 8 * 1024 * 1024;

/// Seconds after UTC midnight at which a spent allowance is tried again.
const QUOTA_RESUME: u64 = 120;
/// A refusal of the allowance this soon after midnight is retried after
/// [`QUOTA_EARLY_RETRY`]: the provider's day may start a little later.
const QUOTA_EARLY_WINDOW: u64 = 600;
const QUOTA_EARLY_RETRY: u64 = 300;

/// UTC time, injectable for tests.
pub trait Clock: Send + Sync {
    fn unix_seconds(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// Answer size limits in bytes, per family of endpoints.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Headers, genesis, epochs and pools.
    pub small: usize,
    /// One page of a block's transaction CBOR.
    pub block: usize,
    pub datum: usize,
    pub metadata: usize,
    /// A streamed `/utxos` answer, its input rendering included.
    pub outputs: usize,
    /// The outputs kept from a `/utxos` answer.
    pub output_values: usize,
    /// One page of an address, asset or transaction list.
    pub lists: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            small: 2 * 1024 * 1024,
            block: 1024 * 1024,
            datum: 1024 * 1024,
            metadata: 8 * 1024 * 1024,
            outputs: 128 * 1024 * 1024,
            output_values: 8 * 1024 * 1024,
            lists: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    Small,
    Block,
    Datum,
    Metadata,
    List,
    /// A whole `/txs/{hash}/utxos` answer, its inputs included.
    Outputs,
}

#[derive(Clone)]
pub struct Config {
    pub base_url: String,
    pub key_file: PathBuf,
    pub root_certificate: Option<PathBuf>,
    pub requests_per_second: f64,
    pub max_in_flight: usize,
    /// The longest wait for the next bytes of an answer.
    pub idle_timeout: Duration,
    /// The total time of one request, except `/utxos`.
    pub request_timeout: Duration,
    /// The total time of one `/utxos` request.
    pub outputs_timeout: Duration,
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
            idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            outputs_timeout: Duration::from_secs(600),
            limits: Limits::default(),
        }
    }

    /// Check every setting before the key file is read.
    pub fn validate(&self) -> Result<(Url, Duration), Error> {
        let limits = &self.limits;
        for (field, value) in [
            ("provider.limits.small", limits.small),
            ("provider.limits.block", limits.block),
            ("provider.limits.datum", limits.datum),
            ("provider.limits.metadata", limits.metadata),
            ("provider.limits.outputs", limits.outputs),
            ("provider.limits.output_values", limits.output_values),
            ("provider.limits.lists", limits.lists),
        ] {
            if value == 0 || value > isize::MAX as usize / 2 {
                return Err(Error::configuration(field));
            }
        }
        if !self.requests_per_second.is_finite()
            || self.requests_per_second <= 0.0
            || self.requests_per_second > 10.0
        {
            return Err(Error::configuration("provider.requests_per_second"));
        }
        let interval = Duration::try_from_secs_f64(1.0 / self.requests_per_second)
            .map_err(|_| Error::configuration("provider.requests_per_second"))?;
        if self.max_in_flight == 0 || self.max_in_flight > Semaphore::MAX_PERMITS {
            return Err(Error::configuration("provider.max_in_flight"));
        }
        for (field, value) in [
            ("provider.idle_timeout", self.idle_timeout),
            ("provider.request_timeout", self.request_timeout),
            ("provider.outputs_timeout", self.outputs_timeout),
        ] {
            if value.is_zero() || value > Duration::from_secs(86_400) {
                return Err(Error::configuration(field));
            }
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

/// The provider's last refusal of every request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Refusal {
    pub http_status: u16,
    pub code: &'static str,
    pub at: u64,
    pub until: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub requests_per_second: f64,
    pub requests_in_flight: usize,
    pub last_refusal: Option<Refusal>,
    /// Every request waits for this error to expire.
    pub blocked: Option<Error>,
}

#[derive(Default)]
struct Gate {
    failures: u32,
    blocked: Option<Error>,
    last_refusal: Option<Refusal>,
}

struct Inner {
    http: reqwest::Client,
    base_url: Url,
    header: HeaderValue,
    config: Config,
    interval: Duration,
    next_send: AsyncMutex<Instant>,
    permits: Semaphore,
    in_flight: AtomicUsize,
    gate: Mutex<Gate>,
    counts: Mutex<Counts>,
    clock: Arc<dyn Clock>,
}

/// A Blockfrost client. Clones share pacing, the in-flight limit, the gate
/// and the call counts.
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

fn retry_after(value: &str, now: u64) -> Option<u64> {
    value.parse::<u64>().ok().or_else(|| {
        httpdate::parse_http_date(value)
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|time| time.as_secs().saturating_sub(now))
    })
}

/// When to try again after the provider says the allowance is spent.
pub(crate) fn quota_until(now: u64) -> u64 {
    let into_day = now % 86_400;
    if into_day < QUOTA_EARLY_WINDOW {
        now + QUOTA_EARLY_RETRY
    } else {
        now - into_day + 86_400 + QUOTA_RESUME
    }
}

struct InFlight<'a>(&'a AtomicUsize);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Provider {
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    pub fn with_clock(config: Config, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        let (base_url, interval) = config.validate()?;
        let bytes = read_bounded(&config.key_file, 512, "provider.key_file")?;
        let key = std::str::from_utf8(&bytes)
            .map_err(|_| Error::configuration("provider.key_file"))?
            .trim_end_matches(['\n', '\r']);
        if key.is_empty() {
            return Err(Error::configuration("provider.key_file"));
        }
        if base_url.as_str().contains(key) {
            return Err(Error::configuration("provider.base_url"));
        }
        let mut header =
            HeaderValue::from_str(key).map_err(|_| Error::configuration("provider.key_file"))?;
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
            .connect_timeout(config.idle_timeout)
            .read_timeout(config.idle_timeout)
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
            http,
            base_url,
            header,
            interval,
            next_send: AsyncMutex::new(Instant::now()),
            permits: Semaphore::new(config.max_in_flight),
            in_flight: AtomicUsize::new(0),
            gate: Mutex::new(Gate::default()),
            counts: Mutex::new(Counts::default()),
            clock,
            config,
        })))
    }

    /// Requests sent per UTC day, oldest first, at most eight days.
    pub fn counts(&self) -> Vec<(NaiveDate, i64)> {
        self.0.counts.lock().unwrap().get()
    }

    /// Restore persisted counts; for each day the larger count stays.
    pub fn seed_counts(&self, counts: &[(NaiveDate, i64)]) {
        self.0.counts.lock().unwrap().seed(counts);
    }

    pub fn status(&self) -> Status {
        let now = self.now();
        let gate = self.0.gate.lock().unwrap();
        Status {
            requests_per_second: self.0.config.requests_per_second,
            requests_in_flight: self.0.in_flight.load(Ordering::SeqCst),
            last_refusal: gate.last_refusal.clone(),
            blocked: gate
                .blocked
                .clone()
                .filter(|error| error.until().is_none_or(|until| until > now)),
        }
    }

    pub(crate) fn now(&self) -> u64 {
        self.0.clock.unix_seconds()
    }

    /// An error of this request only: nothing shared changes.
    pub(crate) fn invalid(&self, code: &'static str) -> Error {
        Error::retry(code, self.now())
    }

    fn check_gate(&self) -> Result<(), Error> {
        let now = self.now();
        let mut gate = self.0.gate.lock().unwrap();
        if let Some(error) = &gate.blocked {
            if error.until().is_none_or(|until| until > now) {
                return Err(Error {
                    http_status: None,
                    ..error.clone()
                });
            }
            gate.blocked = None;
        }
        Ok(())
    }

    fn block(&self, error: &Error) {
        let mut gate = self.0.gate.lock().unwrap();
        let replace = gate.blocked.as_ref().is_none_or(|old| match old.until() {
            None => false,
            Some(old) => error.until().is_none_or(|until| until >= old),
        });
        if replace {
            gate.blocked = Some(error.clone());
        }
    }

    /// A refusal of every request by the provider.
    fn refused(&self, status: u16, retry_after: Option<u64>) -> Error {
        let now = self.now();
        let (code, kind) = match status {
            401 | 403 => (
                "provider_key_rejected",
                Kind::Fatal {
                    field: "provider.key_file",
                },
            ),
            402 => (
                "quota_exhausted",
                Kind::Wait {
                    until: quota_until(now),
                },
            ),
            418 => (
                "provider_banned",
                Kind::Wait {
                    until: now + retry_after.unwrap_or(3600).clamp(3600, 86_400),
                },
            ),
            _ => (
                "provider_rate_limited",
                Kind::Wait {
                    until: now + retry_after.unwrap_or(60).clamp(1, 86_400),
                },
            ),
        };
        let error = Error {
            code,
            kind,
            http_status: Some(status),
        };
        if let Kind::Wait { until } = kind {
            self.0.gate.lock().unwrap().last_refusal = Some(Refusal {
                http_status: status,
                code,
                at: now,
                until,
            });
        }
        self.block(&error);
        error
    }

    /// A transport failure or a 5xx answer: every request backs off.
    fn unavailable(&self, code: &'static str, status: Option<u16>) -> Error {
        let now = self.now();
        let failures = {
            let mut gate = self.0.gate.lock().unwrap();
            gate.failures = gate.failures.saturating_add(1);
            gate.failures
        };
        let delay = match failures {
            1 => 1,
            2 => 2,
            3 => 4,
            4 => 8,
            5 => 16,
            _ => 30,
        };
        let error = Error {
            code,
            kind: Kind::Retry { until: now + delay },
            http_status: status,
        };
        self.block(&error);
        error
    }

    fn transport(&self, error: &reqwest::Error) -> Error {
        self.unavailable(
            if error.is_timeout() {
                "provider_timeout"
            } else {
                "provider_unavailable"
            },
            None,
        )
    }

    fn succeeded(&self) {
        self.0.gate.lock().unwrap().failures = 0;
    }

    /// Wait for a permit and the pace, count the attempt and send.
    async fn send(&self, path: &str, timeout: Duration) -> Result<Sent<'_>, Error> {
        self.check_gate()?;
        let permit = self
            .0
            .permits
            .acquire()
            .await
            .expect("the semaphore is never closed");
        let mut next = self.0.next_send.lock().await;
        tokio::time::sleep_until(*next).await;
        self.check_gate()?;
        self.0.counts.lock().unwrap().count(self.now());
        *next = Instant::now() + self.0.interval;
        drop(next);
        let url = self
            .0
            .base_url
            .join(path.trim_start_matches('/'))
            .expect("typed relative provider path");
        self.0.in_flight.fetch_add(1, Ordering::SeqCst);
        let flight = InFlight(&self.0.in_flight);
        let response = self
            .0
            .http
            .get(url)
            .header("project_id", self.0.header.clone())
            .header("accept-encoding", "identity")
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| self.transport(&error))?;
        Ok(Sent {
            response,
            _permit: permit,
            _flight: flight,
        })
    }

    /// Map a non-200 status. `Ok(())` for a 404, which the endpoint reads.
    fn status_error(&self, response: &reqwest::Response) -> Result<(), Error> {
        let status = response.status().as_u16();
        let after = || {
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| retry_after(value, self.now()))
        };
        match status {
            404 => Ok(()),
            401 | 402 | 403 | 418 | 429 => Err(self.refused(status, after())),
            500..=599 => Err(self.unavailable("provider_unavailable", Some(status))),
            _ => Err(Error {
                http_status: Some(status),
                ..self.invalid("provider_request_rejected")
            }),
        }
    }

    /// GET a buffered answer; `None` for a 404.
    pub(crate) async fn get(&self, path: &str, family: Family) -> Result<Option<Bytes>, Error> {
        let limit = match family {
            Family::Small => self.0.config.limits.small,
            Family::Block => self.0.config.limits.block,
            Family::Datum => self.0.config.limits.datum,
            Family::Metadata => self.0.config.limits.metadata,
            Family::List => self.0.config.limits.lists,
            Family::Outputs => self.0.config.limits.outputs,
        };
        let timeout = if family == Family::Outputs {
            self.0.config.outputs_timeout
        } else {
            self.0.config.request_timeout
        };
        let mut sent = self.send(path, timeout).await?;
        let response = &mut sent.response;
        if response.status().as_u16() != 200 {
            self.status_error(response)?;
            self.succeeded();
            return Ok(None);
        }
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(self.invalid("provider_response_size_limit"));
        }
        let mut body = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| self.read(&error))? {
            if body.len() + chunk.len() > limit {
                return Err(self.invalid("provider_response_size_limit"));
            }
            body.extend_from_slice(&chunk);
        }
        self.succeeded();
        Ok(Some(body.freeze()))
    }

    /// A failure while reading an answer that has started.
    fn read(&self, error: &reqwest::Error) -> Error {
        if error.is_timeout() {
            self.invalid("provider_timeout")
        } else {
            self.unavailable("provider_unavailable", None)
        }
    }

    /// GET `/txs/{hash}/utxos`, streaming past the input rendering.
    pub(crate) async fn get_outputs(
        &self,
        hash: &Hash32,
    ) -> Result<Option<TransactionOutputs>, Error> {
        let limits = &self.0.config.limits;
        let path = format!("/txs/{hash}/utxos");
        let mut sent = self.send(&path, self.0.config.outputs_timeout).await?;
        let response = &mut sent.response;
        if response.status().as_u16() != 200 {
            self.status_error(response)?;
            self.succeeded();
            return Ok(None);
        }
        if response
            .content_length()
            .is_some_and(|length| length > limits.outputs as u64)
        {
            return Err(self.invalid("provider_response_size_limit"));
        }
        let (sender, decoder) = outputs::start(limits.output_values);
        let mut kept = Some(BytesMut::new());
        let mut received = 0usize;
        let mut failure = None;
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    received += chunk.len();
                    if received > limits.outputs {
                        failure = Some(self.invalid("provider_response_size_limit"));
                        break;
                    }
                    kept = kept.filter(|kept| kept.len() + chunk.len() <= OUTPUT_ANSWER_KEPT);
                    if let Some(kept) = kept.as_mut() {
                        kept.extend_from_slice(&chunk);
                    }
                    if sender.send(chunk).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    failure = Some(self.read(&error));
                    break;
                }
            }
        }
        drop(sender);
        let decoded = decoder
            .await
            .map_err(|_| self.invalid("provider_invalid_response"))?;
        if let Some(error) = failure {
            return Err(error);
        }
        let mut outputs = decoded.map_err(|code| self.invalid(code))?;
        outputs.validate(hash).map_err(|code| self.invalid(code))?;
        outputs.body = kept.map(BytesMut::freeze);
        self.succeeded();
        Ok(Some(outputs))
    }
}

struct Sent<'a> {
    response: reqwest::Response,
    _permit: tokio::sync::SemaphorePermit<'a>,
    _flight: InFlight<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_is_checked_before_the_key_file() {
        for rate in [0.0, -1.0, f64::NAN, f64::INFINITY, 10.01] {
            let mut config = Config::new("https://example.invalid/api/v0", "/missing-key");
            config.requests_per_second = rate;
            assert_eq!(
                config.validate().unwrap_err().kind,
                Kind::Fatal {
                    field: "provider.requests_per_second"
                }
            );
        }
        let mut credential = Url::parse("https://example.invalid").unwrap();
        credential.set_username("example-user").unwrap();
        credential.set_password(Some("example-password")).unwrap();
        for url in [
            "http://example.invalid/api/v0",
            "https://example.invalid/?key=secret",
            "https://example.invalid/#fragment",
            credential.as_str(),
        ] {
            assert_eq!(
                Config::new(url, "/missing-key")
                    .validate()
                    .unwrap_err()
                    .kind,
                Kind::Fatal {
                    field: "provider.base_url"
                }
            );
        }
        let mut config = Config::new("https://example.invalid/api/v0", "/missing-key");
        config.idle_timeout = Duration::ZERO;
        assert_eq!(
            config.validate().unwrap_err().kind,
            Kind::Fatal {
                field: "provider.idle_timeout"
            }
        );
        for rate in [0.5, 2.0, 8.0, 10.0] {
            let mut config = Config::new("https://example.invalid/api/v0", "/missing-key");
            config.requests_per_second = rate;
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn a_spent_allowance_resumes_after_midnight_and_early_refusals_soon() {
        let midnight = 1_795_046_400;
        assert_eq!(quota_until(midnight - 1), midnight + QUOTA_RESUME);
        assert_eq!(quota_until(midnight - 43_200), midnight + QUOTA_RESUME);
        assert_eq!(quota_until(midnight + 1), midnight + 1 + QUOTA_EARLY_RETRY);
        assert_eq!(
            quota_until(midnight + 599),
            midnight + 599 + QUOTA_EARLY_RETRY
        );
        assert_eq!(
            quota_until(midnight + 600),
            midnight + 86_400 + QUOTA_RESUME
        );
    }
}
