//! Where batches go: the GCS staging bucket, or a fake.
//!
//! # Why a hand-rolled client and not a GCS SDK
//!
//! The emulator and real GCS differ in exactly two places — the endpoint and
//! the `Authorization` header — and `pulse-infra/docs/divergences.md` is blunt
//! that everything about auth is unverified until the dev deployment. Keeping
//! both as explicit seams ([`TokenSource`], [`GcsSink::endpoint`]) means that
//! work is a change to one small file rather than a fight with an SDK's
//! credential discovery.
//!
//! # Error classification is the part that matters locally
//!
//! The divergences page names the failure it expects us to get wrong: *"retry
//! logic that treats an auth failure as retryable and hammers the API."* The
//! emulator will never produce a 401, so that path cannot be tested against it
//! — but it can be tested directly, and it is, in this module's tests.
//!
//! The rule: **401/403 never retry.** A token that is wrong now is wrong in
//! 200ms, and retrying turns one misconfiguration into a rate-limit incident.
//! 429 and 5xx retry with backoff; other 4xx are permanent.

use std::time::Duration;

use async_trait::async_trait;

use crate::batching::ObjectName;

/// Real GCS. Used when no emulator host is configured.
pub const REAL_GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// What went wrong with an upload, classified by what a caller should *do*.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    /// Transport-level failure, 429, or 5xx. Safe to retry: the object name is
    /// deterministic, so a duplicate attempt overwrites rather than doubles.
    #[error("retryable upload failure: {0}")]
    Retryable(String),

    /// 401 or 403. Never retried — see the module docs.
    #[error("authentication/authorisation failure (not retried): {0}")]
    Auth(String),

    /// Anything else: a bad request, a missing bucket, a name we cannot encode.
    #[error("permanent upload failure: {0}")]
    Permanent(String),
}

impl UploadError {
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

/// Classify an HTTP status into the action a caller should take.
///
/// Split out from the request path so it is testable without a server — which
/// is the only way to test the 401 branch at all, locally.
#[must_use]
pub fn classify_status(status: u16, body: &str) -> Option<UploadError> {
    let detail = format!("HTTP {status}: {}", first_line(body, 200));
    match status {
        200..=299 => None,
        401 | 403 => Some(UploadError::Auth(detail)),
        408 | 429 => Some(UploadError::Retryable(detail)),
        500..=599 => Some(UploadError::Retryable(detail)),
        _ => Some(UploadError::Permanent(detail)),
    }
}

/// Somewhere a Parquet batch can be written.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write `bytes` at `name`, overwriting whatever is there.
    ///
    /// Overwriting is required, not incidental: a retried batch reproduces the
    /// same name on purpose. An implementation that refused to overwrite would
    /// break the idempotency the whole design rests on.
    async fn put(&self, name: &ObjectName, bytes: Vec<u8>) -> Result<(), UploadError>;

    /// Human-readable destination, for logs.
    fn describe(&self) -> String;
}

/// Supplies the bearer token for real GCS.
///
/// Locally there is none: fake-gcs is unauthenticated.
#[async_trait]
pub trait TokenSource: Send + Sync {
    async fn token(&self) -> Result<Option<String>, UploadError>;
}

/// No credentials at all — the emulator.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAuth;

#[async_trait]
impl TokenSource for NoAuth {
    async fn token(&self) -> Result<Option<String>, UploadError> {
        Ok(None)
    }
}

/// Application Default Credentials, for the dev deployment.
///
/// Deliberately unimplemented rather than half-implemented: the emulator cannot
/// validate any of it, so code written now would be untested guesswork that
/// *looks* finished. It fails loudly at startup instead of on the first upload
/// an hour into a run.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApplicationDefaultCredentials;

#[async_trait]
impl TokenSource for ApplicationDefaultCredentials {
    async fn token(&self) -> Result<Option<String>, UploadError> {
        Err(UploadError::Auth(
            "ADC is not implemented yet — real GCS auth is scheduled for the dev \
             deployment. Set STORAGE_EMULATOR_HOST to use the local emulator."
                .to_owned(),
        ))
    }
}

/// Uploads to GCS (or an emulator) over the JSON API.
pub struct GcsSink {
    client: reqwest::Client,
    endpoint: String,
    bucket: String,
    tokens: Box<dyn TokenSource>,
}

impl GcsSink {
    /// Build a sink.
    ///
    /// `emulator_host` is `host:port` when running against the local stack;
    /// `None` means real GCS, which also means real credentials.
    ///
    /// # Errors
    ///
    /// Fails if the HTTP client cannot be constructed.
    pub fn new(bucket: impl Into<String>, emulator_host: Option<&str>) -> anyhow::Result<Self> {
        let (endpoint, tokens): (String, Box<dyn TokenSource>) = match emulator_host {
            // The emulator serves plain HTTP. Scheme is added here rather than
            // asked of the operator so STORAGE_EMULATOR_HOST keeps the
            // host:port shape the rest of the platform uses.
            Some(host) => (normalise_emulator(host), Box::new(NoAuth)),
            None => (
                REAL_GCS_ENDPOINT.to_owned(),
                Box::new(ApplicationDefaultCredentials),
            ),
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            endpoint,
            bucket: bucket.into(),
            tokens,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn upload_url(&self, name: &ObjectName) -> String {
        format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.endpoint,
            percent_encode(&self.bucket),
            percent_encode(name.as_str()),
        )
    }
}

#[async_trait]
impl Sink for GcsSink {
    async fn put(&self, name: &ObjectName, bytes: Vec<u8>) -> Result<(), UploadError> {
        let mut req = self
            .client
            .post(self.upload_url(name))
            .header("content-type", "application/octet-stream")
            .body(bytes);

        if let Some(token) = self.tokens.token().await? {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            // A transport error (connection refused, timeout) is retryable:
            // nothing was necessarily written, and if it was, the name makes
            // the retry an overwrite.
            .map_err(|e| UploadError::Retryable(e.to_string()))?;

        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        match classify_status(status, &body) {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    fn describe(&self) -> String {
        format!("{}/{}", self.endpoint, self.bucket)
    }
}

fn normalise_emulator(host: &str) -> String {
    let h = host.trim().trim_end_matches('/');
    if h.starts_with("http://") || h.starts_with("https://") {
        h.to_owned()
    } else {
        format!("http://{h}")
    }
}

/// Percent-encode a GCS object name for use in a query parameter.
///
/// Object names contain `/` and `=` (`dt=2026-09-14`), both of which must be
/// escaped here or the API reads them as path and parameter syntax.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn first_line(s: &str, max: usize) -> String {
    let line = s.lines().next().unwrap_or("").trim();
    if line.len() <= max {
        return line.to_owned();
    }
    let mut cut = max;
    while cut > 0 && !line.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &line[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> ObjectName {
        use std::num::NonZeroU64;
        let r = crate::batching::BatchRange::containing(0, NonZeroU64::new(10_000).unwrap());
        ObjectName::for_range("ingestion-events", 0, "2026-09-14", r)
    }

    #[test]
    fn auth_failures_are_never_retryable() {
        // The single most important line in this file: divergences.md warns
        // that retrying a 401 is the bug this stack cannot catch for us.
        for status in [401, 403] {
            let e = classify_status(status, "denied").expect("should be an error");
            assert!(!e.is_retryable(), "HTTP {status} must not retry");
            assert!(matches!(e, UploadError::Auth(_)));
        }
    }

    #[test]
    fn throttling_and_server_errors_retry() {
        for status in [408, 429, 500, 502, 503, 504] {
            let e = classify_status(status, "slow down").expect("should be an error");
            assert!(e.is_retryable(), "HTTP {status} should retry");
        }
    }

    #[test]
    fn client_errors_are_permanent() {
        for status in [400, 404, 409, 412] {
            let e = classify_status(status, "nope").expect("should be an error");
            assert!(!e.is_retryable());
            assert!(matches!(e, UploadError::Permanent(_)));
        }
    }

    #[test]
    fn success_is_not_an_error() {
        assert!(classify_status(200, "").is_none());
        assert!(classify_status(204, "").is_none());
    }

    #[test]
    fn object_name_is_percent_encoded_into_the_query() {
        let sink = GcsSink::new("pulse-staging-local", Some("localhost:4443")).unwrap();
        let url = sink.upload_url(&name());
        // Slashes and the `dt=` separator must not leak into URL syntax.
        assert!(url.contains("name=ingestion-events%2Fdt%3D2026-09-14%2F0-0-9999.parquet"));
        assert!(url.starts_with("http://localhost:4443/upload/storage/v1/b/"));
    }

    #[test]
    fn emulator_host_gets_a_scheme_but_a_url_is_left_alone() {
        assert_eq!(normalise_emulator("fake-gcs:4443"), "http://fake-gcs:4443");
        assert_eq!(
            normalise_emulator("http://localhost:4443/"),
            "http://localhost:4443"
        );
    }

    #[test]
    fn no_emulator_means_real_gcs_and_real_credentials() {
        let sink = GcsSink::new("b", None).unwrap();
        assert_eq!(sink.endpoint(), REAL_GCS_ENDPOINT);
    }

    #[tokio::test]
    async fn adc_fails_loudly_rather_than_pretending() {
        // Better a clear error than an upload that silently 401s in a retry
        // loop at 3am in the dev environment.
        let err = ApplicationDefaultCredentials.token().await.unwrap_err();
        assert!(matches!(err, UploadError::Auth(_)));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn emulator_needs_no_token() {
        assert!(NoAuth.token().await.unwrap().is_none());
    }
}
