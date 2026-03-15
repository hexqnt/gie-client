use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER, USER_AGENT};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::GieError;

use super::query::GieQuery;
use super::serde_ext::{deserialize_optional_dataset_name, deserialize_optional_date};
use super::types::{DatasetName, GieDate, GiePage};

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

pub(crate) const DEFAULT_BROWSER_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0";

/// Настройки встроенного best-effort rate limiting для одного процесса.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimitConfig {
    pub(crate) max_requests_per_minute: NonZeroU32,
    pub(crate) cooldown_on_429: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_minute: NonZeroU32::new(60).expect("60 is non-zero"),
            cooldown_on_429: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RateLimiter {
    config: RateLimitConfig,
    state: Mutex<RateLimiterState>,
}

#[derive(Debug, Default)]
struct RateLimiterState {
    recent_requests: VecDeque<Instant>,
    blocked_until: Option<Instant>,
}

impl RateLimiter {
    pub(crate) fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RateLimiterState::default()),
        }
    }

    pub(crate) fn wait_turn_blocking(&self) {
        while let Some(delay) = self.reserve_slot() {
            std::thread::sleep(delay);
        }
    }

    pub(crate) async fn wait_turn_async(&self) {
        while let Some(delay) = self.reserve_slot() {
            tokio::time::sleep(delay).await;
        }
    }

    pub(crate) fn on_too_many_requests(&self, retry_after: Option<Duration>) {
        let cooldown = retry_after
            .unwrap_or(self.config.cooldown_on_429)
            .max(self.config.cooldown_on_429);
        let next_allowed_at = Instant::now() + cooldown;

        let mut state = self.state.lock().expect("rate limiter state poisoned");
        state.blocked_until = Some(match state.blocked_until {
            Some(current) if current > next_allowed_at => current,
            _ => next_allowed_at,
        });
    }

    fn reserve_slot(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut state = self.state.lock().expect("rate limiter state poisoned");

        while let Some(timestamp) = state.recent_requests.front() {
            if now.duration_since(*timestamp) >= RATE_LIMIT_WINDOW {
                state.recent_requests.pop_front();
            } else {
                break;
            }
        }

        if let Some(blocked_until) = state.blocked_until
            && blocked_until <= now
        {
            state.blocked_until = None;
        }

        let mut next_allowed_at = now;

        if let Some(blocked_until) = state.blocked_until
            && blocked_until > next_allowed_at
        {
            next_allowed_at = blocked_until;
        }

        if state.recent_requests.len()
            >= usize::try_from(self.config.max_requests_per_minute.get()).unwrap_or(usize::MAX)
            && let Some(oldest) = state.recent_requests.front().copied()
        {
            let window_release_at = oldest + RATE_LIMIT_WINDOW;
            if window_release_at > next_allowed_at {
                next_allowed_at = window_release_at;
            }
        }

        if next_allowed_at > now {
            return Some(next_allowed_at.saturating_duration_since(now));
        }

        state.recent_requests.push_back(now);
        None
    }
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestContext<'a> {
    pub(crate) api_key: Option<&'a str>,
    pub(crate) user_agent: Option<&'a str>,
    pub(crate) debug_requests: bool,
    pub(crate) rate_limiter: Option<&'a RateLimiter>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GieEnvelope<T> {
    pub last_page: u32,
    pub total: u32,
    #[serde(default, deserialize_with = "deserialize_optional_dataset_name")]
    pub dataset: Option<DatasetName>,
    #[serde(default, deserialize_with = "deserialize_optional_date")]
    pub gas_day: Option<GieDate>,
    pub error: Option<String>,
    pub message: Option<String>,
    pub data: Vec<T>,
}

impl<T> From<GieEnvelope<T>> for GiePage<T> {
    fn from(value: GieEnvelope<T>) -> Self {
        Self {
            last_page: value.last_page,
            total: value.total,
            dataset: value.dataset,
            gas_day: value.gas_day,
            data: value.data,
        }
    }
}

pub(crate) fn fetch_page<T>(
    client: &reqwest::blocking::Client,
    url: &str,
    context: RequestContext<'_>,
    query: &GieQuery,
    page_override: Option<NonZeroU32>,
) -> Result<GiePage<T>, GieError>
where
    T: DeserializeOwned,
{
    let query_params = query.as_params_with_page(page_override);
    if context.debug_requests {
        log_debug_request(
            url,
            context.api_key,
            context.user_agent,
            query,
            page_override,
        );
    }

    if let Some(rate_limiter) = context.rate_limiter {
        rate_limiter.wait_turn_blocking();
    }

    let mut request = client.get(url).query(&query_params);
    if let Some(user_agent) = normalized_user_agent(context.user_agent) {
        request = request.header(USER_AGENT, user_agent);
    }
    if let Some(api_key) = normalized_api_key(context.api_key) {
        request = request.header("x-key", api_key);
    }

    let response = request.send()?;
    if response.status() == StatusCode::TOO_MANY_REQUESTS
        && let Some(rate_limiter) = context.rate_limiter
    {
        rate_limiter.on_too_many_requests(parse_retry_after(response.headers()));
    }

    decode_page_response(response)
}

pub(crate) fn build_blocking_client_with_proxy(
    proxy_url: &str,
) -> Result<reqwest::blocking::Client, GieError> {
    reqwest::blocking::Client::builder()
        .proxy(reqwest::Proxy::all(proxy_url)?)
        .build()
        .map_err(Into::into)
}

pub(crate) fn build_async_client_with_proxy(proxy_url: &str) -> Result<reqwest::Client, GieError> {
    reqwest::Client::builder()
        .proxy(reqwest::Proxy::all(proxy_url)?)
        .build()
        .map_err(Into::into)
}

pub(crate) async fn fetch_page_async<T>(
    client: &reqwest::Client,
    url: &str,
    context: RequestContext<'_>,
    query: &GieQuery,
    page_override: Option<NonZeroU32>,
) -> Result<GiePage<T>, GieError>
where
    T: DeserializeOwned,
{
    let query_params = query.as_params_with_page(page_override);
    if context.debug_requests {
        log_debug_request(
            url,
            context.api_key,
            context.user_agent,
            query,
            page_override,
        );
    }

    if let Some(rate_limiter) = context.rate_limiter {
        rate_limiter.wait_turn_async().await;
    }

    let mut request = client.get(url).query(&query_params);
    if let Some(user_agent) = normalized_user_agent(context.user_agent) {
        request = request.header(USER_AGENT, user_agent);
    }
    if let Some(api_key) = normalized_api_key(context.api_key) {
        request = request.header("x-key", api_key);
    }

    let response = request.send().await?;
    if response.status() == StatusCode::TOO_MANY_REQUESTS
        && let Some(rate_limiter) = context.rate_limiter
    {
        rate_limiter.on_too_many_requests(parse_retry_after(response.headers()));
    }

    decode_page_response_async(response).await
}

fn decode_page_response<T>(response: reqwest::blocking::Response) -> Result<GiePage<T>, GieError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(GieError::HttpStatus {
            status,
            body: response.text()?,
        });
    }

    let body = response.bytes()?;
    let envelope: GieEnvelope<T> = serde_json::from_slice(&body)?;
    decode_envelope(envelope)
}

async fn decode_page_response_async<T>(response: reqwest::Response) -> Result<GiePage<T>, GieError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(GieError::HttpStatus {
            status,
            body: response.text().await?,
        });
    }

    let body = response.bytes().await?;
    let envelope: GieEnvelope<T> = serde_json::from_slice(&body)?;
    decode_envelope(envelope)
}

fn decode_envelope<T>(envelope: GieEnvelope<T>) -> Result<GiePage<T>, GieError> {
    if let Some(error) = envelope.error {
        return Err(GieError::Api {
            error,
            message: envelope
                .message
                .unwrap_or_else(|| "Unknown API error".to_string()),
        });
    }

    Ok(envelope.into())
}

pub(crate) fn fetch_all_pages<T, F>(
    start_page: NonZeroU32,
    mut fetch_page: F,
) -> Result<Vec<T>, GieError>
where
    F: FnMut(NonZeroU32) -> Result<GiePage<T>, GieError>,
{
    let mut next_page = start_page;
    let first_page = fetch_page(next_page)?;
    let mut all_rows = first_page.data;
    let mut last_page = first_page.last_page;

    if let Some(extra_capacity) = usize::try_from(first_page.total)
        .ok()
        .and_then(|total| total.checked_sub(all_rows.len()))
    {
        all_rows.reserve(extra_capacity);
    }

    while last_page != 0 && next_page.get() < last_page {
        let raw_next_page = next_page.get().checked_add(1).ok_or_else(|| {
            GieError::InvalidPageInput("page number overflow while fetching pages".to_string())
        })?;
        next_page = NonZeroU32::new(raw_next_page).ok_or_else(|| {
            GieError::InvalidPageInput("page number overflow while fetching pages".to_string())
        })?;

        let response = fetch_page(next_page)?;
        last_page = response.last_page;
        all_rows.extend(response.data);
    }

    Ok(all_rows)
}

pub(crate) async fn fetch_all_pages_async<T, F, Fut>(
    start_page: NonZeroU32,
    mut fetch_page: F,
) -> Result<Vec<T>, GieError>
where
    F: FnMut(NonZeroU32) -> Fut,
    Fut: Future<Output = Result<GiePage<T>, GieError>>,
{
    let mut next_page = start_page;
    let first_page = fetch_page(next_page).await?;
    let mut all_rows = first_page.data;
    let mut last_page = first_page.last_page;

    if let Some(extra_capacity) = usize::try_from(first_page.total)
        .ok()
        .and_then(|total| total.checked_sub(all_rows.len()))
    {
        all_rows.reserve(extra_capacity);
    }

    while last_page != 0 && next_page.get() < last_page {
        let raw_next_page = next_page.get().checked_add(1).ok_or_else(|| {
            GieError::InvalidPageInput("page number overflow while fetching pages".to_string())
        })?;
        next_page = NonZeroU32::new(raw_next_page).ok_or_else(|| {
            GieError::InvalidPageInput("page number overflow while fetching pages".to_string())
        })?;

        let response = fetch_page(next_page).await?;
        last_page = response.last_page;
        all_rows.extend(response.data);
    }

    Ok(all_rows)
}

fn normalized_api_key(api_key: Option<&str>) -> Option<&str> {
    normalized_header_value(api_key)
}

fn normalized_user_agent(user_agent: Option<&str>) -> Option<&str> {
    normalized_header_value(user_agent)
}

fn normalized_header_value(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn log_debug_request(
    url: &str,
    api_key: Option<&str>,
    user_agent: Option<&str>,
    query: &GieQuery,
    page_override: Option<NonZeroU32>,
) {
    let full_url = match reqwest::Url::parse(url) {
        Ok(mut parsed_url) => {
            {
                let mut url_query = parsed_url.query_pairs_mut();
                for (key, value) in query.to_debug_pairs(page_override) {
                    url_query.append_pair(key, &value);
                }
            }
            parsed_url.to_string()
        }
        Err(_) => url.to_string(),
    };

    let x_key_state = if normalized_api_key(api_key).is_some() {
        "set"
    } else {
        "none"
    };
    let ua_state = if normalized_user_agent(user_agent).is_some() {
        "set"
    } else {
        "none"
    };
    eprintln!("GIE debug request: GET {full_url} (x-key: {x_key_state}, user-agent: {ua_state})");
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;

    use super::*;

    #[test]
    fn envelope_deserializes_typed_dataset_name() {
        let envelope: GieEnvelope<serde_json::Value> = serde_json::from_str(
            r#"{
                "last_page": 1,
                "total": 1,
                "dataset": " LNG ",
                "gas_day": "2026-03-10",
                "data": []
            }"#,
        )
        .unwrap();

        assert_eq!(envelope.dataset, Some(DatasetName::Lng));
    }

    #[test]
    fn retry_after_parser_accepts_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("17"));

        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(17)));
    }

    #[test]
    fn rate_limiter_respects_429_cooldown() {
        let limiter = RateLimiter::new(RateLimitConfig {
            max_requests_per_minute: NonZeroU32::new(60).unwrap(),
            cooldown_on_429: Duration::from_millis(10),
        });

        limiter.on_too_many_requests(None);
        let started_at = Instant::now();
        limiter.wait_turn_blocking();

        assert!(started_at.elapsed() >= Duration::from_millis(8));
    }

    #[test]
    fn empty_or_missing_api_key_is_treated_as_absent() {
        assert_eq!(normalized_api_key(None), None);
        assert_eq!(normalized_api_key(Some("")), None);
        assert_eq!(normalized_api_key(Some("   ")), None);
        assert_eq!(normalized_api_key(Some(" key ")), Some("key"));
    }

    #[test]
    fn empty_or_missing_user_agent_is_treated_as_absent() {
        assert_eq!(normalized_user_agent(None), None);
        assert_eq!(normalized_user_agent(Some("")), None);
        assert_eq!(normalized_user_agent(Some("   ")), None);
        assert_eq!(normalized_user_agent(Some(" browser ")), Some("browser"));
    }

    #[test]
    fn proxy_builders_accept_valid_proxy_url() {
        assert!(build_blocking_client_with_proxy("http://127.0.0.1:8080").is_ok());
        assert!(build_async_client_with_proxy("http://127.0.0.1:8080").is_ok());
    }

    #[test]
    fn proxy_builders_reject_invalid_proxy_url() {
        assert!(build_blocking_client_with_proxy("http://[::1").is_err());
        assert!(build_async_client_with_proxy("http://[::1").is_err());
    }
}
