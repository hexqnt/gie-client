use std::env;
use std::time::Duration;

use gie_client::GiePage;
use gie_client::GieQuery;
use gie_client::agsi::AgsiClient;
use gie_client::alsi::AlsiClient;
use serde_json::Value;

const AGSI_API_URL: &str = "https://agsi.gie.eu/api";
const ALSI_API_URL: &str = "https://alsi.gie.eu/api";
const QUERY_SIZE: &str = "1";

#[test]
#[ignore]
fn agsi_public_contract() {
    if !live_tests_enabled() {
        eprintln!("Skipping live test: set GIE_LIVE_TESTS=1 to enable");
        return;
    }

    let query = GieQuery::new()
        .country("DE")
        .try_size(1)
        .expect("failed to build AGSI live query");
    let page = AgsiClient::without_api_key()
        .fetch_page(&query)
        .expect("AGSI public typed request failed");

    assert_typed_contract(&page);

    let raw = fetch_raw_page(AGSI_API_URL, "DE", None);
    assert_envelope_core_shape(&raw);
}

#[test]
#[ignore]
fn alsi_public_contract() {
    if !live_tests_enabled() {
        eprintln!("Skipping live test: set GIE_LIVE_TESTS=1 to enable");
        return;
    }

    let query = GieQuery::new()
        .country("FR")
        .try_size(1)
        .expect("failed to build ALSI live query");
    let page = AlsiClient::without_api_key()
        .fetch_page(&query)
        .expect("ALSI public typed request failed");

    assert_typed_contract(&page);

    let raw = fetch_raw_page(ALSI_API_URL, "FR", None);
    assert_envelope_core_shape(&raw);
}

#[test]
#[ignore]
fn agsi_auth_contract_optional() {
    if !live_tests_enabled() {
        eprintln!("Skipping live test: set GIE_LIVE_TESTS=1 to enable");
        return;
    }

    let Some(api_key) = read_api_key_or_skip() else {
        return;
    };

    let query = GieQuery::new()
        .country("DE")
        .try_size(1)
        .expect("failed to build AGSI auth query");
    let page = AgsiClient::new(api_key.as_str())
        .fetch_page(&query)
        .expect("AGSI auth typed request failed");

    assert_typed_contract(&page);

    let raw = fetch_raw_page(AGSI_API_URL, "DE", Some(api_key.as_str()));
    assert_envelope_core_shape(&raw);
}

#[test]
#[ignore]
fn alsi_auth_contract_optional() {
    if !live_tests_enabled() {
        eprintln!("Skipping live test: set GIE_LIVE_TESTS=1 to enable");
        return;
    }

    let Some(api_key) = read_api_key_or_skip() else {
        return;
    };

    let query = GieQuery::new()
        .country("FR")
        .try_size(1)
        .expect("failed to build ALSI auth query");
    let page = AlsiClient::new(api_key.as_str())
        .fetch_page(&query)
        .expect("ALSI auth typed request failed");

    assert_typed_contract(&page);

    let raw = fetch_raw_page(ALSI_API_URL, "FR", Some(api_key.as_str()));
    assert_envelope_core_shape(&raw);
}

fn live_tests_enabled() -> bool {
    normalized_env_var("GIE_LIVE_TESTS").is_some_and(|value| value == "1")
}

fn read_api_key_or_skip() -> Option<String> {
    let Some(api_key) = normalized_env_var("GIE_API_KEY") else {
        eprintln!("Skipping auth live test: GIE_API_KEY is not set");
        return None;
    };

    Some(api_key)
}

fn normalized_env_var(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn build_http_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("failed to build reqwest client for live tests")
}

fn build_query_params(country: &str) -> [(&'static str, &str); 2] {
    [("country", country), ("size", QUERY_SIZE)]
}

fn fetch_raw_page(url: &str, country: &str, api_key: Option<&str>) -> Value {
    let client = build_http_client();
    let mut request = client.get(url).query(&build_query_params(country));

    if let Some(api_key) = api_key {
        request = request.header("x-key", api_key);
    }

    let response = request.send().expect("failed to send raw contract request");
    let status = response.status();
    let body = response
        .text()
        .expect("failed to read raw contract response body");

    assert!(
        status.is_success(),
        "expected successful HTTP status from {url}, got {status}: {body}"
    );

    serde_json::from_str(&body).expect("failed to decode raw contract JSON response")
}

fn assert_typed_contract<T>(page: &GiePage<T>) {
    assert!(
        page.last_page >= 1,
        "expected last_page >= 1, got {}",
        page.last_page
    );

    let total = usize::try_from(page.total).expect("total did not fit in usize");
    assert!(
        total >= page.data.len(),
        "expected total >= data.len(), got total={} len={}",
        page.total,
        page.data.len()
    );
    assert!(!page.data.is_empty(), "expected non-empty data payload");
}

fn assert_envelope_core_shape(raw: &Value) {
    let envelope = raw
        .as_object()
        .expect("expected top-level JSON object envelope");

    for key in ["last_page", "total", "data"] {
        assert!(
            envelope.contains_key(key),
            "expected envelope to contain key {key}"
        );
    }

    let data = envelope
        .get("data")
        .and_then(Value::as_array)
        .expect("expected envelope.data to be an array");

    if let Some(first_record) = data.first()
        && let Some(first_object) = first_record.as_object()
    {
        for key in ["name", "code", "type", "gasDayStart"] {
            assert!(
                first_object.contains_key(key),
                "expected first record to contain key {key}"
            );
        }
    }
}
