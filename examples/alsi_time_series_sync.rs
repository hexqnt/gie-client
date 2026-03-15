use std::env;

use gie_client::GieQuery;
use gie_client::alsi::AlsiClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("GIE_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let user_agent = env::var("GIE_USER_AGENT")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let proxy_url = env::var("GIE_PROXY_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let client = match (api_key, proxy_url) {
        (Some(api_key), Some(proxy_url)) => AlsiClient::with_proxy(api_key, proxy_url)?,
        (Some(api_key), None) => AlsiClient::new(api_key),
        (None, Some(proxy_url)) => AlsiClient::with_proxy_without_api_key(proxy_url)?,
        (None, None) => AlsiClient::without_api_key(),
    };
    let client = if let Some(user_agent) = user_agent {
        client.with_user_agent(user_agent)
    } else {
        client
    };
    let client = client.with_debug_requests(true);
    let query = GieQuery::new()
        .country("FR")
        .try_range("2026-03-01", "2026-03-10")?
        .try_size(200)?;

    let series = client.fetch_time_series(&query)?;
    println!("time series sets: {}", series.len());

    if let Some(first) = series.first() {
        println!(
            "first series: code={:?}, name={:?}, points={}",
            first.key.code,
            first.key.name,
            first.points.len()
        );
    }

    Ok(())
}
