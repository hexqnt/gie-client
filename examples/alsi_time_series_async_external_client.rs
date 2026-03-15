use std::env;
use std::time::Duration;

use gie_client::GieQuery;
use gie_client::alsi::AlsiAsyncClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("GIE_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let user_agent = env::var("GIE_USER_AGENT")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let proxy_url = env::var("GIE_PROXY_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let mut http_builder = reqwest::Client::builder().timeout(Duration::from_secs(20));
    if let Some(proxy_url) = proxy_url.as_deref() {
        http_builder = http_builder.proxy(reqwest::Proxy::all(proxy_url)?);
    }
    let http = http_builder.build()?;

    let client = match api_key {
        Some(api_key) => AlsiAsyncClient::with_http_client(api_key, http),
        None => AlsiAsyncClient::with_http_client_without_api_key(http),
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

    let series = client.fetch_time_series(&query).await?;
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
