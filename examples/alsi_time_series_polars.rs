use std::env;

#[cfg(feature = "polars")]
use gie_client::GieQuery;
#[cfg(feature = "polars")]
use gie_client::alsi::AlsiClient;

#[cfg(feature = "polars")]
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

    let dataframe = client.fetch_time_series_dataframe(&query)?;
    println!(
        "DataFrame: rows={}, columns={}",
        dataframe.height(),
        dataframe.width()
    );

    Ok(())
}

#[cfg(not(feature = "polars"))]
fn main() {
    let _ = env::var("GIE_API_KEY");
    eprintln!("This example requires --features polars");
}
