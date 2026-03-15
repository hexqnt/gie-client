use std::env;

use gie_client::GieQuery;
use gie_client::agsi::AgsiClient;

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
        (Some(api_key), Some(proxy_url)) => AgsiClient::with_proxy(api_key, proxy_url)?,
        (Some(api_key), None) => AgsiClient::new(api_key),
        (None, Some(proxy_url)) => AgsiClient::with_proxy_without_api_key(proxy_url)?,
        (None, None) => AgsiClient::without_api_key(),
    };
    let client = if let Some(user_agent) = user_agent {
        client.with_user_agent(user_agent)
    } else {
        client
    };
    let client = client.with_debug_requests(true);
    let query = GieQuery::new()
        .country("DE")
        .try_date("2026-03-10")?
        .try_size(25)?;

    let page = client.fetch_page(&query)?;

    println!(
        "dataset={:?}, gas_day={:?}, rows={}",
        page.dataset,
        page.gas_day,
        page.data.len()
    );

    if let Some(first) = page.data.first() {
        println!(
            "first record: code={:?}, name={:?}, record_type={:?}, gas_in_storage={:?}",
            first.code, first.name, first.record_type, first.gas_in_storage
        );
    }

    Ok(())
}
