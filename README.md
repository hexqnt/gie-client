# gie-client

A lightweight Rust client for GIE transparency APIs: **AGSI** and **ALSI**.

[![CI](https://github.com/hexqnt/gie-client/actions/workflows/ci.yml/badge.svg)](https://github.com/hexqnt/gie-client/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/gie-client.svg)](https://crates.io/crates/gie-client)
[![docs.rs](https://docs.rs/gie-client/badge.svg)](https://docs.rs/gie-client)

## Features

- sync and async clients
- works with or without `GIE_API_KEY`
- proxy support
- typed query builder (`GieQuery`)
- pagination and time-series helpers
- optional `polars` integration
- optional `chrono` date backend

## Installation

```toml
[dependencies]
gie-client = "0.1"
```

## Quick Start (Sync)

```rust
use gie_client::GieQuery;
use gie_client::agsi::AgsiClient;

let client = std::env::var("GIE_API_KEY")
    .ok()
    .filter(|v| !v.trim().is_empty())
    .map(AgsiClient::new)
    .unwrap_or_else(AgsiClient::without_api_key);

let query = GieQuery::new()
    .country("DE")
    .try_date("2026-03-10")?
    .try_size(25)?;

let page = client.fetch_page(&query)?;
println!("rows={}", page.data.len());
```

## Quick Start (Async)

```rust
use gie_client::GieQuery;
use gie_client::alsi::AlsiAsyncClient;

let http = reqwest::Client::new();
let client = std::env::var("GIE_API_KEY")
    .ok()
    .filter(|v| !v.trim().is_empty())
    .map(|key| AlsiAsyncClient::with_http_client(key, http.clone()))
    .unwrap_or_else(|| AlsiAsyncClient::with_http_client_without_api_key(http));

let query = GieQuery::new()
    .country("FR")
    .try_range("2026-03-01", "2026-03-10")?
    .try_size(200)?;

let series = client.fetch_time_series(&query).await?;
println!("series={}", series.len());
```

## Common Options

Proxy:

```rust
let client = gie_client::agsi::AgsiClient::with_proxy_without_api_key("http://127.0.0.1:8080")?;
```

Debug requests:

```rust
let client = gie_client::agsi::AgsiClient::without_api_key().with_debug_requests(true);
```

Rate limit (default is enabled: 60 req/min, 60s cooldown on `429`):

```rust
let client = gie_client::agsi::AgsiClient::without_api_key()
    .with_rate_limit(std::num::NonZeroU32::new(30).unwrap());

let client_no_limit = gie_client::agsi::AgsiClient::without_api_key().without_rate_limit();
```

Custom User-Agent:

```rust
let client = gie_client::agsi::AgsiClient::without_api_key().with_user_agent("MyApp/1.0");
```

## Examples

```bash
cargo run --example agsi_snapshot_sync
cargo run --example alsi_time_series_sync
cargo run --example alsi_time_series_async_external_client
cargo run --example alsi_time_series_polars --features polars
```

Environment variables used by examples:

- `GIE_API_KEY`
- `GIE_PROXY_URL`
- `GIE_USER_AGENT`
