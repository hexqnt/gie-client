# gie-client

[🇺🇸 English](./README.md) · [🇷🇺 Русский](./README.ru.md)

Легковесный клиент на Rust для API прозрачности GIE: **AGSI** и **ALSI**.

[![CI](https://github.com/hexqnt/gie-client/actions/workflows/ci.yml/badge.svg)](https://github.com/hexqnt/gie-client/actions/workflows/ci.yml)
[![Live Contract](https://github.com/hexqnt/gie-client/actions/workflows/live-contract.yml/badge.svg)](https://github.com/hexqnt/gie-client/actions/workflows/live-contract.yml)
[![crates.io](https://img.shields.io/crates/v/gie-client.svg)](https://crates.io/crates/gie-client)
[![docs.rs](https://docs.rs/gie-client/badge.svg)](https://docs.rs/gie-client)

## Возможности

- синхронные и асинхронные клиенты
- работа с `GIE_API_KEY` и без него
- поддержка прокси
- типизированный конструктор запросов (`GieQuery`)
- вспомогательные функции для пагинации и временных рядов
- опциональная интеграция с `polars`
- опциональная поддержка дат через `chrono`

## Установка

```toml
[dependencies]
gie-client = "0.1"
```

## Быстрый старт (синхронный клиент)

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

## Быстрый старт (асинхронный клиент)

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

## Основные настройки

Прокси:

```rust
let client = gie_client::agsi::AgsiClient::with_proxy_without_api_key("http://127.0.0.1:8080")?;
```

Отладка запросов:

```rust
let client = gie_client::agsi::AgsiClient::without_api_key().with_debug_requests(true);
```

Ограничение частоты запросов (по умолчанию — 60 запросов в минуту и пауза 60 секунд после ответа `429`):

```rust
let client = gie_client::agsi::AgsiClient::without_api_key()
    .with_rate_limit(std::num::NonZeroU32::new(30).unwrap());

let client_no_limit = gie_client::agsi::AgsiClient::without_api_key().without_rate_limit();
```

Собственный `User-Agent`:

```rust
let client = gie_client::agsi::AgsiClient::without_api_key().with_user_agent("MyApp/1.0");
```

## Примеры

```bash
cargo run --example agsi_snapshot_sync
cargo run --example alsi_time_series_sync
cargo run --example alsi_time_series_async_external_client
cargo run --example alsi_time_series_polars --features polars
```

В примерах используются переменные окружения:

- `GIE_API_KEY`
- `GIE_PROXY_URL`
- `GIE_USER_AGENT`

## Тесты контракта с API

Эти тесты обращаются к настоящему API GIE, поэтому они намеренно исключены из стандартного задания CI.

Локальный запуск:

```bash
GIE_LIVE_TESTS=1 cargo test --test live_api_contract -- --ignored
```

Чтобы также запустить тесты с аутентификацией, задайте `GIE_API_KEY`.
