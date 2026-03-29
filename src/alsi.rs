//! ALSI clients and record models.

use std::num::NonZeroU32;

use serde::Deserialize;

use crate::client_core::{AsyncClientCore, BlockingClientCore, Endpoint};
#[cfg(feature = "polars")]
use crate::common::polars_core::CommonFrameColumns;
use crate::common::{
    GieDate, GiePage, GieQuery, RecordType,
    serde_ext::{
        deserialize_optional_date, deserialize_optional_f64, deserialize_optional_record_type,
    },
    time_series::group_time_series,
};
use crate::error::GieError;
#[cfg(feature = "polars")]
use polars::prelude::{DataFrame, NamedFrom, Series};

const ALSI_API_URL: &str = "https://alsi.gie.eu/api";

struct AlsiEndpoint;

impl Endpoint for AlsiEndpoint {
    type Record = AlsiRecord;

    const URL: &'static str = ALSI_API_URL;
}

/// Synchronous ALSI client.
#[derive(Debug, Clone)]
pub struct AlsiClient {
    core: BlockingClientCore,
}

impl AlsiClient {
    fn from_core(core: BlockingClientCore) -> Self {
        Self { core }
    }

    fn map_core(self, map: impl FnOnce(BlockingClientCore) -> BlockingClientCore) -> Self {
        Self::from_core(map(self.core))
    }

    /// Creates a client with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::from_core(BlockingClientCore::new(api_key))
    }

    /// Creates a client without an API key.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn without_api_key() -> Self {
        Self::from_core(BlockingClientCore::without_api_key())
    }

    /// Creates a client using an external blocking HTTP client.
    pub fn with_http_client(api_key: impl Into<String>, http: reqwest::blocking::Client) -> Self {
        Self::from_core(BlockingClientCore::with_http_client(
            Some(api_key.into()),
            http,
        ))
    }

    /// Creates a blocking client configured with a proxy URL.
    pub fn with_proxy(
        api_key: impl Into<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        BlockingClientCore::with_proxy(Some(api_key.into()), proxy_url).map(Self::from_core)
    }

    /// Creates a client without an API key using an external blocking HTTP client.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_http_client_without_api_key(http: reqwest::blocking::Client) -> Self {
        Self::from_core(BlockingClientCore::with_http_client(None, http))
    }

    /// Creates a blocking client without an API key and with proxy support.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_proxy_without_api_key(proxy_url: impl AsRef<str>) -> Result<Self, GieError> {
        BlockingClientCore::with_proxy(None, proxy_url).map(Self::from_core)
    }

    /// Overrides the `User-Agent` header used for API requests.
    pub fn with_user_agent(self, user_agent: impl Into<String>) -> Self {
        self.map_core(|core| core.with_user_agent(user_agent))
    }

    /// Disables sending the `User-Agent` header.
    pub fn without_user_agent(self) -> Self {
        self.map_core(BlockingClientCore::without_user_agent)
    }

    /// Enables or disables debug logging of outgoing requests.
    pub fn with_debug_requests(self, enabled: bool) -> Self {
        self.map_core(|core| core.with_debug_requests(enabled))
    }

    /// Sets per-process request limit (requests per minute).
    pub fn with_rate_limit(self, requests_per_minute: NonZeroU32) -> Self {
        self.map_core(|core| core.with_rate_limit(requests_per_minute))
    }

    /// Disables built-in per-process rate limiting.
    pub fn without_rate_limit(self) -> Self {
        self.map_core(BlockingClientCore::without_rate_limit)
    }

    /// Fetches a single page of ALSI records.
    pub fn fetch_page(&self, query: &GieQuery) -> Result<GiePage<AlsiRecord>, GieError> {
        self.core.fetch_page::<AlsiEndpoint>(query)
    }

    /// Fetches and flattens all pages for the provided query.
    pub fn fetch_all(&self, query: &GieQuery) -> Result<Vec<AlsiRecord>, GieError> {
        self.core.fetch_all::<AlsiEndpoint>(query)
    }

    /// Fetches all rows and groups them into sorted time series.
    pub fn fetch_time_series(&self, query: &GieQuery) -> Result<Vec<AlsiTimeSeries>, GieError> {
        let rows = self.fetch_all(query)?;
        Ok(build_time_series(rows))
    }

    #[cfg(feature = "polars")]
    /// Fetches all rows and converts them into a `polars::DataFrame`.
    pub fn fetch_all_dataframe(&self, query: &GieQuery) -> Result<DataFrame, GieError> {
        let rows = self.fetch_all(query)?;
        records_to_dataframe(&rows)
    }

    #[cfg(feature = "polars")]
    /// Fetches time series and converts them into a flat `polars::DataFrame`.
    pub fn fetch_time_series_dataframe(&self, query: &GieQuery) -> Result<DataFrame, GieError> {
        let series = self.fetch_time_series(query)?;
        time_series_to_dataframe(&series)
    }
}

/// Asynchronous ALSI client.
#[derive(Debug, Clone)]
pub struct AlsiAsyncClient {
    core: AsyncClientCore,
}

impl AlsiAsyncClient {
    fn from_core(core: AsyncClientCore) -> Self {
        Self { core }
    }

    fn map_core(self, map: impl FnOnce(AsyncClientCore) -> AsyncClientCore) -> Self {
        Self::from_core(map(self.core))
    }

    /// Creates an async client with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::from_core(AsyncClientCore::new(api_key))
    }

    /// Creates an async client without an API key.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn without_api_key() -> Self {
        Self::from_core(AsyncClientCore::without_api_key())
    }

    /// Creates an async client using an external HTTP client.
    pub fn with_http_client(api_key: impl Into<String>, http: reqwest::Client) -> Self {
        Self::from_core(AsyncClientCore::with_http_client(
            Some(api_key.into()),
            http,
        ))
    }

    /// Creates an async client configured with a proxy URL.
    pub fn with_proxy(
        api_key: impl Into<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        AsyncClientCore::with_proxy(Some(api_key.into()), proxy_url).map(Self::from_core)
    }

    /// Creates an async client without an API key using an external HTTP client.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_http_client_without_api_key(http: reqwest::Client) -> Self {
        Self::from_core(AsyncClientCore::with_http_client(None, http))
    }

    /// Creates an async client without an API key and with proxy support.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_proxy_without_api_key(proxy_url: impl AsRef<str>) -> Result<Self, GieError> {
        AsyncClientCore::with_proxy(None, proxy_url).map(Self::from_core)
    }

    /// Overrides the `User-Agent` header used for API requests.
    pub fn with_user_agent(self, user_agent: impl Into<String>) -> Self {
        self.map_core(|core| core.with_user_agent(user_agent))
    }

    /// Disables sending the `User-Agent` header.
    pub fn without_user_agent(self) -> Self {
        self.map_core(AsyncClientCore::without_user_agent)
    }

    /// Enables or disables debug logging of outgoing requests.
    pub fn with_debug_requests(self, enabled: bool) -> Self {
        self.map_core(|core| core.with_debug_requests(enabled))
    }

    /// Sets per-process request limit (requests per minute).
    pub fn with_rate_limit(self, requests_per_minute: NonZeroU32) -> Self {
        self.map_core(|core| core.with_rate_limit(requests_per_minute))
    }

    /// Disables built-in per-process rate limiting.
    pub fn without_rate_limit(self) -> Self {
        self.map_core(AsyncClientCore::without_rate_limit)
    }

    /// Fetches a single page of ALSI records.
    pub async fn fetch_page(&self, query: &GieQuery) -> Result<GiePage<AlsiRecord>, GieError> {
        self.core.fetch_page::<AlsiEndpoint>(query).await
    }

    /// Fetches and flattens all pages for the provided query.
    pub async fn fetch_all(&self, query: &GieQuery) -> Result<Vec<AlsiRecord>, GieError> {
        self.core.fetch_all::<AlsiEndpoint>(query).await
    }

    /// Fetches all rows and groups them into sorted time series.
    pub async fn fetch_time_series(
        &self,
        query: &GieQuery,
    ) -> Result<Vec<AlsiTimeSeries>, GieError> {
        let rows = self.fetch_all(query).await?;
        Ok(build_time_series(rows))
    }

    #[cfg(feature = "polars")]
    /// Fetches all rows and converts them into a `polars::DataFrame`.
    pub async fn fetch_all_dataframe(&self, query: &GieQuery) -> Result<DataFrame, GieError> {
        let rows = self.fetch_all(query).await?;
        records_to_dataframe(&rows)
    }

    #[cfg(feature = "polars")]
    /// Fetches time series and converts them into a flat `polars::DataFrame`.
    pub async fn fetch_time_series_dataframe(
        &self,
        query: &GieQuery,
    ) -> Result<DataFrame, GieError> {
        let series = self.fetch_time_series(query).await?;
        time_series_to_dataframe(&series)
    }
}

/// Identity of a single ALSI time series.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlsiSeriesKey {
    /// Country or facility code.
    pub code: Option<String>,
    /// Human-readable entity name.
    pub name: Option<String>,
    /// Entity URL slug from the API.
    pub url: Option<String>,
}

impl From<&AlsiRecord> for AlsiSeriesKey {
    fn from(value: &AlsiRecord) -> Self {
        Self {
            code: value.code.clone(),
            name: value.name.clone(),
            url: value.url.clone(),
        }
    }
}

/// A single ALSI time series, sorted by gas day.
#[derive(Debug, Clone)]
pub struct AlsiTimeSeries {
    /// Series identity.
    pub key: AlsiSeriesKey,
    /// Time-ordered points.
    pub points: Vec<AlsiRecord>,
}

fn build_time_series(rows: Vec<AlsiRecord>) -> Vec<AlsiTimeSeries> {
    group_time_series(
        rows,
        |record: &AlsiRecord| AlsiSeriesKey::from(record),
        |record| record.gas_day_start,
    )
    .into_iter()
    .map(|(key, points)| AlsiTimeSeries { key, points })
    .collect()
}

#[cfg(feature = "polars")]
/// Converts a flat ALSI record slice into a `polars::DataFrame`.
pub fn records_to_dataframe(rows: &[AlsiRecord]) -> Result<DataFrame, GieError> {
    records_to_dataframe_from_iter(rows.iter())
}

#[cfg(feature = "polars")]
/// Converts ALSI time series into a flat `polars::DataFrame`.
pub fn time_series_to_dataframe(series: &[AlsiTimeSeries]) -> Result<DataFrame, GieError> {
    records_to_dataframe_from_iter(series.iter().flat_map(|entry| entry.points.iter()))
}

#[cfg(feature = "polars")]
fn records_to_dataframe_from_iter<'a, I>(rows: I) -> Result<DataFrame, GieError>
where
    I: IntoIterator<Item = &'a AlsiRecord>,
{
    let rows = rows.into_iter();
    let (capacity, _) = rows.size_hint();

    let mut common = CommonFrameColumns::with_capacity(capacity);
    let mut inventory = Vec::with_capacity(capacity);
    let mut send_out = Vec::with_capacity(capacity);
    let mut dtmi = Vec::with_capacity(capacity);
    let mut dtrs = Vec::with_capacity(capacity);

    for row in rows {
        common.push(
            &row.name,
            &row.code,
            &row.url,
            row.gas_day_start,
            row.info.as_deref(),
            row.children.as_deref(),
        )?;
        inventory.push(row.inventory);
        send_out.push(row.send_out);
        dtmi.push(row.dtmi);
        dtrs.push(row.dtrs);
    }

    let height = common.height();
    let (mut columns, tail_columns) = common.into_polars_columns();
    columns.extend([
        Series::new("inventory".into(), inventory).into(),
        Series::new("send_out".into(), send_out).into(),
        Series::new("dtmi".into(), dtmi).into(),
        Series::new("dtrs".into(), dtrs).into(),
    ]);
    columns.extend(tail_columns);

    DataFrame::new(height, columns).map_err(Into::into)
}

/// Raw ALSI record as returned by the API.
///
/// When no API key is provided, ALSI usually returns aggregate/country rows only.
/// Company/facility hierarchy rows require `GIE_API_KEY`.
/// In practice, this mostly affects:
/// - `record_type` (`company`/`facility` levels);
/// - `code`/`url`/`name` values for company and facility entities.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlsiRecord {
    pub name: Option<String>,
    pub code: Option<String>,
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_optional_record_type"
    )]
    pub record_type: Option<RecordType>,
    pub url: Option<String>,
    #[serde(
        rename = "gasDayStart",
        default,
        deserialize_with = "deserialize_optional_date"
    )]
    pub gas_day_start: Option<GieDate>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub inventory: Option<f64>,
    #[serde(
        rename = "sendOut",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub send_out: Option<f64>,
    #[serde(
        rename = "dtmi",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub dtmi: Option<f64>,
    #[serde(
        rename = "dtrs",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub dtrs: Option<f64>,
    pub info: Option<Vec<serde_json::Value>>,
    pub children: Option<Vec<serde_json::Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_core::client_configuration_tests;
    use crate::common::types::parse_date;

    fn test_date(value: &str) -> GieDate {
        parse_date(value).unwrap()
    }

    #[test]
    fn builds_sorted_time_series_sets() {
        let rows = vec![
            AlsiRecord {
                code: Some("FR-1".to_string()),
                name: Some("Terminal 1".to_string()),
                gas_day_start: Some(test_date("2026-03-03")),
                ..AlsiRecord::default()
            },
            AlsiRecord {
                code: Some("FR-2".to_string()),
                name: Some("Terminal 2".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                ..AlsiRecord::default()
            },
            AlsiRecord {
                code: Some("FR-1".to_string()),
                name: Some("Terminal 1".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                ..AlsiRecord::default()
            },
        ];

        let series = build_time_series(rows);

        assert_eq!(series.len(), 2);
        assert_eq!(series[0].key.code.as_deref(), Some("FR-1"));
        assert_eq!(
            series[0]
                .points
                .iter()
                .filter_map(|row| row.gas_day_start)
                .collect::<Vec<_>>(),
            vec![test_date("2026-03-01"), test_date("2026-03-03")]
        );
        assert_eq!(series[1].key.code.as_deref(), Some("FR-2"));
    }

    client_configuration_tests!(AlsiClient, AlsiAsyncClient);

    #[cfg(feature = "polars")]
    #[test]
    fn records_are_converted_to_polars_dataframe() {
        let rows = vec![
            AlsiRecord {
                code: Some("FR-1".to_string()),
                name: Some("Terminal 1".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                inventory: Some(10.0),
                ..AlsiRecord::default()
            },
            AlsiRecord {
                code: Some("FR-2".to_string()),
                name: Some("Terminal 2".to_string()),
                gas_day_start: Some(test_date("2026-03-02")),
                inventory: Some(20.0),
                ..AlsiRecord::default()
            },
        ];

        let frame = records_to_dataframe(&rows).unwrap();
        assert_eq!(frame.height(), 2);
        assert_eq!(frame.width(), 10);
        assert!(frame.column("code").is_ok());
        assert!(frame.column("inventory").is_ok());
    }

    #[cfg(feature = "polars")]
    #[test]
    fn time_series_are_converted_to_polars_dataframe() {
        let series = vec![
            AlsiTimeSeries {
                key: AlsiSeriesKey {
                    code: Some("FR-1".to_string()),
                    name: Some("Terminal 1".to_string()),
                    url: None,
                },
                points: vec![AlsiRecord {
                    code: Some("FR-1".to_string()),
                    gas_day_start: Some(test_date("2026-03-01")),
                    ..AlsiRecord::default()
                }],
            },
            AlsiTimeSeries {
                key: AlsiSeriesKey {
                    code: Some("FR-2".to_string()),
                    name: Some("Terminal 2".to_string()),
                    url: None,
                },
                points: vec![
                    AlsiRecord {
                        code: Some("FR-2".to_string()),
                        gas_day_start: Some(test_date("2026-03-02")),
                        ..AlsiRecord::default()
                    },
                    AlsiRecord {
                        code: Some("FR-2".to_string()),
                        gas_day_start: Some(test_date("2026-03-03")),
                        ..AlsiRecord::default()
                    },
                ],
            },
        ];

        let frame = time_series_to_dataframe(&series).unwrap();
        assert_eq!(frame.height(), 3);
        assert!(frame.column("gas_day_start").is_ok());
    }
}
