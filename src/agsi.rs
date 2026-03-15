//! AGSI clients and record models.

use std::num::NonZeroU32;

use serde::Deserialize;

use crate::client_core::{AsyncClientCore, BlockingClientCore, Endpoint};
use crate::common::{
    GieDate, GiePage, GieQuery, RecordType,
    serde_ext::{
        deserialize_optional_date, deserialize_optional_f64, deserialize_optional_record_type,
        deserialize_optional_string,
    },
    time_series::group_time_series,
};
#[cfg(feature = "polars")]
use crate::common::{format_date, serde_ext::json_vec_to_string};
use crate::error::GieError;
#[cfg(feature = "polars")]
use polars::prelude::{DataFrame, NamedFrom, Series};

const AGSI_API_URL: &str = "https://agsi.gie.eu/api";

struct AgsiEndpoint;

impl Endpoint for AgsiEndpoint {
    type Record = AgsiRecord;

    const URL: &'static str = AGSI_API_URL;
}

/// Synchronous AGSI client.
#[derive(Debug, Clone)]
pub struct AgsiClient {
    core: BlockingClientCore,
}

impl AgsiClient {
    /// Creates a client with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            core: BlockingClientCore::new(api_key),
        }
    }

    /// Creates a client without an API key.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn without_api_key() -> Self {
        Self {
            core: BlockingClientCore::without_api_key(),
        }
    }

    /// Creates a client using an external blocking HTTP client.
    pub fn with_http_client(api_key: impl Into<String>, http: reqwest::blocking::Client) -> Self {
        Self {
            core: BlockingClientCore::with_http_client(Some(api_key.into()), http),
        }
    }

    /// Creates a blocking client configured with a proxy URL.
    pub fn with_proxy(
        api_key: impl Into<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        Ok(Self {
            core: BlockingClientCore::with_proxy(Some(api_key.into()), proxy_url)?,
        })
    }

    /// Creates a client without an API key using an external blocking HTTP client.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_http_client_without_api_key(http: reqwest::blocking::Client) -> Self {
        Self {
            core: BlockingClientCore::with_http_client(None, http),
        }
    }

    /// Creates a blocking client without an API key and with proxy support.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_proxy_without_api_key(proxy_url: impl AsRef<str>) -> Result<Self, GieError> {
        Ok(Self {
            core: BlockingClientCore::with_proxy(None, proxy_url)?,
        })
    }

    /// Overrides the `User-Agent` header used for API requests.
    pub fn with_user_agent(self, user_agent: impl Into<String>) -> Self {
        let core = self.core.with_user_agent(user_agent);
        Self { core }
    }

    /// Disables sending the `User-Agent` header.
    pub fn without_user_agent(self) -> Self {
        let core = self.core.without_user_agent();
        Self { core }
    }

    /// Enables or disables debug logging of outgoing requests.
    pub fn with_debug_requests(self, enabled: bool) -> Self {
        let core = self.core.with_debug_requests(enabled);
        Self { core }
    }

    /// Sets per-process request limit (requests per minute).
    pub fn with_rate_limit(self, requests_per_minute: NonZeroU32) -> Self {
        let core = self.core.with_rate_limit(requests_per_minute);
        Self { core }
    }

    /// Disables built-in per-process rate limiting.
    pub fn without_rate_limit(self) -> Self {
        let core = self.core.without_rate_limit();
        Self { core }
    }

    /// Fetches a single page of AGSI records.
    pub fn fetch_page(&self, query: &GieQuery) -> Result<GiePage<AgsiRecord>, GieError> {
        self.core.fetch_page::<AgsiEndpoint>(query)
    }

    /// Fetches and flattens all pages for the provided query.
    pub fn fetch_all(&self, query: &GieQuery) -> Result<Vec<AgsiRecord>, GieError> {
        self.core.fetch_all::<AgsiEndpoint>(query)
    }

    /// Fetches all rows and groups them into sorted time series.
    pub fn fetch_time_series(&self, query: &GieQuery) -> Result<Vec<AgsiTimeSeries>, GieError> {
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

/// Asynchronous AGSI client.
#[derive(Debug, Clone)]
pub struct AgsiAsyncClient {
    core: AsyncClientCore,
}

impl AgsiAsyncClient {
    /// Creates an async client with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            core: AsyncClientCore::new(api_key),
        }
    }

    /// Creates an async client without an API key.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn without_api_key() -> Self {
        Self {
            core: AsyncClientCore::without_api_key(),
        }
    }

    /// Creates an async client using an external HTTP client.
    pub fn with_http_client(api_key: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            core: AsyncClientCore::with_http_client(Some(api_key.into()), http),
        }
    }

    /// Creates an async client configured with a proxy URL.
    pub fn with_proxy(
        api_key: impl Into<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        Ok(Self {
            core: AsyncClientCore::with_proxy(Some(api_key.into()), proxy_url)?,
        })
    }

    /// Creates an async client without an API key using an external HTTP client.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_http_client_without_api_key(http: reqwest::Client) -> Self {
        Self {
            core: AsyncClientCore::with_http_client(None, http),
        }
    }

    /// Creates an async client without an API key and with proxy support.
    ///
    /// Company/facility hierarchy rows are typically unavailable in this mode.
    pub fn with_proxy_without_api_key(proxy_url: impl AsRef<str>) -> Result<Self, GieError> {
        Ok(Self {
            core: AsyncClientCore::with_proxy(None, proxy_url)?,
        })
    }

    /// Overrides the `User-Agent` header used for API requests.
    pub fn with_user_agent(self, user_agent: impl Into<String>) -> Self {
        let core = self.core.with_user_agent(user_agent);
        Self { core }
    }

    /// Disables sending the `User-Agent` header.
    pub fn without_user_agent(self) -> Self {
        let core = self.core.without_user_agent();
        Self { core }
    }

    /// Enables or disables debug logging of outgoing requests.
    pub fn with_debug_requests(self, enabled: bool) -> Self {
        let core = self.core.with_debug_requests(enabled);
        Self { core }
    }

    /// Sets per-process request limit (requests per minute).
    pub fn with_rate_limit(self, requests_per_minute: NonZeroU32) -> Self {
        let core = self.core.with_rate_limit(requests_per_minute);
        Self { core }
    }

    /// Disables built-in per-process rate limiting.
    pub fn without_rate_limit(self) -> Self {
        let core = self.core.without_rate_limit();
        Self { core }
    }

    /// Fetches a single page of AGSI records.
    pub async fn fetch_page(&self, query: &GieQuery) -> Result<GiePage<AgsiRecord>, GieError> {
        self.core.fetch_page::<AgsiEndpoint>(query).await
    }

    /// Fetches and flattens all pages for the provided query.
    pub async fn fetch_all(&self, query: &GieQuery) -> Result<Vec<AgsiRecord>, GieError> {
        self.core.fetch_all::<AgsiEndpoint>(query).await
    }

    /// Fetches all rows and groups them into sorted time series.
    pub async fn fetch_time_series(
        &self,
        query: &GieQuery,
    ) -> Result<Vec<AgsiTimeSeries>, GieError> {
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

/// Identity of a single AGSI time series.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AgsiSeriesKey {
    /// Country or facility code.
    pub code: Option<String>,
    /// Human-readable entity name.
    pub name: Option<String>,
    /// Entity URL slug from the API.
    pub url: Option<String>,
}

impl From<&AgsiRecord> for AgsiSeriesKey {
    fn from(value: &AgsiRecord) -> Self {
        Self {
            code: value.code.clone(),
            name: value.name.clone(),
            url: value.url.clone(),
        }
    }
}

/// A single AGSI time series, sorted by gas day.
#[derive(Debug, Clone)]
pub struct AgsiTimeSeries {
    /// Series identity.
    pub key: AgsiSeriesKey,
    /// Time-ordered points.
    pub points: Vec<AgsiRecord>,
}

fn build_time_series(rows: Vec<AgsiRecord>) -> Vec<AgsiTimeSeries> {
    group_time_series(
        rows,
        |record: &AgsiRecord| AgsiSeriesKey::from(record),
        |record| record.gas_day_start,
    )
    .into_iter()
    .map(|(key, points)| AgsiTimeSeries { key, points })
    .collect()
}

#[cfg(feature = "polars")]
/// Converts a flat AGSI record slice into a `polars::DataFrame`.
pub fn records_to_dataframe(rows: &[AgsiRecord]) -> Result<DataFrame, GieError> {
    records_to_dataframe_from_iter(rows.iter())
}

#[cfg(feature = "polars")]
/// Converts AGSI time series into a flat `polars::DataFrame`.
pub fn time_series_to_dataframe(series: &[AgsiTimeSeries]) -> Result<DataFrame, GieError> {
    records_to_dataframe_from_iter(series.iter().flat_map(|entry| entry.points.iter()))
}

#[cfg(feature = "polars")]
fn records_to_dataframe_from_iter<'a, I>(rows: I) -> Result<DataFrame, GieError>
where
    I: IntoIterator<Item = &'a AgsiRecord>,
{
    let rows = rows.into_iter();
    let (capacity, _) = rows.size_hint();

    let mut name = Vec::with_capacity(capacity);
    let mut code = Vec::with_capacity(capacity);
    let mut url = Vec::with_capacity(capacity);
    let mut gas_day_start = Vec::with_capacity(capacity);
    let mut gas_in_storage = Vec::with_capacity(capacity);
    let mut consumption = Vec::with_capacity(capacity);
    let mut consumption_full = Vec::with_capacity(capacity);
    let mut injection = Vec::with_capacity(capacity);
    let mut net_withdrawal = Vec::with_capacity(capacity);
    let mut withdrawal = Vec::with_capacity(capacity);
    let mut working_gas_volume = Vec::with_capacity(capacity);
    let mut injection_capacity = Vec::with_capacity(capacity);
    let mut withdrawal_capacity = Vec::with_capacity(capacity);
    let mut status = Vec::with_capacity(capacity);
    let mut trend = Vec::with_capacity(capacity);
    let mut full = Vec::with_capacity(capacity);
    let mut info_json = Vec::with_capacity(capacity);
    let mut children_json = Vec::with_capacity(capacity);

    for row in rows {
        name.push(row.name.clone());
        code.push(row.code.clone());
        url.push(row.url.clone());
        gas_day_start.push(row.gas_day_start.map(format_date));
        gas_in_storage.push(row.gas_in_storage);
        consumption.push(row.consumption);
        consumption_full.push(row.consumption_full);
        injection.push(row.injection);
        net_withdrawal.push(row.net_withdrawal);
        withdrawal.push(row.withdrawal);
        working_gas_volume.push(row.working_gas_volume);
        injection_capacity.push(row.injection_capacity);
        withdrawal_capacity.push(row.withdrawal_capacity);
        status.push(row.status.clone());
        trend.push(row.trend);
        full.push(row.full);
        info_json.push(json_vec_to_string(row.info.as_deref())?);
        children_json.push(json_vec_to_string(row.children.as_deref())?);
    }

    DataFrame::new(
        name.len(),
        vec![
            Series::new("name".into(), name).into(),
            Series::new("code".into(), code).into(),
            Series::new("url".into(), url).into(),
            Series::new("gas_day_start".into(), gas_day_start).into(),
            Series::new("gas_in_storage".into(), gas_in_storage).into(),
            Series::new("consumption".into(), consumption).into(),
            Series::new("consumption_full".into(), consumption_full).into(),
            Series::new("injection".into(), injection).into(),
            Series::new("net_withdrawal".into(), net_withdrawal).into(),
            Series::new("withdrawal".into(), withdrawal).into(),
            Series::new("working_gas_volume".into(), working_gas_volume).into(),
            Series::new("injection_capacity".into(), injection_capacity).into(),
            Series::new("withdrawal_capacity".into(), withdrawal_capacity).into(),
            Series::new("status".into(), status).into(),
            Series::new("trend".into(), trend).into(),
            Series::new("full".into(), full).into(),
            Series::new("info_json".into(), info_json).into(),
            Series::new("children_json".into(), children_json).into(),
        ],
    )
    .map_err(Into::into)
}

/// Raw AGSI record as returned by the API.
///
/// When no API key is provided, AGSI usually returns aggregate/country rows only.
/// Company/facility hierarchy rows require `GIE_API_KEY`.
/// In practice, this mostly affects:
/// - `record_type` (`company`/`facility` levels);
/// - `code`/`url`/`name` values for company and facility entities.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgsiRecord {
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
    #[serde(
        rename = "gasInStorage",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub gas_in_storage: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub consumption: Option<f64>,
    #[serde(
        rename = "consumptionFull",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub consumption_full: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub injection: Option<f64>,
    #[serde(
        rename = "netWithdrawal",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub net_withdrawal: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub withdrawal: Option<f64>,
    #[serde(
        rename = "workingGasVolume",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub working_gas_volume: Option<f64>,
    #[serde(
        rename = "injectionCapacity",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub injection_capacity: Option<f64>,
    #[serde(
        rename = "withdrawalCapacity",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    pub withdrawal_capacity: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub trend: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    pub full: Option<f64>,
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
            AgsiRecord {
                code: Some("DE-1".to_string()),
                name: Some("Site 1".to_string()),
                gas_day_start: Some(test_date("2026-03-03")),
                ..AgsiRecord::default()
            },
            AgsiRecord {
                code: Some("DE-2".to_string()),
                name: Some("Site 2".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                ..AgsiRecord::default()
            },
            AgsiRecord {
                code: Some("DE-1".to_string()),
                name: Some("Site 1".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                ..AgsiRecord::default()
            },
        ];

        let series = build_time_series(rows);

        assert_eq!(series.len(), 2);
        assert_eq!(series[0].key.code.as_deref(), Some("DE-1"));
        assert_eq!(
            series[0]
                .points
                .iter()
                .filter_map(|row| row.gas_day_start)
                .collect::<Vec<_>>(),
            vec![test_date("2026-03-01"), test_date("2026-03-03")]
        );
        assert_eq!(series[1].key.code.as_deref(), Some("DE-2"));
    }

    client_configuration_tests!(AgsiClient, AgsiAsyncClient);

    #[cfg(feature = "polars")]
    #[test]
    fn records_are_converted_to_polars_dataframe() {
        let rows = vec![
            AgsiRecord {
                code: Some("DE-1".to_string()),
                name: Some("Site 1".to_string()),
                gas_day_start: Some(test_date("2026-03-01")),
                gas_in_storage: Some(10.0),
                ..AgsiRecord::default()
            },
            AgsiRecord {
                code: Some("DE-2".to_string()),
                name: Some("Site 2".to_string()),
                gas_day_start: Some(test_date("2026-03-02")),
                gas_in_storage: Some(20.0),
                ..AgsiRecord::default()
            },
        ];

        let frame = records_to_dataframe(&rows).unwrap();
        assert_eq!(frame.height(), 2);
        assert_eq!(frame.width(), 18);
        assert!(frame.column("code").is_ok());
        assert!(frame.column("gas_in_storage").is_ok());
    }

    #[cfg(feature = "polars")]
    #[test]
    fn time_series_are_converted_to_polars_dataframe() {
        let series = vec![
            AgsiTimeSeries {
                key: AgsiSeriesKey {
                    code: Some("DE-1".to_string()),
                    name: Some("Site 1".to_string()),
                    url: None,
                },
                points: vec![AgsiRecord {
                    code: Some("DE-1".to_string()),
                    gas_day_start: Some(test_date("2026-03-01")),
                    ..AgsiRecord::default()
                }],
            },
            AgsiTimeSeries {
                key: AgsiSeriesKey {
                    code: Some("DE-2".to_string()),
                    name: Some("Site 2".to_string()),
                    url: None,
                },
                points: vec![
                    AgsiRecord {
                        code: Some("DE-2".to_string()),
                        gas_day_start: Some(test_date("2026-03-02")),
                        ..AgsiRecord::default()
                    },
                    AgsiRecord {
                        code: Some("DE-2".to_string()),
                        gas_day_start: Some(test_date("2026-03-03")),
                        ..AgsiRecord::default()
                    },
                ],
            },
        ];

        let frame = time_series_to_dataframe(&series).unwrap();
        assert_eq!(frame.height(), 3);
        assert!(frame.column("gas_day_start").is_ok());
    }
}
