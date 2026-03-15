#[cfg(feature = "polars")]
use polars::error::PolarsError;
use reqwest::StatusCode;
use thiserror::Error;

/// Unified error type for AGSI/ALSI client operations.
#[derive(Debug, Error)]
pub enum GieError {
    /// Transport-level error produced by `reqwest`.
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// Failed to decode JSON payload returned by the API.
    #[error("failed to decode response JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Non-success HTTP status with the raw response body.
    #[error("HTTP {status}: {body}")]
    HttpStatus { status: StatusCode, body: String },
    /// Structured API-level error returned by GIE.
    #[error("GIE API error: {error}: {message}")]
    Api { error: String, message: String },
    /// Invalid date input provided by caller or API payload.
    #[error("invalid date input: {0}")]
    InvalidDateInput(String),
    /// Invalid dataset type input provided by caller.
    #[error("invalid dataset type input: {0}")]
    InvalidDatasetTypeInput(String),
    /// Invalid page input provided by caller.
    #[error("invalid page input: {0}")]
    InvalidPageInput(String),
    /// Invalid page size input provided by caller.
    #[error("invalid size input: {0}")]
    InvalidSizeInput(String),
    /// Invalid date range input provided by caller.
    #[error("invalid date range input: {0}")]
    InvalidDateRangeInput(String),
    #[cfg(feature = "polars")]
    /// Error while building a `polars::DataFrame`.
    #[error("failed to build polars DataFrame: {0}")]
    Polars(#[from] PolarsError),
}
