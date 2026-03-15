//! Rust client for GIE transparency APIs (AGSI and ALSI).

/// AGSI clients and data models.
pub mod agsi;
/// ALSI clients and data models.
pub mod alsi;

mod client_core;
mod common;
mod error;

/// Dataset name returned in API response envelope.
pub use common::DatasetName;
/// Dataset type accepted by the `type` query parameter.
pub use common::DatasetType;
/// Date filter used by query builder.
pub use common::DateFilter;
/// Validated inclusive date range.
pub use common::DateRange;
/// Unified date type (`time::Date` by default, `chrono::NaiveDate` with `chrono` feature).
pub use common::GieDate;
/// Generic paginated response wrapper returned by GIE endpoints.
pub use common::GiePage;
/// Shared query builder used by both AGSI and ALSI clients.
pub use common::GieQuery;
/// Entity level returned by record `type` field.
pub use common::RecordType;
/// Error type returned by all client operations.
pub use error::GieError;
