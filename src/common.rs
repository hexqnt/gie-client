pub(crate) mod date_range;
pub(crate) mod http_core;
pub(crate) mod query;
pub(crate) mod serde_ext;
pub(crate) mod time_series;
pub(crate) mod types;

pub(crate) use http_core::DEFAULT_BROWSER_USER_AGENT;
#[cfg(feature = "polars")]
pub(crate) use types::format_date;

pub use date_range::DateRange;
pub use query::GieQuery;
pub use types::{DatasetName, DatasetType, DateFilter, GieDate, GiePage, RecordType};
