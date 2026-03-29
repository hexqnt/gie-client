pub(crate) mod date_range;
pub(crate) mod http_core;
#[cfg(feature = "polars")]
pub(crate) mod polars_core;
pub(crate) mod query;
pub(crate) mod serde_ext;
pub(crate) mod time_series;
pub(crate) mod types;

pub(crate) use http_core::DEFAULT_BROWSER_USER_AGENT;

pub use date_range::DateRange;
pub use query::{GieQuery, QueryText};
pub use types::{DatasetName, DatasetType, DateFilter, GieDate, GiePage, RecordType};
