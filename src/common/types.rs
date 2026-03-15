use std::fmt;

#[cfg(feature = "chrono")]
use chrono::NaiveDate;
#[cfg(not(feature = "chrono"))]
use time::{Date, Month};

use super::date_range::DateRange;

/// Unified date type used by the public API.
#[cfg(feature = "chrono")]
pub type GieDate = NaiveDate;
/// Unified date type used by the public API.
#[cfg(not(feature = "chrono"))]
pub type GieDate = Date;

/// Dataset scope accepted by the `type` query parameter on facility reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetType {
    /// Europe aggregate.
    Eu,
    /// Non-EU aggregate.
    Ne,
    /// Additional information aggregate.
    Ai,
}

impl DatasetType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eu => "eu",
            Self::Ne => "ne",
            Self::Ai => "ai",
        }
    }
}

impl fmt::Display for DatasetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dataset name returned by API response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetName {
    /// Underground gas storage dataset.
    Storage,
    /// LNG terminals dataset.
    Lng,
    /// Any future/unknown dataset name.
    Unknown(String),
}

impl DatasetName {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Storage => "storage",
            Self::Lng => "lng",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

impl fmt::Display for DatasetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Entity level returned by API record `type` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordType {
    /// Country-level aggregate.
    Country,
    /// Company-level aggregate.
    Company,
    /// Facility-level aggregate.
    Facility,
    /// Any future/unknown record type.
    Unknown(String),
}

impl RecordType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Country => "country",
            Self::Company => "company",
            Self::Facility => "facility",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Date filter accepted by GIE API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFilter {
    /// Exact gas day.
    Day(GieDate),
    /// Inclusive gas day range.
    Range(DateRange),
}

/// Decoded paginated API response.
#[derive(Debug, Clone)]
pub struct GiePage<T> {
    /// Last page number reported by the API.
    pub last_page: u32,
    /// Total item count reported by the API.
    pub total: u32,
    /// Dataset descriptor from the response envelope.
    pub dataset: Option<DatasetName>,
    /// Gas day from the response envelope.
    pub gas_day: Option<GieDate>,
    /// Decoded page payload.
    pub data: Vec<T>,
}

pub(crate) fn format_date(date: GieDate) -> String {
    #[cfg(feature = "chrono")]
    {
        date.format("%Y-%m-%d").to_string()
    }
    #[cfg(not(feature = "chrono"))]
    {
        YmdDate(date).to_string()
    }
}

pub(crate) fn parse_date(value: &str) -> Result<GieDate, String> {
    let trimmed = value.trim();
    if !trimmed.is_ascii() {
        return Err("invalid date format, expected ASCII YYYY-MM-DD".to_string());
    }

    let bytes = trimmed.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err("invalid date format, expected YYYY-MM-DD".to_string());
    }

    #[cfg(feature = "chrono")]
    {
        NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .map_err(|error| format!("invalid calendar date: {error}"))
    }
    #[cfg(not(feature = "chrono"))]
    {
        let year = parse_year_component(&trimmed[0..4])?;
        let month = parse_month_component(&trimmed[5..7])?;
        let day = parse_day_component(&trimmed[8..10])?;

        Date::from_calendar_date(year, month, day)
            .map_err(|error| format!("invalid calendar date: {error}"))
    }
}

pub(crate) fn parse_dataset_type(value: &str) -> Result<DatasetType, String> {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("eu") {
        Ok(DatasetType::Eu)
    } else if trimmed.eq_ignore_ascii_case("ne") {
        Ok(DatasetType::Ne)
    } else if trimmed.eq_ignore_ascii_case("ai") {
        Ok(DatasetType::Ai)
    } else {
        Err(format!(
            "invalid dataset type, expected one of: eu, ne, ai (got {trimmed:?})"
        ))
    }
}

pub(crate) fn parse_dataset_name(value: &str) -> DatasetName {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("storage") {
        DatasetName::Storage
    } else if trimmed.eq_ignore_ascii_case("lng") {
        DatasetName::Lng
    } else {
        DatasetName::Unknown(trimmed.to_string())
    }
}

pub(crate) fn parse_record_type(value: &str) -> RecordType {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("country") {
        RecordType::Country
    } else if trimmed.eq_ignore_ascii_case("company") {
        RecordType::Company
    } else if trimmed.eq_ignore_ascii_case("facility") {
        RecordType::Facility
    } else {
        RecordType::Unknown(trimmed.to_string())
    }
}

#[cfg(not(feature = "chrono"))]
fn parse_year_component(value: &str) -> Result<i32, String> {
    value
        .parse::<i32>()
        .map_err(|error| format!("invalid year component: {error}"))
}

#[cfg(not(feature = "chrono"))]
fn parse_month_component(value: &str) -> Result<Month, String> {
    let month = value
        .parse::<u8>()
        .map_err(|error| format!("invalid month component: {error}"))?;

    Month::try_from(month).map_err(|_| format!("month component is out of range: {month}"))
}

#[cfg(not(feature = "chrono"))]
fn parse_day_component(value: &str) -> Result<u8, String> {
    value
        .parse::<u8>()
        .map_err(|error| format!("invalid day component: {error}"))
}

#[cfg(not(feature = "chrono"))]
struct YmdDate(Date);

#[cfg(not(feature = "chrono"))]
impl fmt::Display for YmdDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02}",
            self.0.year(),
            u8::from(self.0.month()),
            self.0.day()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_type_parser_is_case_insensitive_and_keeps_unknown_values() {
        assert_eq!(parse_record_type(" country "), RecordType::Country);
        assert_eq!(parse_record_type("CoMpAnY"), RecordType::Company);
        assert_eq!(parse_record_type("FACILITY"), RecordType::Facility);
        assert_eq!(
            parse_record_type("pipeline"),
            RecordType::Unknown("pipeline".to_string())
        );
    }

    #[test]
    fn dataset_name_parser_is_case_insensitive_and_keeps_unknown_values() {
        assert_eq!(parse_dataset_name(" storage "), DatasetName::Storage);
        assert_eq!(parse_dataset_name("LNG"), DatasetName::Lng);
        assert_eq!(
            parse_dataset_name("storage ERROR"),
            DatasetName::Unknown("storage ERROR".to_string())
        );
    }
}
