use std::num::NonZeroU32;
use std::ops::Deref;

use serde::Serialize;

use crate::error::GieError;

use super::date_range::DateRange;
use super::serde_ext::{serialize_optional_dataset_type, serialize_optional_date};
use super::types::{DatasetType, DateFilter, GieDate, format_date, parse_dataset_type, parse_date};

/// Validated non-empty text filter used by query fields like `country`, `company`, and `facility`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryText(String);

impl QueryText {
    /// Creates a validated filter value.
    ///
    /// Leading/trailing whitespace is trimmed.
    /// Returns an error if the resulting value is empty.
    pub fn try_new(value: impl Into<String>) -> Result<Self, GieError> {
        parse_required_text_filter("value", value.into())
    }

    /// Returns the normalized string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn parse_lossy(value: String) -> Option<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        if trimmed.len() == value.len() {
            return Some(Self(value));
        }
        Some(Self(trimmed.to_string()))
    }
}

impl AsRef<str> for QueryText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for QueryText {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Query builder shared by AGSI and ALSI endpoints.
#[must_use = "query builders are immutable; use the returned value"]
#[derive(Debug, Clone, Default)]
pub struct GieQuery {
    country: Option<QueryText>,
    company: Option<QueryText>,
    facility: Option<QueryText>,
    dataset_type: Option<DatasetType>,
    date_filter: Option<DateFilter>,
    page: Option<NonZeroU32>,
    size: Option<NonZeroU32>,
}

impl GieQuery {
    /// Creates an empty query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `country`.
    ///
    /// Leading/trailing whitespace is trimmed.
    /// Empty values clear the filter.
    pub fn country(mut self, country: impl Into<String>) -> Self {
        self.country = QueryText::parse_lossy(country.into());
        self
    }

    /// Parses and sets `country` as non-empty text.
    pub fn try_country(mut self, country: impl Into<String>) -> Result<Self, GieError> {
        self.country = Some(parse_required_text_filter("country", country.into())?);
        Ok(self)
    }

    /// Sets `company`.
    ///
    /// Leading/trailing whitespace is trimmed.
    /// Empty values clear the filter.
    pub fn company(mut self, company: impl Into<String>) -> Self {
        self.company = QueryText::parse_lossy(company.into());
        self
    }

    /// Parses and sets `company` as non-empty text.
    pub fn try_company(mut self, company: impl Into<String>) -> Result<Self, GieError> {
        self.company = Some(parse_required_text_filter("company", company.into())?);
        Ok(self)
    }

    /// Sets `facility`.
    ///
    /// Leading/trailing whitespace is trimmed.
    /// Empty values clear the filter.
    pub fn facility(mut self, facility: impl Into<String>) -> Self {
        self.facility = QueryText::parse_lossy(facility.into());
        self
    }

    /// Parses and sets `facility` as non-empty text.
    pub fn try_facility(mut self, facility: impl Into<String>) -> Result<Self, GieError> {
        self.facility = Some(parse_required_text_filter("facility", facility.into())?);
        Ok(self)
    }

    /// Sets dataset `type`.
    pub fn dataset_type(mut self, dataset_type: DatasetType) -> Self {
        self.dataset_type = Some(dataset_type);
        self
    }

    /// Parses and sets dataset `type` from string (`eu`, `ne`, `ai`).
    pub fn try_dataset_type(mut self, dataset_type: impl AsRef<str>) -> Result<Self, GieError> {
        self.dataset_type = Some(
            parse_dataset_type(dataset_type.as_ref()).map_err(GieError::InvalidDatasetTypeInput)?,
        );
        Ok(self)
    }

    /// Clears dataset `type`.
    pub fn without_dataset_type(mut self) -> Self {
        self.dataset_type = None;
        self
    }

    /// Sets the single-day `date` filter.
    pub fn date(mut self, date: GieDate) -> Self {
        self.date_filter = Some(DateFilter::Day(date));
        self
    }

    /// Parses and sets the single-day `date` filter from `YYYY-MM-DD`.
    pub fn try_date(mut self, date: impl AsRef<str>) -> Result<Self, GieError> {
        self.date_filter = Some(DateFilter::Day(
            parse_date(date.as_ref()).map_err(GieError::InvalidDateInput)?,
        ));
        Ok(self)
    }

    /// Sets `from` and `to` range.
    pub fn range(mut self, from: GieDate, to: GieDate) -> Result<Self, GieError> {
        let range = DateRange::new(from, to)?;
        self.date_filter = Some(DateFilter::Range(range));
        Ok(self)
    }

    /// Parses and sets `from` and `to` from `YYYY-MM-DD`.
    pub fn try_range(self, from: impl AsRef<str>, to: impl AsRef<str>) -> Result<Self, GieError> {
        let from = parse_date(from.as_ref()).map_err(GieError::InvalidDateInput)?;
        let to = parse_date(to.as_ref()).map_err(GieError::InvalidDateInput)?;
        self.range(from, to)
    }

    /// Sets the page number.
    pub fn page(mut self, page: NonZeroU32) -> Self {
        self.page = Some(page);
        self
    }

    /// Parses and sets the page number.
    pub fn try_page(mut self, page: u32) -> Result<Self, GieError> {
        self.page = Some(NonZeroU32::new(page).ok_or_else(|| {
            GieError::InvalidPageInput("page must be greater than zero".to_string())
        })?);
        Ok(self)
    }

    /// Sets requested page size.
    pub fn size(mut self, size: NonZeroU32) -> Self {
        self.size = Some(size);
        self
    }

    /// Parses and sets requested page size.
    pub fn try_size(mut self, size: u32) -> Result<Self, GieError> {
        self.size = Some(NonZeroU32::new(size).ok_or_else(|| {
            GieError::InvalidSizeInput("size must be greater than zero".to_string())
        })?);
        Ok(self)
    }

    pub(crate) fn initial_page(&self) -> NonZeroU32 {
        self.page.unwrap_or_else(default_page)
    }

    pub(crate) fn as_params_with_page(
        &self,
        page_override: Option<NonZeroU32>,
    ) -> GieQueryParams<'_> {
        let (date, from, to) = match self.date_filter {
            Some(DateFilter::Day(value)) => (Some(value), None, None),
            Some(DateFilter::Range(value)) => (None, Some(value.from()), Some(value.to())),
            None => (None, None, None),
        };

        GieQueryParams {
            country: self.country.as_deref(),
            company: self.company.as_deref(),
            facility: self.facility.as_deref(),
            dataset_type: self.dataset_type,
            date,
            from,
            to,
            page: page_override.or(self.page),
            size: self.size,
        }
    }

    pub(crate) fn visit_debug_pairs(
        &self,
        page_override: Option<NonZeroU32>,
        mut visit: impl FnMut(&'static str, &str),
    ) {
        if let Some(value) = self.country.as_deref() {
            visit("country", value);
        }
        if let Some(value) = self.company.as_deref() {
            visit("company", value);
        }
        if let Some(value) = self.facility.as_deref() {
            visit("facility", value);
        }
        if let Some(value) = self.dataset_type {
            visit("type", value.as_str());
        }

        match self.date_filter {
            Some(DateFilter::Day(value)) => {
                let formatted_date = format_date(value);
                visit("date", &formatted_date);
            }
            Some(DateFilter::Range(value)) => {
                let from = format_date(value.from());
                let to = format_date(value.to());
                visit("from", &from);
                visit("to", &to);
            }
            None => {}
        }

        if let Some(value) = page_override.or(self.page) {
            let page = value.to_string();
            visit("page", &page);
        }
        if let Some(value) = self.size {
            let size = value.to_string();
            visit("size", &size);
        }
    }
}

fn default_page() -> NonZeroU32 {
    NonZeroU32::new(1).expect("1 is non-zero")
}

fn parse_required_text_filter(field_name: &str, value: String) -> Result<QueryText, GieError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(GieError::InvalidTextFilterInput(format!(
            "{field_name} must not be blank"
        )));
    }
    if trimmed.len() == value.len() {
        return Ok(QueryText(value));
    }
    Ok(QueryText(trimmed.to_string()))
}

#[derive(Debug, Serialize)]
pub(crate) struct GieQueryParams<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    company: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facility: Option<&'a str>,
    #[serde(
        rename = "type",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_dataset_type"
    )]
    dataset_type: Option<DatasetType>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_date"
    )]
    date: Option<GieDate>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_date"
    )]
    from: Option<GieDate>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_date"
    )]
    to: Option<GieDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page: Option<NonZeroU32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<NonZeroU32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_debug_pairs(
        query: &GieQuery,
        page_override: Option<NonZeroU32>,
    ) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        query.visit_debug_pairs(page_override, |key, value| {
            pairs.push((key, value.to_string()));
        });
        pairs
    }

    fn test_date(value: &str) -> GieDate {
        parse_date(value).unwrap()
    }

    #[test]
    fn query_params_are_mapped_to_expected_keys() {
        let query = GieQuery::new()
            .country("DE")
            .company("Comp")
            .facility("Fac")
            .dataset_type(DatasetType::Eu)
            .range(test_date("2026-03-01"), test_date("2026-03-10"))
            .unwrap()
            .try_page(2)
            .unwrap()
            .try_size(50)
            .unwrap();

        let pairs = collect_debug_pairs(&query, None);
        assert!(pairs.contains(&("country", "DE".to_string())));
        assert!(pairs.contains(&("company", "Comp".to_string())));
        assert!(pairs.contains(&("facility", "Fac".to_string())));
        assert!(pairs.contains(&("type", "eu".to_string())));
        assert!(pairs.contains(&("from", "2026-03-01".to_string())));
        assert!(pairs.contains(&("to", "2026-03-10".to_string())));
        assert!(pairs.contains(&("page", "2".to_string())));
        assert!(pairs.contains(&("size", "50".to_string())));
        assert!(!pairs.iter().any(|(key, _)| *key == "date"));
    }

    #[test]
    fn date_builder_replaces_range_filter() {
        let query = GieQuery::new()
            .range(test_date("2026-03-01"), test_date("2026-03-10"))
            .unwrap()
            .date(test_date("2026-03-10"));

        let pairs = collect_debug_pairs(&query, None);
        assert!(pairs.contains(&("date", "2026-03-10".to_string())));
        assert!(!pairs.iter().any(|(key, _)| *key == "from"));
        assert!(!pairs.iter().any(|(key, _)| *key == "to"));
    }

    #[test]
    fn try_range_rejects_invalid_order() {
        let error = GieQuery::new()
            .try_range("2026-03-10", "2026-03-01")
            .unwrap_err();

        assert!(matches!(error, GieError::InvalidDateRangeInput(_)));
    }

    #[test]
    fn try_date_rejects_invalid_input() {
        let error = GieQuery::new().try_date("2026/03/10").unwrap_err();
        assert!(matches!(error, GieError::InvalidDateInput(_)));
    }

    #[test]
    fn try_dataset_type_parses_supported_values() {
        let query = GieQuery::new().try_dataset_type("NE").unwrap();
        let pairs = collect_debug_pairs(&query, None);

        assert!(pairs.contains(&("type", "ne".to_string())));
    }

    #[test]
    fn try_dataset_type_rejects_invalid_input() {
        let error = GieQuery::new().try_dataset_type("country").unwrap_err();
        assert!(matches!(error, GieError::InvalidDatasetTypeInput(_)));
    }

    #[test]
    fn try_page_and_try_size_reject_zero() {
        assert!(matches!(
            GieQuery::new().try_page(0).unwrap_err(),
            GieError::InvalidPageInput(_)
        ));
        assert!(matches!(
            GieQuery::new().try_size(0).unwrap_err(),
            GieError::InvalidSizeInput(_)
        ));
    }

    #[test]
    fn initial_page_defaults_to_one() {
        assert_eq!(GieQuery::new().initial_page().get(), 1);
    }

    #[test]
    fn page_override_wins_in_debug_pairs() {
        let query = GieQuery::new().try_page(2).unwrap();
        let override_page = NonZeroU32::new(7).unwrap();
        let pairs = collect_debug_pairs(&query, Some(override_page));

        assert!(pairs.contains(&("page", "7".to_string())));
        assert!(!pairs.contains(&("page", "2".to_string())));
    }

    #[test]
    fn text_filters_are_trimmed_and_blank_values_are_dropped() {
        let query = GieQuery::new()
            .country(" DE ")
            .company("   ")
            .facility(" Site ");
        let pairs = collect_debug_pairs(&query, None);

        assert!(pairs.contains(&("country", "DE".to_string())));
        assert!(pairs.contains(&("facility", "Site".to_string())));
        assert!(!pairs.iter().any(|(key, _)| *key == "company"));
    }

    #[test]
    fn try_text_filters_reject_blank_values() {
        assert!(matches!(
            GieQuery::new().try_country(" ").unwrap_err(),
            GieError::InvalidTextFilterInput(_)
        ));
        assert!(matches!(
            GieQuery::new().try_company(" ").unwrap_err(),
            GieError::InvalidTextFilterInput(_)
        ));
        assert!(matches!(
            GieQuery::new().try_facility(" ").unwrap_err(),
            GieError::InvalidTextFilterInput(_)
        ));
    }

    #[test]
    fn query_text_is_trimmed_on_construction() {
        let value = QueryText::try_new("  DE  ").unwrap();
        assert_eq!(value.as_str(), "DE");
    }
}
