use std::num::NonZeroU32;

use serde::Serialize;

use crate::error::GieError;

use super::date_range::DateRange;
use super::serde_ext::{serialize_optional_dataset_type, serialize_optional_date};
use super::types::{DatasetType, DateFilter, GieDate, format_date, parse_dataset_type, parse_date};

/// Query builder shared by AGSI and ALSI endpoints.
#[derive(Debug, Clone, Default)]
pub struct GieQuery {
    country: Option<String>,
    company: Option<String>,
    facility: Option<String>,
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
    pub fn country(mut self, country: impl Into<String>) -> Self {
        self.country = Some(country.into());
        self
    }

    /// Sets `company`.
    pub fn company(mut self, company: impl Into<String>) -> Self {
        self.company = Some(company.into());
        self
    }

    /// Sets `facility`.
    pub fn facility(mut self, facility: impl Into<String>) -> Self {
        self.facility = Some(facility.into());
        self
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

    pub(crate) fn to_debug_pairs(
        &self,
        page_override: Option<NonZeroU32>,
    ) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();

        if let Some(value) = &self.country {
            pairs.push(("country", value.clone()));
        }
        if let Some(value) = &self.company {
            pairs.push(("company", value.clone()));
        }
        if let Some(value) = &self.facility {
            pairs.push(("facility", value.clone()));
        }
        if let Some(value) = self.dataset_type {
            pairs.push(("type", value.to_string()));
        }

        match self.date_filter {
            Some(DateFilter::Day(value)) => pairs.push(("date", format_date(value))),
            Some(DateFilter::Range(value)) => {
                pairs.push(("from", format_date(value.from())));
                pairs.push(("to", format_date(value.to())));
            }
            None => {}
        }

        if let Some(value) = page_override.or(self.page) {
            pairs.push(("page", value.to_string()));
        }
        if let Some(value) = self.size {
            pairs.push(("size", value.to_string()));
        }

        pairs
    }
}

fn default_page() -> NonZeroU32 {
    NonZeroU32::new(1).expect("1 is non-zero")
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

        let pairs = query.to_debug_pairs(None);
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

        let pairs = query.to_debug_pairs(None);
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
        let pairs = query.to_debug_pairs(None);

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
}
