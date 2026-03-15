use std::ops::RangeInclusive;

use crate::error::GieError;

use super::types::{GieDate, format_date};

/// Inclusive date range used in query filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateRange {
    start: GieDate,
    end: GieDate,
}

impl DateRange {
    /// Creates a validated date range (`start <= end`).
    pub fn new(start: GieDate, end: GieDate) -> Result<Self, GieError> {
        if start <= end {
            Ok(Self { start, end })
        } else {
            Err(GieError::InvalidDateRangeInput(format!(
                "from must be less than or equal to to (from={}, to={})",
                format_date(start),
                format_date(end)
            )))
        }
    }

    /// Inclusive range start.
    pub fn start(self) -> GieDate {
        self.start
    }

    /// Inclusive range end.
    pub fn end(self) -> GieDate {
        self.end
    }

    /// Alias for [`Self::start`] kept for compatibility with `from` query parameter naming.
    pub fn from(self) -> GieDate {
        self.start()
    }

    /// Alias for [`Self::end`] kept for compatibility with `to` query parameter naming.
    pub fn to(self) -> GieDate {
        self.end()
    }

    /// Returns `true` when `date` is inside this range.
    pub fn contains(self, date: GieDate) -> bool {
        self.start <= date && date <= self.end
    }

    /// Returns `true` when ranges share at least one date.
    pub fn intersects(self, other: Self) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Returns `true` when this range covers exactly one day.
    pub fn is_single_day(self) -> bool {
        self.start == self.end
    }

    /// Converts range into `(start, end)` bounds.
    pub fn into_bounds(self) -> (GieDate, GieDate) {
        (self.start, self.end)
    }

    /// Returns the standard inclusive range representation.
    pub fn as_inclusive(self) -> RangeInclusive<GieDate> {
        self.start..=self.end
    }
}

impl TryFrom<(GieDate, GieDate)> for DateRange {
    type Error = GieError;

    fn try_from(value: (GieDate, GieDate)) -> Result<Self, Self::Error> {
        Self::new(value.0, value.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::parse_date;

    fn test_date(value: &str) -> GieDate {
        parse_date(value).unwrap()
    }

    #[test]
    fn accepts_valid_bounds() {
        let range = DateRange::new(test_date("2026-03-01"), test_date("2026-03-10")).unwrap();

        assert_eq!(range.start(), test_date("2026-03-01"));
        assert_eq!(range.end(), test_date("2026-03-10"));
    }

    #[test]
    fn rejects_invalid_bounds() {
        let error = DateRange::new(test_date("2026-03-10"), test_date("2026-03-01")).unwrap_err();
        assert!(matches!(error, GieError::InvalidDateRangeInput(_)));
    }

    #[test]
    fn keeps_from_to_aliases() {
        let range = DateRange::new(test_date("2026-03-01"), test_date("2026-03-10")).unwrap();

        assert_eq!(range.from(), test_date("2026-03-01"));
        assert_eq!(range.to(), test_date("2026-03-10"));
    }

    #[test]
    fn detects_contains_correctly() {
        let range = DateRange::new(test_date("2026-03-01"), test_date("2026-03-10")).unwrap();

        assert!(range.contains(test_date("2026-03-01")));
        assert!(range.contains(test_date("2026-03-10")));
        assert!(range.contains(test_date("2026-03-05")));
        assert!(!range.contains(test_date("2026-02-28")));
        assert!(!range.contains(test_date("2026-03-11")));
    }

    #[test]
    fn detects_intersection_correctly() {
        let left = DateRange::new(test_date("2026-03-01"), test_date("2026-03-10")).unwrap();
        let overlap = DateRange::new(test_date("2026-03-10"), test_date("2026-03-15")).unwrap();
        let disjoint = DateRange::new(test_date("2026-03-11"), test_date("2026-03-20")).unwrap();

        assert!(left.intersects(overlap));
        assert!(!left.intersects(disjoint));
    }

    #[test]
    fn exposes_shape_helpers() {
        let single = DateRange::new(test_date("2026-03-05"), test_date("2026-03-05")).unwrap();
        let multiple = DateRange::new(test_date("2026-03-01"), test_date("2026-03-10")).unwrap();

        assert!(single.is_single_day());
        assert!(!multiple.is_single_day());
        assert_eq!(
            multiple.into_bounds(),
            (test_date("2026-03-01"), test_date("2026-03-10"))
        );
    }

    #[test]
    fn can_be_built_via_try_from_tuple() {
        let range =
            DateRange::try_from((test_date("2026-03-01"), test_date("2026-03-10"))).unwrap();

        assert_eq!(range.start(), test_date("2026-03-01"));
        assert_eq!(range.end(), test_date("2026-03-10"));
    }
}
