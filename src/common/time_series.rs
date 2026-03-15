use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::types::GieDate;

pub(crate) fn group_time_series<T, K, FK, FD>(
    rows: Vec<T>,
    make_key: FK,
    gas_day_start: FD,
) -> Vec<(K, Vec<T>)>
where
    K: Ord,
    FK: Fn(&T) -> K,
    FD: Fn(&T) -> Option<GieDate>,
{
    let mut grouped: BTreeMap<K, Vec<T>> = BTreeMap::new();

    for row in rows {
        grouped.entry(make_key(&row)).or_default().push(row);
    }

    for points in grouped.values_mut() {
        points.sort_by(|left, right| {
            compare_optional_dates(gas_day_start(left), gas_day_start(right))
        });
    }

    grouped.into_iter().collect()
}

fn compare_optional_dates(left: Option<GieDate>, right: Option<GieDate>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
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
    fn time_series_are_grouped_and_sorted_by_date() {
        #[derive(Debug, Clone)]
        struct Probe {
            key: &'static str,
            gas_day_start: Option<GieDate>,
            value: u32,
        }

        let rows = vec![
            Probe {
                key: "A",
                gas_day_start: Some(test_date("2026-03-03")),
                value: 3,
            },
            Probe {
                key: "B",
                gas_day_start: Some(test_date("2026-03-02")),
                value: 2,
            },
            Probe {
                key: "A",
                gas_day_start: Some(test_date("2026-03-01")),
                value: 1,
            },
            Probe {
                key: "A",
                gas_day_start: None,
                value: 4,
            },
        ];

        let grouped = group_time_series(rows, |row| row.key, |row| row.gas_day_start);

        assert_eq!(grouped.len(), 2);
        assert_eq!(
            grouped[0].1.iter().map(|row| row.value).collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
        assert_eq!(
            grouped[1].1.iter().map(|row| row.value).collect::<Vec<_>>(),
            vec![2]
        );
    }
}
