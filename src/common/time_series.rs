use std::cmp::Ordering;
#[cfg(test)]
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
    group_time_series_presorted(rows, make_key, gas_day_start)
}

fn group_time_series_presorted<T, K, FK, FD>(
    rows: Vec<T>,
    make_key: FK,
    gas_day_start: FD,
) -> Vec<(K, Vec<T>)>
where
    K: Ord,
    FK: Fn(&T) -> K,
    FD: Fn(&T) -> Option<GieDate>,
{
    let mut keyed_rows: Vec<(K, Option<GieDate>, T)> = rows
        .into_iter()
        .map(|row| (make_key(&row), gas_day_start(&row), row))
        .collect();

    keyed_rows.sort_by(|(left_key, left_day, _), (right_key, right_day, _)| {
        left_key
            .cmp(right_key)
            .then_with(|| compare_optional_dates(*left_day, *right_day))
    });

    let mut grouped: Vec<(K, Vec<T>)> = Vec::new();
    for (key, _, row) in keyed_rows {
        if let Some((last_key, points)) = grouped.last_mut()
            && last_key == &key
        {
            points.push(row);
            continue;
        }
        grouped.push((key, vec![row]));
    }

    grouped
}

#[cfg(test)]
fn group_time_series_btree<T, K, FK, FD>(
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
    use std::time::Instant;

    use super::*;
    use crate::common::types::parse_date;

    fn test_date(value: &str) -> GieDate {
        parse_date(value).unwrap()
    }

    #[test]
    fn time_series_are_grouped_and_sorted_by_date() {
        #[derive(Debug, Clone, PartialEq, Eq)]
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

    #[test]
    fn presorted_strategy_matches_btree_strategy() {
        #[derive(Debug, Clone, PartialEq, Eq)]
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
            Probe {
                key: "B",
                gas_day_start: None,
                value: 5,
            },
        ];

        let presorted = group_time_series(rows.clone(), |row| row.key, |row| row.gas_day_start);
        let btree = group_time_series_btree(rows, |row| row.key, |row| row.gas_day_start);

        assert_eq!(presorted, btree);
    }

    #[test]
    #[ignore]
    fn benchmark_grouping_strategies() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct Probe {
            key: u16,
            gas_day_start: Option<GieDate>,
            value: u32,
        }

        let mut rows = Vec::with_capacity(100_000);
        for index in 0_u32..100_000_u32 {
            let day = 1 + (index % 28);
            let date = parse_date(&format!("2026-03-{day:02}")).ok();

            rows.push(Probe {
                key: u16::try_from(index % 300).expect("key is in range"),
                gas_day_start: date,
                value: index,
            });
        }

        let started = Instant::now();
        let _ = group_time_series(rows.clone(), |row| row.key, |row| row.gas_day_start);
        let presorted_elapsed = started.elapsed();

        let started = Instant::now();
        let _ = group_time_series_btree(rows, |row| row.key, |row| row.gas_day_start);
        let btree_elapsed = started.elapsed();

        eprintln!(
            "group_time_series benchmark: presorted={presorted_elapsed:?}, btree={btree_elapsed:?}"
        );
    }
}
