use std::cmp::Ordering;
#[cfg(test)]
use std::collections::BTreeMap;

use super::types::GieDate;

pub(crate) fn group_time_series<T, K, FC, FK, FD>(
    mut rows: Vec<T>,
    compare_keys: FC,
    make_key: FK,
    gas_day_start: FD,
) -> Vec<(K, Vec<T>)>
where
    FC: Fn(&T, &T) -> Ordering,
    FK: Fn(&T) -> K,
    FD: Fn(&T) -> Option<GieDate>,
{
    rows.sort_by(|left, right| {
        compare_keys(left, right)
            .then_with(|| compare_optional_dates(gas_day_start(left), gas_day_start(right)))
    });

    let mut grouped: Vec<(K, Vec<T>)> = Vec::new();
    for row in rows {
        if let Some((_, points)) = grouped.last_mut()
            && let Some(last_row) = points.last()
            && compare_keys(last_row, &row).is_eq()
        {
            points.push(row);
            continue;
        }
        grouped.push((make_key(&row), vec![row]));
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

        let grouped = group_time_series(
            rows,
            |left, right| left.key.cmp(right.key),
            |row| row.key,
            |row| row.gas_day_start,
        );

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

        let presorted = group_time_series(
            rows.clone(),
            |left, right| left.key.cmp(right.key),
            |row| row.key,
            |row| row.gas_day_start,
        );
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
        let _ = group_time_series(
            rows.clone(),
            |left, right| left.key.cmp(&right.key),
            |row| row.key,
            |row| row.gas_day_start,
        );
        let presorted_elapsed = started.elapsed();

        let started = Instant::now();
        let _ = group_time_series_btree(rows, |row| row.key, |row| row.gas_day_start);
        let btree_elapsed = started.elapsed();

        eprintln!(
            "group_time_series benchmark: presorted={presorted_elapsed:?}, btree={btree_elapsed:?}"
        );
    }
}
