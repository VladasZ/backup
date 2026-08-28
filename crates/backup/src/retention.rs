//! Milestone keeping on top of the configured retention rule.
//!
//! Besides the newest archives the count or age rule keeps, every job always
//! keeps one archive per age bucket: 1w-2w, 2w-1m, 1m-2m, 2m-3m, 3m-6m,
//! 6m-1y, and then one per year forever. The oldest archive in a bucket is
//! the keeper, so it slides into the next bucket and hands over without
//! leaving a bucket empty for long. Keepers are recomputed from the archive
//! listing on every prune, no extra state is stored.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};

const DAY: i64 = 24 * 60 * 60;
const YEAR_DAYS: i64 = 365;
const BUCKET_STARTS_DAYS: [i64; 7] = [7, 14, 30, 60, 90, 180, YEAR_DAYS];

/// The age bucket an archive of this age falls into, or `None` when it is
/// younger than one week and only the configured rule applies to it.
fn milestone_bucket(age: Duration) -> Option<i64> {
    let days = age.num_seconds() / DAY;
    if days < BUCKET_STARTS_DAYS[0] {
        return None;
    }
    if days >= YEAR_DAYS {
        let yearly_start = BUCKET_STARTS_DAYS.len() as i64 - 1;
        return Some(yearly_start + (days - YEAR_DAYS) / YEAR_DAYS);
    }
    let mut bucket = 0;
    for (index, start) in BUCKET_STARTS_DAYS.iter().enumerate() {
        if days >= *start {
            bucket = index as i64;
        }
    }
    Some(bucket)
}

/// Indices into `created` of the archives milestone keeping protects.
/// Each entry of `created` is one archive's creation time.
pub fn milestone_keepers(created: &[DateTime<Utc>], now: DateTime<Utc>) -> HashSet<usize> {
    let mut keepers: HashMap<i64, usize> = HashMap::new();
    for (index, archive) in created.iter().enumerate() {
        let Some(bucket) = milestone_bucket(now - *archive) else {
            continue;
        };
        keepers
            .entry(bucket)
            .and_modify(|keeper| {
                if *archive < created[*keeper] {
                    *keeper = index;
                }
            })
            .or_insert(index);
    }
    keepers.into_values().collect()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{milestone_bucket, milestone_keepers};

    #[test]
    fn ages_map_to_the_documented_buckets() {
        let bucket = |days| milestone_bucket(Duration::days(days));
        assert_eq!(bucket(0), None);
        assert_eq!(bucket(6), None);
        assert_eq!(bucket(7), Some(0));
        assert_eq!(bucket(13), Some(0));
        assert_eq!(bucket(14), Some(1));
        assert_eq!(bucket(29), Some(1));
        assert_eq!(bucket(30), Some(2));
        assert_eq!(bucket(59), Some(2));
        assert_eq!(bucket(60), Some(3));
        assert_eq!(bucket(89), Some(3));
        assert_eq!(bucket(90), Some(4));
        assert_eq!(bucket(179), Some(4));
        assert_eq!(bucket(180), Some(5));
        assert_eq!(bucket(364), Some(5));
        assert_eq!(bucket(365), Some(6));
        assert_eq!(bucket(729), Some(6));
        assert_eq!(bucket(730), Some(7));
        assert_eq!(bucket(3 * 365), Some(8));
        assert_eq!(bucket(10 * 365), Some(15));
    }

    #[test]
    fn the_oldest_archive_in_each_bucket_is_the_keeper() {
        let now = Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0).unwrap();
        let ages_days = [0, 1, 6, 8, 10, 20, 40, 100, 400, 500, 800];
        let created: Vec<_> = ages_days
            .iter()
            .map(|days| now - Duration::days(*days))
            .collect();

        let keepers = milestone_keepers(&created, now);

        // 10 beats 8 in the 1w-2w bucket and 500 beats 400 in the 1y-2y
        // bucket, since the oldest archive in a bucket wins.
        let kept_ages: Vec<_> = {
            let mut ages: Vec<_> = keepers
                .iter()
                .map(|index| (now - created[*index]).num_days())
                .collect();
            ages.sort_unstable();
            ages
        };
        assert_eq!(kept_ages, [10, 20, 40, 100, 500, 800]);
    }

    #[test]
    fn young_archives_are_never_keepers() {
        let now = Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0).unwrap();
        let created = [now, now - Duration::days(3), now - Duration::days(6)];
        assert!(milestone_keepers(&created, now).is_empty());
    }
}
