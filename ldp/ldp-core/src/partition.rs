//! Partition model and the range-partition planning algorithm.
//!
//! The algorithm is **specified** in `docs/LDP_Phase1_Operational_Details.md`
//! §1 and is reproduced here exactly, because restart correctness depends on
//! two runs computing identical partitions. Do not "improve" it.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub id: u64,
    /// inclusive
    pub start: Option<i64>,
    /// exclusive
    pub end: Option<i64>,
}

impl Partition {
    pub fn single(id: u64) -> Self {
        Self {
            id,
            start: None,
            end: None,
        }
    }

    pub fn range(id: u64, start: i64, end: i64) -> Self {
        Self {
            id,
            start: Some(start),
            end: Some(end),
        }
    }

    /// True when this partition reads unconstrained rows (`partitioning: none`).
    pub fn is_unbounded(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }
}

/// Split `[min, max]` (inclusive bounds from `MIN()`/`MAX()`) into at most
/// `chunks` half-open partitions.
///
/// ```
/// use ldp_core::partition::plan_range;
/// let parts = plan_range(1, 100, 4);
/// assert_eq!(parts.len(), 4);
/// assert_eq!(parts[0], ldp_core::Partition::range(0, 1, 26));
/// ```
pub fn plan_range(min: i64, max: i64, chunks: usize) -> Vec<Partition> {
    if chunks == 0 || min > max {
        return Vec::new();
    }
    // integer division, at least 1 — Operational Details §1 step 3
    let span = (max as i128) - (min as i128);
    let chunk_size = ((span / chunks as i128) + 1).max(1);
    let upper = (max as i128) + 1; // exclusive end of the last partition
    let mut out = Vec::with_capacity(chunks);
    for i in 0..chunks {
        let start = (min as i128) + (i as i128) * chunk_size;
        if start > i64::MAX as i128 {
            break;
        }
        let end = (start + chunk_size).min(upper);
        if start < end {
            out.push(Partition::range(out.len() as u64, start as i64, end as i64));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_evenly() {
        let p = plan_range(1, 100, 4);
        assert_eq!(p.len(), 4);
        assert_eq!((p[0].start, p[0].end), (Some(1), Some(26)));
        assert_eq!((p[3].start, p[3].end), (Some(76), Some(101)));
    }

    #[test]
    fn single_row_table_is_one_partition() {
        let p = plan_range(7, 7, 8);
        assert_eq!(p.len(), 1);
        assert_eq!((p[0].start, p[0].end), (Some(7), Some(8)));
    }

    #[test]
    fn more_chunks_than_rows_never_overlaps_or_skips() {
        let p = plan_range(1, 5, 100);
        // every integer in 1..=5 appears exactly once
        let mut seen = Vec::new();
        for part in &p {
            for v in part.start.unwrap()..part.end.unwrap() {
                seen.push(v);
            }
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn covers_full_range_with_no_gaps() {
        for (lo, hi, n) in [(0i64, 1000i64, 7usize), (-50, 50, 3), (1, 2, 2)] {
            let p = plan_range(lo, hi, n);
            let mut expect = lo;
            for part in &p {
                assert_eq!(part.start.unwrap(), expect);
                expect = part.end.unwrap();
            }
            assert_eq!(expect, hi + 1, "gap at end for {lo}..{hi}/{n}");
        }
    }

    #[test]
    fn negates_and_extremes_do_not_overflow() {
        let p = plan_range(i64::MIN + 1, i64::MAX - 1, 3);
        assert!(!p.is_empty());
        assert!(p.iter().all(|x| x.start < x.end));
    }

    #[test]
    fn empty_bounds_produce_no_partitions() {
        assert!(plan_range(0, -1, 4).is_empty());
        assert!(plan_range(0, 10, 0).is_empty());
    }
}
