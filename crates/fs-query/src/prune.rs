//! File-level pruning from query filters.
//!
//! Only conjunctions of simple comparisons are understood. Anything else is
//! ignored, which keeps pruning conservative: a file is dropped only when a
//! recognised predicate proves it cannot match.

use chrono::{DateTime, TimeZone, Utc};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use fs_catalog::FileRecord;
use fs_schema::spans::col;

/// What the filters say about the scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pruning {
    /// Inclusive lower bound on `timestamp`, nanoseconds.
    pub ts_min: Option<i64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds.
    pub ts_max: Option<i64>,
    /// Allowed service names, when the filters pin them.
    pub services: Option<Vec<String>>,
    /// Trace ids the filters pin, as 16-byte values.
    pub trace_ids: Option<Vec<Vec<u8>>>,
    /// Span ids the filters pin, as 8-byte values.
    pub span_ids: Option<Vec<Vec<u8>>>,
}

/// Bucket width in seconds as a signed value for timestamp arithmetic. 1800 fits trivially.
#[allow(clippy::cast_possible_wrap)]
const BUCKET_SECONDS_I64: i64 = fs_schema::bucket::BUCKET_SECONDS as i64;

impl Pruning {
    /// Derives bounds from `filters` (implicitly ANDed).
    pub fn from_filters(filters: &[Expr]) -> Self {
        let mut out = Self::default();
        for filter in filters {
            out.absorb(filter);
        }
        out
    }

    fn absorb(&mut self, expr: &Expr) {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                self.absorb(left);
                self.absorb(right);
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if let (Some(column), Some(literal)) = (column_name(left), literal(right)) {
                    self.compare(column, *op, literal);
                } else if let (Some(literal), Some(column)) = (literal(left), column_name(right)) {
                    if let Some(flipped) = op.swap() {
                        self.compare(column, flipped, literal);
                    }
                }
            }
            Expr::InList(list)
                if !list.negated && column_name(&list.expr) == Some("service_name") =>
            {
                let values: Option<Vec<String>> = list
                    .list
                    .iter()
                    .map(|e| literal(e).and_then(string_value))
                    .collect();
                if let Some(values) = values {
                    self.restrict_services(values);
                }
            }
            Expr::InList(list)
                if !list.negated
                    && matches!(column_name(&list.expr), Some(col::TRACE_ID | col::SPAN_ID)) =>
            {
                let values: Option<Vec<Vec<u8>>> = list
                    .list
                    .iter()
                    .map(|e| literal(e).and_then(binary_value))
                    .collect();
                if let (Some(values), Some(column)) = (values, column_name(&list.expr)) {
                    self.restrict_ids(column, values);
                }
            }
            _ => {}
        }
    }

    fn compare(&mut self, column: &str, op: Operator, literal: &ScalarValue) {
        match column {
            col::TIMESTAMP => {
                if let Some(ns) = timestamp_nanos(literal) {
                    self.bound(op, ns);
                }
            }
            col::TS_BUCKET_START => {
                if let Some(seconds) = integer(literal) {
                    // A bucket predicate bounds the timestamp at bucket granularity.
                    match op {
                        Operator::Gt | Operator::GtEq => {
                            self.bound(Operator::GtEq, seconds.saturating_mul(1_000_000_000));
                        }
                        Operator::Lt | Operator::LtEq => self.bound(
                            Operator::LtEq,
                            seconds
                                .saturating_add(BUCKET_SECONDS_I64)
                                .saturating_mul(1_000_000_000),
                        ),
                        Operator::Eq => {
                            self.bound(Operator::GtEq, seconds.saturating_mul(1_000_000_000));
                            self.bound(
                                Operator::LtEq,
                                seconds
                                    .saturating_add(BUCKET_SECONDS_I64)
                                    .saturating_mul(1_000_000_000),
                            );
                        }
                        _ => {}
                    }
                }
            }
            "service_name" if op == Operator::Eq => {
                if let Some(service) = string_value(literal) {
                    self.restrict_services(vec![service]);
                }
            }
            col::TRACE_ID | col::SPAN_ID if op == Operator::Eq => {
                if let Some(id) = binary_value(literal) {
                    self.restrict_ids(column, vec![id]);
                }
            }
            _ => {}
        }
    }

    /// Narrows the pinned ids of `column` to `values`, keeping the
    /// intersection with ids pinned earlier.
    fn restrict_ids(&mut self, column: &str, values: Vec<Vec<u8>>) {
        let slot = if column == col::TRACE_ID {
            &mut self.trace_ids
        } else {
            &mut self.span_ids
        };
        *slot = Some(match slot.take() {
            None => values,
            Some(existing) => existing
                .into_iter()
                .filter(|v| values.contains(v))
                .collect(),
        });
    }

    fn bound(&mut self, op: Operator, ns: i64) {
        match op {
            Operator::Gt | Operator::GtEq => {
                self.ts_min = Some(self.ts_min.map_or(ns, |m| m.max(ns)));
            }
            Operator::Lt | Operator::LtEq => {
                self.ts_max = Some(self.ts_max.map_or(ns, |m| m.min(ns)));
            }
            Operator::Eq => {
                self.bound(Operator::GtEq, ns);
                self.bound(Operator::LtEq, ns);
            }
            _ => {}
        }
    }

    fn restrict_services(&mut self, values: Vec<String>) {
        self.services = Some(match self.services.take() {
            None => values,
            Some(existing) => existing
                .into_iter()
                .filter(|s| values.contains(s))
                .collect(),
        });
    }

    /// True when `file` may hold matching rows.
    pub fn keeps(&self, file: &FileRecord) -> bool {
        if let Some(min) = self.ts_min {
            if file.range.end < nanos_to_datetime(min) {
                return false;
            }
        }
        if let Some(max) = self.ts_max {
            if file.range.start > nanos_to_datetime(max) {
                return false;
            }
        }
        if let Some(services) = &self.services {
            // An empty service list on the file means the writer did not record it.
            if !file.services.is_empty() && !file.services.iter().any(|s| services.contains(s)) {
                return false;
            }
        }
        true
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Cast(cast) => column_name(&cast.expr),
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    }
}

fn string_value(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        ScalarValue::Dictionary(_, inner) => string_value(inner),
        _ => None,
    }
}

fn binary_value(v: &ScalarValue) -> Option<Vec<u8>> {
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b.clone()),
        ScalarValue::Dictionary(_, inner) => binary_value(inner),
        _ => None,
    }
}

fn integer(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Int64(Some(i)) => Some(*i),
        ScalarValue::Int32(Some(i)) => Some(i64::from(*i)),
        ScalarValue::UInt64(Some(u)) => i64::try_from(*u).ok(),
        ScalarValue::UInt32(Some(u)) => Some(i64::from(*u)),
        _ => None,
    }
}

fn timestamp_nanos(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampNanosecond(Some(ns), _) => Some(*ns),
        ScalarValue::TimestampMicrosecond(Some(us), _) => us.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(ms), _) => ms.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(s), _) => s.checked_mul(1_000_000_000),
        ScalarValue::Utf8(Some(text)) => DateTime::parse_from_rfc3339(text)
            .ok()
            .and_then(|t| t.timestamp_nanos_opt()),
        _ => None,
    }
}

fn nanos_to_datetime(ns: i64) -> DateTime<Utc> {
    Utc.timestamp_nanos(ns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col as dfcol, lit};
    use fs_catalog::{FileId, FileState, Level, Signal, TenantId, TimeRange};

    fn file(start: i64, end: i64, services: &[&str]) -> FileRecord {
        FileRecord {
            id: FileId::new(),
            tenant_id: TenantId::new(),
            signal: Signal::Traces,
            level: Level::L0,
            path: String::new(),
            size_bytes: 1,
            row_count: 1,
            range: TimeRange {
                start: nanos_to_datetime(start),
                end: nanos_to_datetime(end),
            },
            services: services.iter().map(|s| (*s).to_owned()).collect(),
            summary: None,
            state: FileState::Visible,
            created_at: Utc::now(),
            quarantined_at: None,
        }
    }

    fn ts(ns: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(
            Some(ns),
            Some("UTC".into()),
        ))
    }

    #[test]
    fn time_bounds_and_services_prune() {
        let filters = vec![
            dfcol("timestamp").gt_eq(ts(100)),
            dfcol("timestamp")
                .lt(ts(200))
                .and(dfcol("service_name").eq(lit("api"))),
        ];
        let pruning = Pruning::from_filters(&filters);
        assert_eq!(
            pruning,
            Pruning {
                ts_min: Some(100),
                ts_max: Some(200),
                services: Some(vec!["api".into()]),
                trace_ids: None,
                span_ids: None,
            }
        );
        assert!(pruning.keeps(&file(150, 160, &["api"])));
        assert!(
            pruning.keeps(&file(50, 100, &["api", "worker"])),
            "touching the lower bound"
        );
        assert!(!pruning.keeps(&file(201, 300, &["api"])));
        assert!(!pruning.keeps(&file(0, 99, &["api"])));
        assert!(!pruning.keeps(&file(150, 160, &["worker"])));
        assert!(
            pruning.keeps(&file(150, 160, &[])),
            "unknown services never prune"
        );
    }

    #[test]
    fn literal_on_the_left_and_in_lists() {
        let filters = vec![
            ts(500).lt_eq(dfcol("timestamp")),
            dfcol("service_name").in_list(vec![lit("a"), lit("b")], false),
        ];
        let pruning = Pruning::from_filters(&filters);
        assert_eq!(pruning.ts_min, Some(500));
        assert_eq!(pruning.services, Some(vec!["a".into(), "b".into()]));
        assert!(!pruning.keeps(&file(0, 100, &["a"])));
        assert!(!pruning.keeps(&file(600, 700, &["c"])));
    }

    #[test]
    fn trace_and_span_ids_are_pinned() {
        let filters = vec![
            dfcol("trace_id").eq(lit(ScalarValue::Binary(Some(vec![7u8; 16])))),
            dfcol("span_id").in_list(
                vec![
                    lit(ScalarValue::FixedSizeBinary(8, Some(vec![1u8; 8]))),
                    lit(ScalarValue::Binary(Some(vec![2u8; 8]))),
                ],
                false,
            ),
        ];
        let p = Pruning::from_filters(&filters);
        assert_eq!(p.trace_ids, Some(vec![vec![7u8; 16]]));
        assert_eq!(p.span_ids, Some(vec![vec![1u8; 8], vec![2u8; 8]]));

        let narrowed = Pruning::from_filters(&[
            dfcol("trace_id").eq(lit(ScalarValue::Binary(Some(vec![7u8; 16])))),
            dfcol("trace_id").eq(lit(ScalarValue::Binary(Some(vec![8u8; 16])))),
        ]);
        assert_eq!(
            narrowed.trace_ids,
            Some(vec![]),
            "two different ids pin nothing"
        );
    }

    #[test]
    fn unknown_predicates_do_not_prune() {
        let filters = vec![
            dfcol("name").eq(lit("x")),
            dfcol("service_name").not_eq(lit("api")),
        ];
        let pruning = Pruning::from_filters(&filters);
        assert_eq!(pruning, Pruning::default());
        assert!(pruning.keeps(&file(0, 1, &["api"])));
    }

    #[test]
    fn bucket_predicates_widen_to_bucket_edges() {
        let filters = vec![
            dfcol("ts_bucket_start").gt_eq(lit(1800_i64)),
            dfcol("ts_bucket_start").lt_eq(lit(3600_i64)),
        ];
        let pruning = Pruning::from_filters(&filters);
        assert_eq!(pruning.ts_min, Some(1_800_000_000_000));
        assert_eq!(pruning.ts_max, Some(5_400_000_000_000));
    }
}
