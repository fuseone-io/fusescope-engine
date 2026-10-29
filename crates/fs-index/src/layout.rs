//! The bucket layout of span files and their sidecars, parsed strictly:
//! anything the engine would not write is not the engine's.
//!
//! ```text
//! t/{tenant uuid}/traces/l{0|1|2}/dt=YYYY-MM-DD/hr=HH/{file uuid}.parquet
//! t/{tenant uuid}/traces/index/trace_id/dt=YYYY-MM-DD/hr=HH/{file uuid}.tix
//! t/{tenant uuid}/derived/attribute_keys/dt=YYYY-MM-DD/hr=HH/{file uuid}.parquet
//! ```
//!
//! A derived table is partitioned by the hour of the compaction pass that
//! rebuilt it.

/// What an object under the engine's prefix is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanObject {
    /// Tenant id, as written in the path.
    pub tenant: String,
    /// Compaction level of a span file; `None` for a sidecar.
    pub level: Option<u8>,
    /// `dt=YYYY-MM-DD/hr=HH`.
    pub partition: String,
    /// File id, as written in the path.
    pub id: String,
    /// True for a `trace_id` sidecar.
    pub sidecar: bool,
    /// True for a derived table file.
    pub derived: bool,
}

impl SpanObject {
    /// Parses a path relative to the engine root. `None` for anything the
    /// engine does not lay out: another prefix, another signal, a level it
    /// does not write, a malformed partition or id, another extension.
    pub fn parse(path: &str) -> Option<Self> {
        let mut parts = path.split('/');
        if parts.next()? != "t" {
            return None;
        }
        let tenant = parts.next()?;
        if !is_uuid(tenant) {
            return None;
        }
        let (level, sidecar, derived) = match parts.next()? {
            "traces" => match parts.next()? {
                "l0" => (Some(0), false, false),
                "l1" => (Some(1), false, false),
                "l2" => (Some(2), false, false),
                "index" => {
                    if parts.next()? != "trace_id" {
                        return None;
                    }
                    (None, true, false)
                }
                _ => return None,
            },
            "derived" => {
                if parts.next()? != "attribute_keys" {
                    return None;
                }
                (None, false, true)
            }
            _ => return None,
        };
        let date = parts.next()?;
        let hour = parts.next()?;
        if !is_date(date) || !is_hour(hour) {
            return None;
        }
        let file = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        let (id, ext) = file.rsplit_once('.')?;
        let expected = if sidecar { "tix" } else { "parquet" };
        if ext != expected || !is_uuid(id) {
            return None;
        }
        Some(Self {
            tenant: tenant.to_owned(),
            level,
            partition: format!("{date}/{hour}"),
            id: id.to_owned(),
            sidecar,
            derived,
        })
    }
}

/// `8-4-4-4-12` lowercase hex.
fn is_uuid(s: &str) -> bool {
    let groups = [8, 4, 4, 4, 12];
    let mut parts = s.split('-');
    for want in groups {
        let Some(part) = parts.next() else {
            return false;
        };
        if part.len() != want
            || !part
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return false;
        }
    }
    parts.next().is_none()
}

/// `dt=YYYY-MM-DD`, a date that exists.
fn is_date(s: &str) -> bool {
    let Some(d) = s.strip_prefix("dt=") else {
        return false;
    };
    let b = d.as_bytes();
    let shaped = b.len() == 10
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        });
    if !shaped {
        return false;
    }
    let number = |range: std::ops::Range<usize>| -> Option<u32> { d.get(range)?.parse().ok() };
    let (Some(year), Some(month), Some(day)) = (number(0..4), number(5..7), number(8..10)) else {
        return false;
    };
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days_in_month).contains(&day)
}

/// `hr=HH`.
fn is_hour(s: &str) -> bool {
    let Some(h) = s.strip_prefix("hr=") else {
        return false;
    };
    h.len() == 2 && h.bytes().all(|c| c.is_ascii_digit()) && h <= "23"
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "0192f1b2-3c4d-7e5f-8a9b-0c1d2e3f4a5b";
    const F: &str = "0192f1b2-3c4d-7e5f-8a9b-0c1d2e3f4a5c";

    #[test]
    fn the_engine_layout_parses() {
        let file =
            SpanObject::parse(&format!("t/{T}/traces/l1/dt=2026-10-24/hr=14/{F}.parquet")).unwrap();
        assert_eq!(file.level, Some(1));
        assert!(!file.sidecar);
        assert_eq!(file.partition, "dt=2026-10-24/hr=14");
        assert_eq!(file.id, F);
        assert!(
            SpanObject::parse(&format!("t/{T}/traces/l0/dt=2024-02-29/hr=23/{F}.parquet"))
                .is_some(),
            "leap day"
        );
        let tix = SpanObject::parse(&format!(
            "t/{T}/traces/index/trace_id/dt=2026-10-24/hr=14/{F}.tix"
        ))
        .unwrap();
        assert!(tix.sidecar);
        assert_eq!(tix.level, None);
        assert_eq!(tix.tenant, T);
        assert!(!tix.derived);
        let keys = SpanObject::parse(&format!(
            "t/{T}/derived/attribute_keys/dt=2026-10-24/hr=14/{F}.parquet"
        ))
        .unwrap();
        assert!(keys.derived);
        assert!(!keys.sidecar);
        assert_eq!(keys.level, None);
        assert_eq!(keys.partition, "dt=2026-10-24/hr=14");
    }

    #[test]
    fn anything_else_fails_closed() {
        for path in [
            "t/notes.parquet",
            "t/notes.txt",
            &format!("t/{T}/logs/l0/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l3/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=24/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-1-24/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-99-99/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-02-29/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-04-31/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-00-10/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/{F}.tix"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/notes.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/{F}.parquet/extra"),
            &format!("t/{T}/traces/index/trace_id/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/index/other/dt=2026-10-24/hr=14/{F}.tix"),
            &format!("t/{T}/derived/other/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!("t/{T}/derived/attribute_keys/dt=2026-10-24/hr=14/{F}.tix"),
            &format!("t/{T}/derived/attribute_keys/{F}.parquet"),
            &format!("x/{T}/traces/l0/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!(
                "t/{}/traces/l0/dt=2026-10-24/hr=14/{F}.parquet",
                T.to_uppercase()
            ),
            &format!("t/not-a-uuid/traces/l0/dt=2026-10-24/hr=14/{F}.parquet"),
        ] {
            assert!(SpanObject::parse(path).is_none(), "{path}");
        }
    }
}
