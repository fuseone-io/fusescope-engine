//! The bucket layout of span files and their sidecars, parsed strictly:
//! anything the engine would not write is not the engine's.
//!
//! ```text
//! t/{tenant uuid}/traces/l{0|1|2}/dt=YYYY-MM-DD/hr=HH/{file uuid}.parquet
//! t/{tenant uuid}/traces/index/trace_id/dt=YYYY-MM-DD/hr=HH/{file uuid}.tix
//! ```

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
        if !is_uuid(tenant) || parts.next()? != "traces" {
            return None;
        }
        let kind = parts.next()?;
        let (level, sidecar) = match kind {
            "l0" => (Some(0), false),
            "l1" => (Some(1), false),
            "l2" => (Some(2), false),
            "index" => {
                if parts.next()? != "trace_id" {
                    return None;
                }
                (None, true)
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

/// `dt=YYYY-MM-DD`.
fn is_date(s: &str) -> bool {
    let Some(d) = s.strip_prefix("dt=") else {
        return false;
    };
    let b = d.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        })
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
        let tix = SpanObject::parse(&format!(
            "t/{T}/traces/index/trace_id/dt=2026-10-24/hr=14/{F}.tix"
        ))
        .unwrap();
        assert!(tix.sidecar);
        assert_eq!(tix.level, None);
        assert_eq!(tix.tenant, T);
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
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/{F}.tix"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/notes.parquet"),
            &format!("t/{T}/traces/l0/dt=2026-10-24/hr=14/{F}.parquet/extra"),
            &format!("t/{T}/traces/index/trace_id/dt=2026-10-24/hr=14/{F}.parquet"),
            &format!("t/{T}/traces/index/other/dt=2026-10-24/hr=14/{F}.tix"),
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
