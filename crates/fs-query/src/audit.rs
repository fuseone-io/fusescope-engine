//! One audit event per `DoGet` and `DoAction`, refused ones included:
//! tenant and subject once known, what ran (the action type or
//! `statement`), a hash of its text once known, rows sent, duration and
//! outcome, logged at `info` under the `fusescope::audit` target once the
//! request ends, however it ends. Strings a client chose are escaped, so
//! one event stays one line; the text itself is never logged.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_util::Stream;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::auth::Caller;

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Its token or its ticket was refused.
    Refused,
    /// Failed before or while streaming.
    Failed,
    /// The client went away mid-stream.
    Cancelled,
    /// Every row was sent.
    Ok,
}

/// The audit record of one request, logged when dropped.
pub(crate) struct Audit {
    stats: std::sync::Arc<crate::querier::QueryStats>,
    tenant: Option<fs_catalog::TenantId>,
    subject: Option<String>,
    kind: String,
    hash: Option<String>,
    started: Instant,
    rows: u64,
    outcome: Outcome,
}

impl Audit {
    /// A record for a request of `kind`, before anything about it is known;
    /// an authenticated one's duration lands in `stats`' latency.
    pub(crate) fn begin(kind: &str, stats: std::sync::Arc<crate::querier::QueryStats>) -> Self {
        Self {
            stats,
            tenant: None,
            subject: None,
            kind: kind.to_owned(),
            hash: None,
            started: Instant::now(),
            rows: 0,
            outcome: Outcome::Failed,
        }
    }

    /// The caller, once authenticated.
    pub(crate) fn caller(&mut self, caller: &Caller) {
        self.tenant = Some(caller.tenant);
        self.subject.clone_from(&caller.subject);
    }

    /// The text that runs, once known; only its hash is kept.
    pub(crate) fn text(&mut self, text: &[u8]) {
        let digest = Sha256::digest(text);
        self.hash = Some(
            digest
                .iter()
                .take(8)
                .fold(String::with_capacity(16), |mut out, b| {
                    use std::fmt::Write;
                    let _ = write!(out, "{b:02x}");
                    out
                }),
        );
    }

    /// Settles the record on the status that ended the request before
    /// streaming: `refused` when the caller or its ticket was turned away
    /// (`UNAUTHENTICATED`, `PERMISSION_DENIED`), `failed` for anything else,
    /// an unreachable catalog or a deadline included.
    pub(crate) fn ended_with(&mut self, status: &tonic::Status) {
        self.outcome = match status.code() {
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => Outcome::Refused,
            _ => Outcome::Failed,
        };
    }

    /// Follows `batches` to their end, counting rows; the record is
    /// logged when the stream ends or is dropped.
    pub(crate) fn follow<S, E>(mut self, batches: S) -> Audited<S>
    where
        S: Stream<Item = Result<arrow::record_batch::RecordBatch, E>>,
    {
        self.outcome = Outcome::Cancelled;
        Audited {
            inner: Box::pin(batches),
            audit: self,
        }
    }
}

impl Drop for Audit {
    fn drop(&mut self) {
        let outcome = match self.outcome {
            Outcome::Refused => "refused",
            Outcome::Failed => "failed",
            Outcome::Cancelled => "cancelled",
            Outcome::Ok => "ok",
        };
        if self.tenant.is_some() && self.outcome != Outcome::Refused {
            self.stats.latency.0.observe(self.started.elapsed());
        }
        let tenant = self
            .tenant
            .map_or_else(|| "-".to_owned(), |t| t.0.to_string());
        let subject = match (&self.tenant, &self.subject) {
            (None, _) => "-",
            (Some(_), None) => "token",
            (Some(_), Some(subject)) => subject,
        };
        info!(
            target: "fusescope::audit",
            tenant = %tenant,
            subject = %subject.escape_debug(),
            kind = %self.kind.escape_debug(),
            query_hash = self.hash.as_deref().unwrap_or("-"),
            rows = self.rows,
            duration_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            outcome,
            "query"
        );
    }
}

/// A batch stream whose end settles its audit record.
pub(crate) struct Audited<S> {
    inner: Pin<Box<S>>,
    audit: Audit,
}

impl<S, E> Stream for Audited<S>
where
    S: Stream<Item = Result<arrow::record_batch::RecordBatch, E>>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = this.inner.as_mut().poll_next(cx);
        match &next {
            Poll::Ready(Some(Ok(batch))) => {
                let rows = u64::try_from(batch.num_rows()).unwrap_or(0);
                this.audit.rows += rows;
                this.audit
                    .stats
                    .rows
                    .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
            }
            Poll::Ready(Some(Err(_))) => this.audit.outcome = Outcome::Failed,
            Poll::Ready(None) if this.audit.outcome == Outcome::Cancelled => {
                this.audit.outcome = Outcome::Ok;
            }
            Poll::Ready(None) | Poll::Pending => {}
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures_util::StreamExt;

    use super::*;

    fn batch(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
        )
        .unwrap()
    }

    fn audit() -> Audit {
        let caller = Caller {
            tenant: fs_catalog::TenantId(uuid::Uuid::from_u128(1)),
            subject: Some("user:alice".into()),
        };
        let mut audit = Audit::begin("statement", std::sync::Arc::default());
        audit.caller(&caller);
        audit.text(b"select 1");
        audit
    }

    #[tokio::test]
    async fn the_outcome_follows_how_the_stream_ends() {
        assert_eq!(audit().outcome, Outcome::Failed, "failed before streaming");
        assert_eq!(audit().hash.as_deref().map(str::len), Some(16));
        let mut refused = Audit::begin("metadata.services\nforged=line", std::sync::Arc::default());
        refused.ended_with(&tonic::Status::unauthenticated("bad signature"));
        assert_eq!((refused.outcome, refused.tenant), (Outcome::Refused, None));
        for (status, outcome) in [
            (tonic::Status::permission_denied("ticket"), Outcome::Refused),
            (tonic::Status::unavailable("catalog"), Outcome::Failed),
            (tonic::Status::deadline_exceeded("timeout"), Outcome::Failed),
            (
                tonic::Status::invalid_argument("bad ticket"),
                Outcome::Failed,
            ),
        ] {
            let mut audit = Audit::begin("statement", std::sync::Arc::default());
            audit.ended_with(&status);
            assert_eq!(audit.outcome, outcome, "{status:?}");
        }
        assert_eq!(
            refused.kind.escape_debug().to_string(),
            "metadata.services\\nforged=line",
            "a client's string stays on one line"
        );

        let mut done = audit().follow(futures_util::stream::iter([
            Ok::<_, String>(batch(2)),
            Ok(batch(3)),
        ]));
        while done.next().await.is_some() {}
        assert_eq!((done.audit.outcome, done.audit.rows), (Outcome::Ok, 5));

        let mut failed = audit().follow(futures_util::stream::iter([
            Ok(batch(1)),
            Err("boom".to_owned()),
        ]));
        while failed.next().await.is_some() {}
        assert_eq!(failed.audit.outcome, Outcome::Failed);

        let mut cancelled = audit().follow(futures_util::stream::iter([
            Ok::<_, String>(batch(1)),
            Ok(batch(1)),
        ]));
        cancelled.next().await;
        assert_eq!(
            cancelled.audit.outcome,
            Outcome::Cancelled,
            "dropped mid-stream"
        );

        let stats = std::sync::Arc::new(crate::querier::QueryStats::default());
        let mut refused = Audit::begin("statement", std::sync::Arc::clone(&stats));
        refused.ended_with(&tonic::Status::unauthenticated("bad token"));
        drop(refused);
        assert_eq!(stats.latency.0.count(), 0, "a refusal is not an execution");
        let mut ran = Audit::begin("statement", std::sync::Arc::clone(&stats));
        ran.caller(&Caller {
            tenant: fs_catalog::TenantId(uuid::Uuid::from_u128(1)),
            subject: None,
        });
        drop(ran);
        assert_eq!(stats.latency.0.count(), 1);
        let mut denied = Audit::begin("statement", std::sync::Arc::clone(&stats));
        denied.caller(&Caller {
            tenant: fs_catalog::TenantId(uuid::Uuid::from_u128(1)),
            subject: Some("user:alice".into()),
        });
        denied.ended_with(&tonic::Status::permission_denied("another tenant's ticket"));
        drop(denied);
        assert_eq!(
            stats.latency.0.count(),
            1,
            "an authenticated caller refused its ticket is not an execution"
        );

        let slow = crate::querier::QueryLatency::default();
        slow.0.observe(std::time::Duration::from_secs(45));
        let at = slow.0.bounds().iter().position(|b| *b >= 45.0).unwrap();
        assert_eq!(slow.0.bounds().get(at), Some(&60.0));
        assert_eq!(
            slow.0.cumulative().get(at),
            Some(&1),
            "45 s lands in a finite bucket, so its quantile is not clipped to 30 s"
        );
    }
}
