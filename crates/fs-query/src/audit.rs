//! One audit event per execution: tenant, subject, what ran (the action
//! type or `statement`), a hash of its text, rows sent, duration and
//! outcome, logged at `info` under the `fusescope::audit` target once the
//! execution ends, however it ends. The text itself is never logged.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_util::Stream;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::auth::Caller;

/// How an execution ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Refused or failed before or while streaming.
    Failed,
    /// The client went away mid-stream.
    Cancelled,
    /// Every row was sent.
    Ok,
}

/// The audit record of one execution, logged when dropped.
pub(crate) struct Audit {
    caller: Caller,
    kind: String,
    hash: String,
    started: Instant,
    rows: u64,
    outcome: Outcome,
}

impl Audit {
    pub(crate) fn start(caller: &Caller, kind: &str, text: &[u8]) -> Self {
        let digest = Sha256::digest(text);
        let hash = digest
            .iter()
            .take(8)
            .fold(String::with_capacity(16), |mut out, b| {
                use std::fmt::Write;
                let _ = write!(out, "{b:02x}");
                out
            });
        Self {
            caller: caller.clone(),
            kind: kind.to_owned(),
            hash,
            started: Instant::now(),
            rows: 0,
            outcome: Outcome::Failed,
        }
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
            Outcome::Failed => "failed",
            Outcome::Cancelled => "cancelled",
            Outcome::Ok => "ok",
        };
        info!(
            target: "fusescope::audit",
            tenant = %self.caller.tenant.0,
            subject = self.caller.subject.as_deref().unwrap_or("token"),
            kind = %self.kind,
            query_hash = %self.hash,
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
                this.audit.rows += u64::try_from(batch.num_rows()).unwrap_or(0);
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
        Audit::start(&caller, "statement", b"select 1")
    }

    #[tokio::test]
    async fn the_outcome_follows_how_the_stream_ends() {
        assert_eq!(audit().outcome, Outcome::Failed, "refused before streaming");
        assert_eq!(audit().hash.len(), 16);

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
    }
}
