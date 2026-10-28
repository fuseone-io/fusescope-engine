//! The OTLP trace service.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::IngestError;
use crate::ingester::Shared;
use crate::record::{RecordSignal, WalRecord};

/// gRPC handler. Cheap to clone; state lives in [`Shared`].
#[derive(Clone)]
pub struct TraceReceiver {
    shared: Arc<Shared>,
}

impl TraceReceiver {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

#[tonic::async_trait]
impl TraceService for TraceReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let shared = &self.shared;
        let tenant = shared.auth.authenticate(request.metadata()).await?;
        let message = request.into_inner();
        let payload = Bytes::from(message.encode_to_vec());
        let now = Instant::now();

        if let Some(limiter) = &shared.limiter {
            let allowed = limiter
                .lock()
                .map_or(true, |mut l| l.allow(tenant, payload.len(), now));
            if !allowed {
                shared
                    .stats
                    .rate_limited
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(Status::resource_exhausted("tenant throughput limit"));
            }
        }
        let record = WalRecord {
            version: crate::record::VERSION,
            tenant,
            signal: RecordSignal::Traces,
            payload,
        };
        // On its own task: a client that goes away after the record is logged
        // must not drop the work before the rows reach the buffer, or memory
        // and the log would disagree until a restart replayed it.
        let task_shared = Arc::clone(shared);
        let outcome = tokio::spawn(async move {
            task_shared
                .ingest(tenant, &message, record.encode(), now)
                .await
        })
        .await
        .map_err(|e| {
            warn!(error = %e, "ingest task failed");
            Status::internal("ingest task failed")
        })?
        .map_err(|e| match e {
            IngestError::Backpressure(what) => {
                shared
                    .stats
                    .over_capacity
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Status::resource_exhausted(format!("ingester busy: {what}"))
            }
            IngestError::Capacity(what) => Status::resource_exhausted(what),
            IngestError::WalFull => Status::unavailable("write-ahead log full"),
            IngestError::Wal(_) | IngestError::Join(_) => {
                warn!(error = %e, "wal append failed");
                Status::unavailable("write-ahead log unavailable")
            }
            other => {
                warn!(error = %other, "conversion failed");
                Status::internal("conversion failed")
            }
        })?;
        debug!(tenant = %tenant.0, spans = outcome.accepted, rejected = outcome.rejected, "export");

        let partial_success = (outcome.rejected > 0).then(|| ExportTracePartialSuccess {
            rejected_spans: i64::try_from(outcome.rejected).unwrap_or(i64::MAX),
            error_message: "spans with malformed trace or span ids were dropped".into(),
        });
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success,
        }))
    }
}
