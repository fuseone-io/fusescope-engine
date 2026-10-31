//! The OTLP trace and logs services.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::IngestError;
use crate::ingester::{Converted, Shared};
use crate::record::{LOG_SEED_BYTES, RecordSignal, WalRecord, log_payload};

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

/// gRPC handler of the logs service. Cheap to clone; state lives in [`Shared`].
#[derive(Clone)]
pub struct LogsReceiver {
    shared: Arc<Shared>,
}

impl LogsReceiver {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

/// Refuses a tenant over its throughput limit.
fn check_rate(
    shared: &Shared,
    tenant: fs_catalog::TenantId,
    bytes: usize,
    now: Instant,
) -> Result<(), Status> {
    if let Some(limiter) = &shared.limiter {
        let allowed = limiter
            .lock()
            .map_or(true, |mut l| l.allow(tenant, bytes, now));
        if !allowed {
            shared
                .stats
                .rate_limited
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(Status::resource_exhausted("tenant throughput limit"));
        }
    }
    Ok(())
}

/// The status a failed ingest answers with.
fn status_of(shared: &Shared, e: IngestError) -> Status {
    match e {
        IngestError::Backpressure(what) => {
            shared
                .stats
                .over_capacity
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Status::resource_exhausted(format!("ingester busy: {what}"))
        }
        IngestError::Capacity(what) => Status::resource_exhausted(what),
        IngestError::WalFull => Status::unavailable("write-ahead log full"),
        IngestError::TooManyHours { .. } => Status::invalid_argument(e.to_string()),
        IngestError::Wal(_) | IngestError::Join(_) => {
            warn!(error = %e, "wal append failed");
            Status::unavailable("write-ahead log unavailable")
        }
        other => {
            warn!(error = %other, "conversion failed");
            Status::internal("conversion failed")
        }
    }
}

/// Runs an ingest on its own task: a client that goes away after the record
/// is logged must not drop the work before the rows reach the buffer, or
/// memory and the log would disagree until a restart replayed it.
async fn run_ingest<F>(shared: &Arc<Shared>, work: F) -> Result<Converted, Status>
where
    F: Future<Output = Result<Converted, IngestError>> + Send + 'static,
{
    tokio::spawn(work)
        .await
        .map_err(|e| {
            warn!(error = %e, "ingest task failed");
            Status::internal("ingest task failed")
        })?
        .map_err(|e| status_of(shared, e))
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
        check_rate(shared, tenant, payload.len(), now)?;
        let record = WalRecord {
            version: crate::record::VERSION,
            tenant,
            signal: RecordSignal::Traces,
            payload,
        };
        let task_shared = Arc::clone(shared);
        let outcome = run_ingest(shared, async move {
            task_shared
                .ingest(tenant, &message, record.encode(), now)
                .await
        })
        .await?;
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

#[tonic::async_trait]
impl LogsService for LogsReceiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let shared = &self.shared;
        let tenant = shared.auth.authenticate(request.metadata()).await?;
        let message = request.into_inner();
        let seed: [u8; LOG_SEED_BYTES] = *uuid::Uuid::new_v4().as_bytes();
        let payload = log_payload(seed, &message.encode_to_vec());
        let now = Instant::now();
        check_rate(shared, tenant, payload.len(), now)?;
        let record = WalRecord {
            version: crate::record::VERSION,
            tenant,
            signal: RecordSignal::Logs,
            payload,
        };
        let task_shared = Arc::clone(shared);
        let outcome = run_ingest(shared, async move {
            task_shared
                .ingest_logs(tenant, &message, &seed, record.encode(), now)
                .await
        })
        .await?;
        debug!(tenant = %tenant.0, logs = outcome.accepted, rejected = outcome.rejected, "export");

        let partial_success = (outcome.rejected > 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: i64::try_from(outcome.rejected).unwrap_or(i64::MAX),
            error_message: "log records with neither an event nor an observed time were dropped"
                .into(),
        });
        Ok(Response::new(ExportLogsServiceResponse { partial_success }))
    }
}
