//! Hot window: rows still in ingester memory, served to queriers over an
//! internal Arrow Flight endpoint.
//!
//! The querier sends a [`HotScanRequest`] as the ticket. The ingester filters
//! its buffers by time and service, projects the requested columns and streams
//! the result. The response header [`PERSISTED_FILES_HEADER`] lists the ids of
//! every file whose rows are included: already registered, being written, or
//! the id the live rows will get. The querier drops those files from its own
//! scan and reads neither twice nor never, whatever the catalog shows at the
//! moment it plans.
//!
//! This endpoint is internal: it trusts the caller's tenant id. It must only
//! be reachable by queriers (cluster network now, mTLS later).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::{and, cast, filter_record_batch};
use arrow::datatypes::{DataType, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use fs_catalog::{FileId, TenantId};
use fs_schema::spans::{col, span_schema_v1};
use futures_util::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::IngestError;
use crate::ingester::Shared;

/// Response header: comma-separated file ids whose rows are included.
pub const PERSISTED_FILES_HEADER: &str = "x-fusescope-persisted-files";

/// What the querier asks for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotScanRequest {
    /// Tenant whose rows to return.
    pub tenant: TenantId,
    /// Inclusive lower bound on `timestamp`, nanoseconds.
    pub ts_min: Option<i64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds.
    pub ts_max: Option<i64>,
    /// Keep only these services.
    pub services: Option<Vec<String>>,
    /// Return only these columns, in this order. `None` returns every column.
    pub columns: Option<Vec<String>>,
    /// Keep only rows of these traces (16-byte ids). Absent in older tickets.
    #[serde(default)]
    pub trace_ids: Option<Vec<Vec<u8>>>,
    /// Keep only these spans (8-byte ids). Absent in older tickets.
    #[serde(default)]
    pub span_ids: Option<Vec<Vec<u8>>>,
}

impl HotScanRequest {
    /// Encodes the request as a Flight ticket.
    pub fn to_ticket(&self) -> Ticket {
        Ticket::new(serde_json::to_vec(self).unwrap_or_default())
    }

    /// Decodes a ticket.
    ///
    /// # Errors
    /// `INVALID_ARGUMENT` when the ticket is not a request.
    pub fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        serde_json::from_slice(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad hot scan ticket: {e}")))
    }
}

/// What an ingester answered.
#[derive(Debug, Default)]
pub struct HotScanResponse {
    /// Matching rows, projected.
    pub batches: Vec<RecordBatch>,
    /// Files whose rows are included; the querier must not read them.
    pub persisted_files: Vec<FileId>,
}

/// Applies the request's filters and projection to one batch. Public for tests.
///
/// # Errors
/// Arrow failures, or unknown column names.
pub fn apply(
    batch: &RecordBatch,
    request: &HotScanRequest,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = batch.schema();
    let mut mask: Option<BooleanArray> = None;
    let mut add = |m: BooleanArray| -> Result<(), arrow::error::ArrowError> {
        mask = Some(match mask.take() {
            Some(existing) => and(&existing, &m)?,
            None => m,
        });
        Ok(())
    };
    if request.ts_min.is_some() || request.ts_max.is_some() {
        let ts = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>();
        let min = request.ts_min.unwrap_or(i64::MIN);
        let max = request.ts_max.unwrap_or(i64::MAX);
        add(BooleanArray::from_iter(
            ts.iter()
                .map(|v| Some(v.is_some_and(|t| t >= min && t <= max))),
        ))?;
    }
    if let Some(services) = &request.services {
        let column = batch.column(schema.index_of("service_name")?);
        let strings = cast(column, &DataType::Utf8)?;
        let strings = strings.as_string::<i32>();
        add(BooleanArray::from_iter((0..strings.len()).map(|i| {
            Some(strings.is_valid(i) && services.iter().any(|s| s == strings.value(i)))
        })))?;
    }
    for (ids, column) in [
        (&request.trace_ids, col::TRACE_ID),
        (&request.span_ids, col::SPAN_ID),
    ] {
        if let Some(ids) = ids {
            let wanted: std::collections::HashSet<&[u8]> = ids.iter().map(Vec::as_slice).collect();
            let values = batch
                .column(schema.index_of(column)?)
                .as_fixed_size_binary();
            add(BooleanArray::from_iter(
                values
                    .iter()
                    .map(|v| Some(v.is_some_and(|v| wanted.contains(v)))),
            ))?;
        }
    }
    let filtered = match mask {
        Some(mask) => filter_record_batch(batch, &mask)?,
        None => batch.clone(),
    };
    match &request.columns {
        Some(columns) => {
            let indices = columns
                .iter()
                .map(|c| schema.index_of(c))
                .collect::<Result<Vec<_>, _>>()?;
            filtered.project(&indices)
        }
        None => Ok(filtered),
    }
}

/// Serves the hot window on `listen` until `shutdown` flips. No-op when
/// `listen` is `None`.
///
/// # Errors
/// Bind or transport failure.
pub(crate) async fn serve(
    shared: Arc<Shared>,
    listen: Option<std::net::SocketAddr>,
    mut shutdown: watch::Receiver<bool>,
    bound: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
) -> Result<(), IngestError> {
    let Some(listen) = listen else {
        let _ = shutdown.changed().await;
        return Ok(());
    };
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| IngestError::Bind {
            addr: listen.to_string(),
            source,
        })?;
    let addr = listener.local_addr().map_err(|source| IngestError::Bind {
        addr: "local_addr".into(),
        source,
    })?;
    info!(%addr, "hot window flight listening");
    if let Some(tx) = bound {
        let _ = tx.send(addr);
    }
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(HotService { shared }))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                let _ = shutdown.changed().await;
            },
        )
        .await?;
    info!("hot window flight stopped");
    Ok(())
}

/// Scans one ingester. Used by the querier.
///
/// # Errors
/// Connection, timeout or decoding failures, wrapped as [`IngestError::HotScan`].
pub async fn scan(
    address: &str,
    request: &HotScanRequest,
    timeout: Duration,
) -> Result<HotScanResponse, IngestError> {
    let fail = |reason: String| IngestError::HotScan {
        ingester: address.to_owned(),
        reason,
    };
    let run = async {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .map_err(|e| fail(e.to_string()))?;
        let channel = endpoint
            .connect_timeout(timeout)
            .connect()
            .await
            .map_err(|e| fail(e.to_string()))?;
        let mut client = FlightServiceClient::new(channel);
        let response = client
            .do_get(request.to_ticket())
            .await
            .map_err(|e| fail(e.to_string()))?;
        let persisted_files = response
            .metadata()
            .get(PERSISTED_FILES_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.split(',')
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| s.parse().ok())
                    .map(FileId)
                    .collect()
            })
            .unwrap_or_default();
        let stream = response
            .into_inner()
            .map_err(|status| FlightError::Tonic(Box::new(status)));
        let batches: Vec<RecordBatch> =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream)
                .try_collect()
                .await
                .map_err(|e| fail(e.to_string()))?;
        Ok(HotScanResponse {
            batches,
            persisted_files,
        })
    };
    tokio::time::timeout(timeout, run)
        .await
        .map_err(|_| fail(format!("timeout after {timeout:?}")))?
}

struct HotService {
    shared: Arc<Shared>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for HotService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let scan = HotScanRequest::from_ticket(request.get_ref())?;
        let (snapshot, live_file) = self.shared.snapshot(scan.tenant).await;
        let sources = snapshot
            .live
            .iter()
            .chain(snapshot.flushing.iter().flat_map(|(_, b)| b.iter()))
            .chain(snapshot.persisted.iter().flat_map(|p| p.batches.iter()));
        // A batch whose trace set holds none of the requested traces is not
        // even filtered: the chunks and the persisted rows carry their set.
        let wanted: Option<std::collections::HashSet<[u8; 16]>> =
            scan.trace_ids.as_ref().map(|ids| {
                ids.iter()
                    .filter_map(|id| <[u8; 16]>::try_from(id.as_slice()).ok())
                    .collect()
            });
        let mut batches = Vec::new();
        let mut skipped = 0u64;
        for source in sources {
            if let (Some(wanted), Some(traces)) = (&wanted, &source.traces) {
                if !wanted.iter().any(|id| traces.contains(id)) {
                    skipped += 1;
                    continue;
                }
            }
            let kept =
                apply(&source.batch, &scan).map_err(|e| Status::invalid_argument(e.to_string()))?;
            if kept.num_rows() > 0 {
                batches.push(kept);
            }
        }
        self.shared
            .stats
            .hot_batches_skipped
            .fetch_add(skipped, std::sync::atomic::Ordering::Relaxed);
        // Every file id whose rows are (or will be) in this answer. Excluding
        // an id that is not in the catalog yet is harmless and closes the race
        // where it appears while the querier plans.
        let persisted: Vec<String> = live_file
            .into_iter()
            .chain(snapshot.flushing.iter().map(|(id, _)| *id))
            .chain(snapshot.persisted.iter().map(|p| p.file))
            .map(|id| id.0.to_string())
            .collect();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        debug!(tenant = %scan.tenant.0, rows, persisted = persisted.len(), "hot scan");

        let schema = match &scan.columns {
            Some(columns) => {
                let full = span_schema_v1();
                let indices = columns
                    .iter()
                    .map(|c| full.index_of(c))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                Arc::new(
                    full.project(&indices)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?,
                )
            }
            None => span_schema_v1(),
        };
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures_util::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(e.to_string()));
        let mut response = Response::new(Box::pin(encoded) as Self::DoGetStream);
        if let Ok(value) = persisted.join(",").parse() {
            response
                .metadata_mut()
                .insert(PERSISTED_FILES_HEADER, value);
        }
        Ok(response)
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("hot window serves do_get only"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_round_trip() {
        let request = HotScanRequest {
            tenant: TenantId::new(),
            ts_min: Some(1),
            ts_max: None,
            services: Some(vec!["api".into()]),
            columns: Some(vec!["timestamp".into(), "name".into()]),
            trace_ids: None,
            span_ids: None,
        };
        assert_eq!(
            HotScanRequest::from_ticket(&request.to_ticket()).unwrap(),
            request
        );
        assert!(HotScanRequest::from_ticket(&Ticket::new(b"nope".to_vec())).is_err());

        // A ticket from a querier that predates the id predicates.
        let old = format!(
            r#"{{"tenant":"{}","ts_min":1,"ts_max":null,"services":null,"columns":null}}"#,
            request.tenant.0
        );
        let decoded = HotScanRequest::from_ticket(&Ticket::new(old.into_bytes())).unwrap();
        assert_eq!(decoded.tenant, request.tenant);
        assert_eq!(decoded.trace_ids, None);
        assert_eq!(decoded.span_ids, None);
    }
}
