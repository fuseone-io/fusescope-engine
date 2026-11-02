//! Hot window: rows still in ingester memory, served to queriers over an
//! internal Arrow Flight endpoint.
//!
//! The querier sends a [`HotScanRequest`] as the ticket. The ingester filters
//! the buffers of the requested signal by time and service, projects the
//! requested columns of that signal's schema and streams the result; the
//! schema it sends names the signal in its `fusescope.signal` metadata. The response header [`PERSISTED_FILES_HEADER`] lists the ids of
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
use fs_schema::spans::col;
use futures_util::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::IngestError;
use crate::flush::schema_of;
use crate::ingester::Shared;
use crate::record::RowSignal;

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
    /// Answer the distinct values of the one projected column, at most
    /// this many, as one batch, instead of every row: the ingester keeps
    /// no more than that. Absent in older tickets.
    #[serde(default)]
    pub distinct: Option<usize>,
    /// Whose rows: spans when absent, as in tickets from before logs.
    #[serde(default, skip_serializing_if = "RowSignal::is_traces")]
    pub signal: RowSignal,
    /// For metrics: the samples or the series. Absent for other signals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<MetricTable>,
}

/// One of the two tables of the metrics signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricTable {
    /// `samples`.
    Samples,
    /// `time_series`.
    Series,
}

/// The schema a scan's rows have: its signal's, for metrics its table's.
pub fn scan_schema(scan: &HotScanRequest) -> arrow::datatypes::SchemaRef {
    match (scan.signal, scan.table) {
        (RowSignal::Metrics, Some(MetricTable::Series)) => fs_schema::metrics::series_schema_v2(),
        (signal, _) => schema_of(signal),
    }
}

const MILLIS_PER_HOUR: i64 = 3_600_000;

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
    /// The schema the ingester sent, rows or not; it names the signal the
    /// ingester answered for.
    pub schema: Option<arrow::datatypes::SchemaRef>,
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
        let min = request.ts_min.unwrap_or(i64::MIN);
        let max = request.ts_max.unwrap_or(i64::MAX);
        if let Ok(index) = schema.index_of(col::TIMESTAMP) {
            let ts = batch
                .column(index)
                .as_primitive::<TimestampNanosecondType>();
            add(BooleanArray::from_iter(
                ts.iter()
                    .map(|v| Some(v.is_some_and(|t| t >= min && t <= max))),
            ))?;
        } else {
            // Metric rows are timed in milliseconds; a series row stands
            // for its whole hour.
            let span = match request.table {
                Some(MetricTable::Series) => MILLIS_PER_HOUR - 1,
                _ => 0,
            };
            let ms = batch
                .column(schema.index_of(fs_schema::metrics::col::UNIX_MILLI)?)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let nanos = |m: i64| m.saturating_mul(1_000_000);
            add(BooleanArray::from_iter(ms.iter().map(|v| {
                Some(v.is_some_and(|m| nanos(m) <= max && nanos(m.saturating_add(span)) >= min))
            })))?;
        }
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
    tls: Option<&fs_auth::ServerTls>,
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
    info!(%addr, tls = tls.is_some(), "hot window flight listening");
    if let Some(tx) = bound {
        let _ = tx.send(addr);
    }
    let router = tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(HotService { shared }));
    fs_auth::serve(router, listener, tls, async move {
        let _ = shutdown.changed().await;
    })
    .await?;
    info!("hot window flight stopped");
    Ok(())
}

/// Scans one ingester in plain text, as [`scan_with`] without TLS.
///
/// # Errors
/// Connection, timeout or decoding failures, wrapped as [`IngestError::HotScan`].
pub async fn scan(
    address: &str,
    request: &HotScanRequest,
    timeout: Duration,
) -> Result<HotScanResponse, IngestError> {
    scan_with(address, request, timeout, None).await
}

/// Scans one ingester, over mTLS with `tls`. Used by the querier.
///
/// # Errors
/// Connection, handshake, timeout or decoding failures, wrapped as
/// [`IngestError::HotScan`].
pub async fn scan_with(
    address: &str,
    request: &HotScanRequest,
    timeout: Duration,
    tls: Option<&fs_auth::ClientTls>,
) -> Result<HotScanResponse, IngestError> {
    scan_charged(address, request, timeout, tls, Arc::new(|_| true)).await
}

/// Bytes one array holds besides its buffers.
const ARRAY_BYTES: usize = 128;

/// What `batch` keeps alive: each distinct allocation its buffers share,
/// once, at its capacity, and every array of it, children included.
pub fn held_bytes(batch: &RecordBatch) -> usize {
    held_bytes_of(batch.columns())
}

/// [`held_bytes`] of `columns`.
pub fn held_bytes_of(columns: &[arrow::array::ArrayRef]) -> usize {
    let mut seen = std::collections::HashSet::new();
    let mut bytes = 0usize;
    let mut pending: Vec<arrow::array::ArrayData> =
        columns.iter().map(arrow::array::Array::to_data).collect();
    while let Some(data) = pending.pop() {
        bytes += ARRAY_BYTES;
        let nulls = data.nulls().map(|n| n.inner().inner().clone());
        for buffer in data.buffers().iter().chain(nulls.iter()) {
            if seen.insert(buffer.data_ptr().as_ptr().addr()) {
                bytes += buffer.capacity();
            }
        }
        pending.extend(data.child_data().iter().cloned());
    }
    bytes
}

/// What a hot scan charges each message to; `false` refuses it.
pub type Charge = Arc<dyn Fn(usize) -> bool + Send + Sync>;

/// [`scan_with`], each message of the answer charged to `charge` before it
/// is decoded, and each batch decoded from it, at [`held_bytes`], before it
/// is kept. A charge refused ends the scan, so no more than one message
/// and the batch decoded from it beyond what `charge` admitted are ever
/// held.
///
/// # Errors
/// As [`scan_with`], and [`IngestError::HotScanBudget`] when `charge`
/// refuses a message.
pub async fn scan_charged(
    address: &str,
    request: &HotScanRequest,
    timeout: Duration,
    tls: Option<&fs_auth::ClientTls>,
    charge: Charge,
) -> Result<HotScanResponse, IngestError> {
    let refused = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fail = |reason: String| IngestError::HotScan {
        ingester: address.to_owned(),
        reason,
    };
    let run = async {
        let endpoint = fs_auth::endpoint(address, tls).map_err(|e| fail(e.to_string()))?;
        let channel = endpoint
            .connect_timeout(timeout)
            .connect()
            .await
            .map_err(|e| fail(fs_auth::error_chain(&e)))?;
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
        let (charged, refusing) = (Arc::clone(&charge), Arc::clone(&refused));
        let stream = response.into_inner().map(move |message| {
            let message = message.map_err(|status| FlightError::Tonic(Box::new(status)))?;
            let bytes = message.data_body.len() + message.data_header.len();
            if charged(bytes) {
                Ok(message)
            } else {
                refusing.store(true, std::sync::atomic::Ordering::Relaxed);
                Err(FlightError::ProtocolError("past the memory budget".into()))
            }
        });
        let mut stream =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream);
        let mut batches = Vec::new();
        loop {
            match stream.try_next().await {
                Ok(Some(batch)) if charge(held_bytes(&batch)) => batches.push(batch),
                Ok(Some(_)) => {
                    return Err(IngestError::HotScanBudget {
                        ingester: address.to_owned(),
                    });
                }
                Ok(None) => break,
                Err(_) if refused.load(std::sync::atomic::Ordering::Relaxed) => {
                    return Err(IngestError::HotScanBudget {
                        ingester: address.to_owned(),
                    });
                }
                Err(e) => return Err(fail(e.to_string())),
            }
        }
        Ok(HotScanResponse {
            batches,
            schema: stream.schema().cloned(),
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

/// The distinct values of the one projected column of a scan, gathered
/// batch by batch so no row is kept beyond its first sighting, and no
/// more than `cap` of them, so the answer is bounded whatever the rows.
struct Distinct {
    column: String,
    cap: usize,
    values: std::collections::BTreeSet<String>,
    schema: arrow::datatypes::SchemaRef,
}

impl Distinct {
    fn for_request(scan: &HotScanRequest, cap: usize) -> Result<Self, Status> {
        match scan.columns.as_deref() {
            Some([column]) => Ok(Self {
                column: column.clone(),
                cap,
                values: std::collections::BTreeSet::new(),
                schema: scan_schema(scan),
            }),
            _ => Err(Status::invalid_argument(
                "a distinct scan projects exactly one column",
            )),
        }
    }

    fn fold(&mut self, batch: &RecordBatch) -> Result<(), Status> {
        let invalid = |e: arrow::error::ArrowError| Status::invalid_argument(e.to_string());
        let column = batch.column(batch.schema().index_of(&self.column).map_err(invalid)?);
        let strings = cast(column, &DataType::Utf8).map_err(invalid)?;
        for value in strings.as_string::<i32>().iter().flatten() {
            if self.values.len() >= self.cap {
                break;
            }
            if !self.values.contains(value) {
                self.values.insert(value.to_owned());
            }
        }
        Ok(())
    }

    /// One batch of the values in the column's declared type; none when
    /// nothing was seen.
    fn finish(self) -> Result<Option<RecordBatch>, Status> {
        if self.values.is_empty() {
            return Ok(None);
        }
        let internal = |e: arrow::error::ArrowError| Status::internal(e.to_string());
        let field = self
            .schema
            .field_with_name(&self.column)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let values = arrow::array::StringArray::from_iter_values(self.values);
        let typed = cast(&values, field.data_type()).map_err(internal)?;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field.clone()]));
        Ok(Some(
            RecordBatch::try_new(schema, vec![typed]).map_err(internal)?,
        ))
    }
}

/// The batches a scan reads, in every state: the samples or series of a
/// metric stream as `scan` asks, the rows of another. Shared, not copied.
fn scan_sources(
    snapshot: &crate::buffer::Snapshot,
    scan: &HotScanRequest,
) -> Vec<crate::buffer::HotBatch> {
    if scan.table == Some(MetricTable::Series) {
        let as_hot = |batch: &RecordBatch| crate::buffer::HotBatch {
            batch: batch.clone(),
            traces: None,
        };
        let series = &snapshot.series;
        return series
            .live
            .iter()
            .map(as_hot)
            .chain(series.flushing.iter().map(|(_, b)| as_hot(b)))
            .chain(series.persisted.iter().flat_map(|(_, b)| b.iter().cloned()))
            .collect();
    }
    snapshot
        .live
        .iter()
        .chain(snapshot.flushing.iter().flat_map(|(_, b)| b.iter()))
        .chain(snapshot.persisted.iter().flat_map(|p| p.batches.iter()))
        .cloned()
        .collect()
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
        let stream = crate::buffer::Stream {
            tenant: scan.tenant,
            signal: scan.signal,
        };
        let (snapshot, live_files) = self.shared.snapshot(stream).await;
        let sources = scan_sources(&snapshot, &scan);
        // A batch whose trace set holds none of the requested traces is not
        // even filtered: the chunks and the persisted rows carry their set.
        let wanted: Option<std::collections::HashSet<[u8; 16]>> =
            scan.trace_ids.as_ref().map(|ids| {
                ids.iter()
                    .filter_map(|id| <[u8; 16]>::try_from(id.as_slice()).ok())
                    .collect()
            });
        let mut batches = Vec::new();
        let mut distinct = scan
            .distinct
            .map(|cap| Distinct::for_request(&scan, cap))
            .transpose()?;
        let mut skipped = 0u64;
        for source in &sources {
            if let (Some(wanted), Some(traces)) = (&wanted, &source.traces) {
                if !wanted.iter().any(|id| traces.binary_search(id).is_ok()) {
                    skipped += 1;
                    continue;
                }
            }
            let kept =
                apply(&source.batch, &scan).map_err(|e| Status::invalid_argument(e.to_string()))?;
            if kept.num_rows() == 0 {
                continue;
            }
            match &mut distinct {
                Some(distinct) => distinct.fold(&kept)?,
                None => batches.push(kept),
            }
        }
        if let Some(distinct) = distinct {
            batches.extend(distinct.finish()?);
        }
        self.shared
            .stats
            .hot_batches_skipped
            .fetch_add(skipped, std::sync::atomic::Ordering::Relaxed);
        // Every file id whose rows are (or will be) in this answer. Excluding
        // an id that is not in the catalog yet is harmless and closes the race
        // where it appears while the querier plans.
        let persisted: Vec<String> = live_files
            .into_iter()
            .chain(snapshot.flushing.iter().map(|(id, _)| *id))
            .chain(snapshot.persisted.iter().map(|p| p.file))
            .chain(snapshot.series.flushing.iter().map(|(id, _)| *id))
            .chain(snapshot.series.persisted.iter().map(|(id, _)| *id))
            .map(|id| id.0.to_string())
            .collect();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        debug!(tenant = %scan.tenant.0, signal = ?scan.signal, rows, persisted = persisted.len(), "hot scan");

        let full = scan_schema(&scan);
        let schema = match &scan.columns {
            Some(columns) => {
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
            None => full,
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
            distinct: None,
            signal: RowSignal::Traces,
            table: None,
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
        assert_eq!(decoded.distinct, None);
        assert_eq!(decoded.signal, RowSignal::Traces, "spans, as before logs");
    }

    /// A scan of logs names its signal; one of spans keeps the ticket an
    /// ingester without logs reads.
    #[test]
    fn a_ticket_names_its_signal_only_for_logs() {
        let spans = HotScanRequest {
            tenant: TenantId::new(),
            ts_min: None,
            ts_max: None,
            services: None,
            columns: None,
            trace_ids: None,
            span_ids: None,
            distinct: None,
            signal: RowSignal::Traces,
            table: None,
        };
        let encoded = String::from_utf8(spans.to_ticket().ticket.to_vec()).unwrap();
        assert!(!encoded.contains("signal"), "{encoded}");
        let logs = HotScanRequest {
            signal: RowSignal::Logs,
            ..spans
        };
        let decoded = HotScanRequest::from_ticket(&logs.to_ticket()).unwrap();
        assert_eq!(decoded, logs);
    }
}
