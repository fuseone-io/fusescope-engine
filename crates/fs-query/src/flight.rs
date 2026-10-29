//! Arrow Flight SQL front end.
//!
//! Clients authenticate with `authorization: Bearer <query token>`. A
//! statement is planned on `GetFlightInfo` (so schema and errors surface
//! early), kept by the querier, and executed on `DoGet` from that plan.
//! The ticket is the tenant id followed by the SQL, unchanged since before
//! plans were kept, so a ticket issued by either version executes on the
//! other: `DoGet` re-authenticates, refuses a ticket from another tenant,
//! and plans the SQL again only when no kept plan matches it. The typed
//! actions of [`crate::actions`] arrive through `DoAction` and answer in
//! Arrow IPC.

use std::pin::Pin;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{Action, ActionType, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::execution::SendableRecordBatchStream;
use fs_catalog::{Catalog, TenantId, TokenScope};
use futures_util::{Future, Stream, StreamExt, TryStreamExt};
use prost::Message;
use tokio::sync::watch;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::QueryError;
use crate::actions::{TRACE_LOOKUP, TraceLookup};
use crate::querier::{Querier, QueryLimits, QueryWarnings};

/// Response header set when the result may be incomplete, with the reasons.
pub const PARTIAL_HEADER: &str = "x-fusescope-partial";

/// Flight SQL service over a [`Querier`].
#[derive(Clone)]
pub struct FlightSql {
    querier: Querier,
    catalog: Catalog,
}

impl FlightSql {
    /// Service over `querier`, authenticating against `catalog`.
    pub fn new(querier: Querier, catalog: Catalog) -> Self {
        Self { querier, catalog }
    }

    /// Serves until `shutdown` flips. Sends the bound address on `bound`.
    ///
    /// # Errors
    /// Bind or transport failure.
    pub async fn serve(
        self,
        listen: std::net::SocketAddr,
        mut shutdown: watch::Receiver<bool>,
        bound: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
    ) -> Result<(), QueryError> {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|source| QueryError::Bind {
                addr: listen.to_string(),
                source,
            })?;
        let addr = listener.local_addr().map_err(|source| QueryError::Bind {
            addr: "local_addr".into(),
            source,
        })?;
        info!(%addr, "flight sql listening");
        if let Some(tx) = bound {
            let _ = tx.send(addr);
        }
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(self))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown.changed().await;
                },
            )
            .await?;
        info!("flight sql stopped");
        Ok(())
    }

    /// Resolves the bearer token under the request's `deadline`, so a
    /// catalog that does not answer ends the request like any other step.
    async fn authenticate_by(
        &self,
        metadata: &MetadataMap,
        deadline: tokio::time::Instant,
        limits: QueryLimits,
    ) -> Result<TenantId, Status> {
        tokio::time::timeout_at(deadline, self.authenticate(metadata))
            .await
            .unwrap_or_else(|_| Err(status_of(&QueryError::Timeout(limits.timeout))))
    }

    async fn authenticate(&self, metadata: &MetadataMap) -> Result<TenantId, Status> {
        let token = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, t)| t.trim())
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        self.catalog
            .resolve_token(token, TokenScope::Query)
            .await
            .map_err(|e| Status::unavailable(format!("token lookup failed: {e}")))?
            .ok_or_else(|| Status::unauthenticated("unknown or revoked token"))
    }
}

/// Ticket payload: tenant id then SQL text.
fn encode_ticket(tenant: TenantId, sql: &str) -> bytes::Bytes {
    let mut out = Vec::with_capacity(16 + sql.len());
    out.extend_from_slice(tenant.0.as_bytes());
    out.extend_from_slice(sql.as_bytes());
    bytes::Bytes::from(out)
}

fn decode_ticket(handle: &[u8]) -> Result<(TenantId, String), Status> {
    let bad = || Status::invalid_argument("bad ticket");
    let (id, sql) = handle.split_at_checked(16).ok_or_else(bad)?;
    let tenant = uuid::Uuid::from_slice(id).map_err(|_| bad())?;
    let sql = std::str::from_utf8(sql).map_err(|_| bad())?;
    Ok((TenantId(tenant), sql.to_owned()))
}

/// Stands in for a deadline the clock cannot represent, as tokio does for
/// a timer with no end: thirty years from now.
const FAR_FUTURE: std::time::Duration = std::time::Duration::from_secs(30 * 365 * 24 * 60 * 60);

/// When a request that arrives now must be done, by `limits.timeout`; a
/// timeout the clock cannot add is [`FAR_FUTURE`].
fn deadline_of(limits: QueryLimits) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(limits.timeout)
        .unwrap_or_else(|| now + FAR_FUTURE)
}

/// Runs `work`, a step of a request, against its `deadline`.
async fn within<T>(
    deadline: tokio::time::Instant,
    limits: QueryLimits,
    work: impl Future<Output = Result<T, QueryError>>,
) -> Result<T, Status> {
    tokio::time::timeout_at(deadline, work)
        .await
        .unwrap_or_else(|_| Err(QueryError::Timeout(limits.timeout)))
        .map_err(|e| status_of(&e))
}

/// Applies the row cap to `stream` and races every wait for a batch
/// against `deadline`, so a stalled upstream ends in `Timeout` too.
fn bound_rows(
    stream: SendableRecordBatchStream,
    limits: QueryLimits,
    deadline: tokio::time::Instant,
) -> impl Stream<Item = Result<RecordBatch, QueryError>> + Send + 'static {
    futures_util::stream::try_unfold((stream, 0usize), move |(mut stream, rows)| async move {
        let next = tokio::time::timeout_at(deadline, stream.next())
            .await
            .map_err(|_| QueryError::Timeout(limits.timeout))?;
        let Some(batch) = next else {
            return Ok(None);
        };
        let batch = batch?;
        let rows = rows + batch.num_rows();
        if rows > limits.max_rows {
            return Err(QueryError::TooManyRows(limits.max_rows));
        }
        Ok(Some((batch, (stream, rows))))
    })
}

/// Encodes `batches` as one Arrow IPC stream, one `Result` body per batch
/// with the schema ahead of the first and the end-of-stream marker last.
fn ipc_results(
    schema: &SchemaRef,
    batches: impl Stream<Item = Result<RecordBatch, QueryError>> + Send + 'static,
) -> Result<impl Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static, Status> {
    let internal = |e: arrow::error::ArrowError| Status::internal(e.to_string());
    let writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema).map_err(internal)?;
    let body = |writer: &mut arrow::ipc::writer::StreamWriter<Vec<u8>>| arrow_flight::Result {
        body: std::mem::take(writer.get_mut()).into(),
    };
    Ok(futures_util::stream::try_unfold(
        (writer, Box::pin(batches), false),
        move |(mut writer, mut batches, done)| async move {
            if done {
                return Ok(None);
            }
            match batches.next().await {
                Some(Ok(batch)) => {
                    writer.write(&batch).map_err(internal)?;
                    Ok(Some((body(&mut writer), (writer, batches, false))))
                }
                Some(Err(e)) => Err(status_of(&e)),
                None => {
                    writer.finish().map_err(internal)?;
                    Ok(Some((body(&mut writer), (writer, batches, true))))
                }
            }
        },
    ))
}

fn announce_partial<T>(response: &mut Response<T>, warnings: &QueryWarnings) {
    let warnings = warnings.list();
    if !warnings.is_empty() {
        if let Ok(value) = warnings.join("; ").parse() {
            response.metadata_mut().insert(PARTIAL_HEADER, value);
        }
    }
}

/// The status of an error met while encoding a `DoGet` stream: a query
/// error keeps its code, anything else is internal.
fn status_of_flight(error: &FlightError) -> Status {
    match error {
        FlightError::ExternalError(inner) => inner
            .downcast_ref::<QueryError>()
            .map_or_else(|| Status::internal(error.to_string()), status_of),
        _ => Status::internal(error.to_string()),
    }
}

fn status_of(error: &QueryError) -> Status {
    match error {
        QueryError::NotAllowed(_) => Status::permission_denied(error.to_string()),
        QueryError::InvalidAction(_) => Status::invalid_argument(error.to_string()),
        QueryError::OverBudget { .. } | QueryError::TooManyRows(_) => {
            Status::resource_exhausted(error.to_string())
        }
        QueryError::Timeout(_) => Status::deadline_exceeded(error.to_string()),
        QueryError::UnknownTenant => Status::unauthenticated(error.to_string()),
        QueryError::DataFusion(e) => match e {
            datafusion::error::DataFusionError::External(inner) => inner
                .downcast_ref::<QueryError>()
                .map_or_else(|| Status::internal(e.to_string()), status_of),
            datafusion::error::DataFusionError::Plan(_)
            | datafusion::error::DataFusionError::SQL(_, _)
            | datafusion::error::DataFusionError::SchemaError(_, _) => {
                Status::invalid_argument(e.to_string())
            }
            _ => Status::internal(e.to_string()),
        },
        _ => Status::internal(error.to_string()),
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSql {
    type FlightService = FlightSql;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let limits = self.querier.limits();
        let deadline = deadline_of(limits);
        let tenant = self
            .authenticate_by(request.metadata(), deadline, limits)
            .await?;
        let schema: SchemaRef =
            within(deadline, limits, self.querier.prepare(tenant, &query.query)).await?;
        let ticket = TicketStatementQuery {
            statement_handle: encode_ticket(tenant, &query.query),
        };
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(
                    arrow_flight::sql::Any::pack(&ticket)
                        .map_err(|e| Status::internal(e.to_string()))?
                        .encode_to_vec(),
                )),
            )
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let limits = self.querier.limits();
        let deadline = deadline_of(limits);
        let tenant = self
            .authenticate_by(request.metadata(), deadline, limits)
            .await?;
        let (ticket_tenant, sql) = decode_ticket(&ticket.statement_handle)?;
        if ticket_tenant != tenant {
            warn!(tenant = %tenant.0, "ticket from another tenant refused");
            return Err(Status::permission_denied(
                "ticket does not belong to this tenant",
            ));
        }
        let kept = within(
            deadline,
            limits,
            self.querier.execute_prepared(tenant, &sql),
        )
        .await?;
        let (stream, warnings) = match kept {
            Some(kept) => kept,
            None => within(deadline, limits, self.querier.execute(tenant, &sql)).await?,
        };
        let schema = stream.schema();
        let limited = bound_rows(stream, limits, deadline)
            .map_err(|e| FlightError::ExternalError(Box::new(e)));
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(limited)
            .map_err(|e| status_of_flight(&e));
        let mut response = Response::new(Box::pin(encoded)
            as Pin<
                Box<
                    dyn futures_util::Stream<Item = Result<arrow_flight::FlightData, Status>>
                        + Send
                        + 'static,
                >,
            >);
        announce_partial(&mut response, &warnings);
        Ok(response)
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let limits = self.querier.limits();
        let deadline = deadline_of(limits);
        let tenant = self
            .authenticate_by(request.metadata(), deadline, limits)
            .await?;
        let action = request.get_ref();
        if action.r#type != TRACE_LOOKUP {
            return Err(Status::unimplemented(format!(
                "unknown action {:?}",
                action.r#type
            )));
        }
        let query = TraceLookup::parse(&action.body)
            .and_then(|lookup| lookup.query())
            .map_err(|e| status_of(&e))?;
        let (stream, warnings) =
            within(deadline, limits, self.querier.trace_lookup(tenant, query)).await?;
        let schema = stream.schema();
        let results = ipc_results(&schema, bound_rows(stream, limits, deadline))?;
        let mut response = Response::new(Box::pin(results)
            as Pin<
                Box<
                    dyn futures_util::Stream<Item = Result<arrow_flight::Result, Status>>
                        + Send
                        + 'static,
                >,
            >);
        announce_partial(&mut response, &warnings);
        Ok(response)
    }

    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        Some(vec![Ok(ActionType {
            r#type: TRACE_LOOKUP.into(),
            description: "Spans of one trace in timestamp order, as an Arrow IPC stream; \
                          body {\"trace_id\": hex, \"start_unix_nano\"?, \"end_unix_nano\"?}"
                .into(),
        })])
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn ticket_round_trip_and_rejects_garbage() {
        let tenant = TenantId::new();
        let ticket = encode_ticket(tenant, "select 1");
        assert_eq!(
            decode_ticket(&ticket).unwrap(),
            (tenant, "select 1".to_owned())
        );
        assert_eq!(
            ticket.len(),
            16 + "select 1".len(),
            "nothing but tenant and SQL"
        );
        assert!(decode_ticket(b"short").is_err());
    }

    fn batch_stream(
        batches: Vec<RecordBatch>,
        then_stall: bool,
    ) -> (SendableRecordBatchStream, usize) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        let count = batches.len();
        let ready = futures_util::stream::iter(batches.into_iter().map(Ok));
        let stream: Pin<Box<dyn Stream<Item = datafusion::error::Result<RecordBatch>> + Send>> =
            if then_stall {
                Box::pin(ready.chain(futures_util::stream::pending()))
            } else {
                Box::pin(ready)
            };
        (
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
            ),
            count,
        )
    }

    fn one_row() -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap()
    }

    #[test]
    fn a_deadline_beyond_the_clock_is_the_far_future() {
        let forever = QueryLimits {
            timeout: std::time::Duration::MAX,
            ..QueryLimits::default()
        };
        let deadline = deadline_of(forever);
        assert!(
            deadline
                > tokio::time::Instant::now() + std::time::Duration::from_secs(365 * 24 * 60 * 60)
        );
        assert!(deadline <= tokio::time::Instant::now() + FAR_FUTURE);
    }

    #[test]
    fn a_query_error_met_while_encoding_keeps_its_code() {
        let external = |e: QueryError| FlightError::ExternalError(Box::new(e));
        assert_eq!(
            status_of_flight(&external(QueryError::Timeout(
                std::time::Duration::from_secs(1)
            )))
            .code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(
            status_of_flight(&external(QueryError::TooManyRows(3))).code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            status_of_flight(&FlightError::NotYetImplemented("x".into())).code(),
            tonic::Code::Internal
        );
    }

    #[tokio::test]
    async fn a_stalled_stream_ends_in_timeout_at_the_deadline() {
        let limits = QueryLimits {
            timeout: std::time::Duration::from_millis(50),
            ..QueryLimits::default()
        };
        let (stream, _) = batch_stream(vec![one_row()], true);
        let mut bounded = Box::pin(bound_rows(stream, limits, deadline_of(limits)));
        assert!(
            bounded.next().await.unwrap().is_ok(),
            "the ready batch flows"
        );
        let stalled = tokio::time::timeout(std::time::Duration::from_secs(5), bounded.next())
            .await
            .expect("the deadline, not the test, ends the wait")
            .unwrap();
        assert!(
            matches!(stalled, Err(QueryError::Timeout(_))),
            "{stalled:?}"
        );

        let (stream, count) = batch_stream(vec![one_row(), one_row()], false);
        let drained: Vec<_> = bound_rows(stream, limits, deadline_of(limits))
            .collect()
            .await;
        assert_eq!(drained.len(), count);
        assert!(drained.iter().all(Result::is_ok));
    }
}
