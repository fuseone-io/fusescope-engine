//! Arrow Flight SQL front end.
//!
//! Clients authenticate with `authorization: Bearer <query token>`. A
//! statement is planned on `GetFlightInfo` (so schema and errors surface
//! early) and executed on `DoGet`. The ticket carries the tenant id, and
//! `DoGet` re-authenticates and refuses a ticket from another tenant.

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use fs_catalog::{Catalog, TenantId, TokenScope};
use futures_util::{StreamExt, TryStreamExt};
use prost::Message;
use tokio::sync::watch;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::QueryError;
use crate::querier::Querier;

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
fn encode_handle(tenant: TenantId, sql: &str) -> bytes::Bytes {
    let mut out = Vec::with_capacity(16 + sql.len());
    out.extend_from_slice(tenant.0.as_bytes());
    out.extend_from_slice(sql.as_bytes());
    bytes::Bytes::from(out)
}

fn decode_handle(handle: &[u8]) -> Result<(TenantId, String), Status> {
    let (id, sql) = handle
        .split_at_checked(16)
        .ok_or_else(|| Status::invalid_argument("bad ticket"))?;
    let uuid = uuid::Uuid::from_slice(id).map_err(|_| Status::invalid_argument("bad ticket"))?;
    let sql = std::str::from_utf8(sql).map_err(|_| Status::invalid_argument("bad ticket"))?;
    Ok((TenantId(uuid), sql.to_owned()))
}

fn status_of(error: &QueryError) -> Status {
    match error {
        QueryError::NotAllowed(_) => Status::permission_denied(error.to_string()),
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
        let tenant = self.authenticate(request.metadata()).await?;
        let (_, plan, _) = self
            .querier
            .plan(tenant, &query.query)
            .await
            .map_err(|e| status_of(&e))?;
        let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        let ticket = TicketStatementQuery {
            statement_handle: encode_handle(tenant, &query.query),
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
        let tenant = self.authenticate(request.metadata()).await?;
        let (ticket_tenant, sql) = decode_handle(&ticket.statement_handle)?;
        if ticket_tenant != tenant {
            warn!(tenant = %tenant.0, "ticket from another tenant refused");
            return Err(Status::permission_denied(
                "ticket does not belong to this tenant",
            ));
        }
        let limits = self.querier.limits();
        let (stream, warnings) = self
            .querier
            .execute(tenant, &sql)
            .await
            .map_err(|e| status_of(&e))?;
        let schema = stream.schema();
        let max_rows = limits.max_rows;
        let mut rows = 0usize;
        let deadline = tokio::time::Instant::now() + limits.timeout;
        let limited = stream.map(
            move |batch| -> Result<arrow::record_batch::RecordBatch, FlightError> {
                let batch = batch.map_err(|e| FlightError::ExternalError(Box::new(e)))?;
                rows += batch.num_rows();
                if rows > max_rows {
                    return Err(FlightError::ExternalError(Box::new(
                        QueryError::TooManyRows(max_rows),
                    )));
                }
                if tokio::time::Instant::now() > deadline {
                    return Err(FlightError::ExternalError(Box::new(QueryError::Timeout(
                        limits.timeout,
                    ))));
                }
                Ok(batch)
            },
        );
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(limited)
            .map_err(|e| Status::internal(e.to_string()));
        let mut response = Response::new(Box::pin(encoded)
            as Pin<
                Box<
                    dyn futures_util::Stream<Item = Result<arrow_flight::FlightData, Status>>
                        + Send
                        + 'static,
                >,
            >);
        let warnings = warnings.list();
        if !warnings.is_empty() {
            if let Ok(value) = warnings.join("; ").parse() {
                response.metadata_mut().insert(PARTIAL_HEADER, value);
            }
        }
        Ok(response)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_round_trip_and_rejects_garbage() {
        let tenant = TenantId::new();
        let handle = encode_handle(tenant, "select 1");
        assert_eq!(
            decode_handle(&handle).unwrap(),
            (tenant, "select 1".to_owned())
        );
        assert!(decode_handle(b"short").is_err());
    }
}
