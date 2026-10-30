//! Operator client over Flight: runs a statement or a typed action against
//! a running querier and renders the answer, for smoke tests and support.
//! The ticket and the action bodies are exactly what the Go backend sends,
//! so a problem seen here is the backend's problem too.

use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use arrow_flight::Action;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures_util::TryStreamExt;
use tonic::transport::Channel;

use crate::ServerError;

/// Plans `sql` with `GetFlightInfo` and executes its ticket with `DoGet`,
/// as the backend does.
///
/// # Errors
/// Connection, authentication, planning or execution failure, with the
/// status the querier answered.
pub async fn query(
    addr: &str,
    token: &str,
    sql: &str,
    tls: Option<&fs_auth::ClientTls>,
) -> Result<Vec<RecordBatch>, ServerError> {
    let mut client = connect(addr, token, tls).await?;
    let info = client.execute(sql.to_owned(), None).await.map_err(flight)?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .ok_or_else(|| ServerError::Flight("endpoint without a ticket".into()))?;
        let stream = client.do_get(ticket).await.map_err(flight)?;
        batches.extend(stream.try_collect::<Vec<_>>().await.map_err(flight)?);
    }
    Ok(batches)
}

/// Runs the action `kind` with `body` over `DoAction` and reads its answer,
/// one Arrow IPC stream split across the results.
///
/// # Errors
/// Connection, authentication or action failure, with the status the
/// querier answered, or an answer that is not an IPC stream.
pub async fn action(
    addr: &str,
    token: &str,
    kind: &str,
    body: &str,
    tls: Option<&fs_auth::ClientTls>,
) -> Result<Vec<RecordBatch>, ServerError> {
    let mut client = connect(addr, token, tls).await?;
    let results: Vec<arrow_flight::Result> = client
        .do_action(Action {
            r#type: kind.to_owned(),
            body: body.as_bytes().to_vec().into(),
        })
        .await
        .map_err(flight)?
        .try_collect()
        .await
        .map_err(|e| ServerError::Flight(e.to_string()))?;
    let mut stream = Vec::new();
    for result in results {
        stream.extend_from_slice(&result.body);
    }
    if stream.is_empty() {
        return Ok(Vec::new());
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(stream), None)
        .map_err(|e| ServerError::Flight(format!("answer is not an IPC stream: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::Flight(format!("answer is not an IPC stream: {e}")))
}

/// The batches as a table, followed by the row count.
///
/// # Errors
/// Arrow refusing to format a column.
pub fn render(batches: &[RecordBatch]) -> Result<String, ServerError> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let table = arrow::util::pretty::pretty_format_batches(batches)
        .map_err(|e| ServerError::Flight(e.to_string()))?;
    Ok(format!("{table}\n{rows} rows"))
}

async fn connect(
    addr: &str,
    token: &str,
    tls: Option<&fs_auth::ClientTls>,
) -> Result<FlightSqlServiceClient<Channel>, ServerError> {
    let channel = fs_auth::endpoint(addr, tls)?.connect().await.map_err(|e| {
        ServerError::Flight(format!("connect to {addr}: {}", fs_auth::error_chain(&e)))
    })?;
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token(token.to_owned());
    Ok(client)
}

fn flight(error: arrow_flight::error::FlightError) -> ServerError {
    match error {
        arrow_flight::error::FlightError::Tonic(status) => {
            ServerError::Flight(format!("{:?}: {}", status.code(), status.message()))
        }
        other => ServerError::Flight(other.to_string()),
    }
}
