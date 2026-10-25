//! The DESIGN.md query suite over Flight SQL.

use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures_util::TryStreamExt;
use serde::Serialize;
use tonic::transport::Channel;

use crate::stats::{Samples, fmt};

/// Parameters of a query run.
#[derive(Debug, Clone)]
pub struct QueryParams {
    /// `host:port` of Flight SQL.
    pub endpoint: String,
    /// Query token.
    pub token: String,
    /// Repetitions per query.
    pub iterations: usize,
}

/// One query's numbers.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    /// Name from DESIGN.md.
    pub name: String,
    /// SQL run.
    pub sql: String,
    /// Rows returned by the last iteration.
    pub rows: usize,
    /// Milliseconds.
    pub p50_ms: f64,
    /// 95th percentile.
    pub p95_ms: f64,
    /// Worst.
    pub max_ms: f64,
    /// Error, when the query failed.
    pub error: Option<String>,
}

async fn run_one(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
    let info = client.execute(sql.to_owned(), None).await?;
    let mut out = Vec::new();
    for endpoint in info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let batches: Vec<RecordBatch> = client.do_get(ticket).await?.try_collect().await?;
            out.extend(batches);
        }
    }
    Ok(out)
}

/// Finds a trace id and a service to parameterise the suite.
async fn sample_ids(
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let batches = run_one(
        client,
        "select encode(trace_id, 'hex') as trace_hex, arrow_cast(service_name, 'Utf8') as svc from spans where timestamp > now() - interval '24 hours' limit 1",
    )
    .await?;
    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or("no spans in the last 24 hours; run `fs-bench load` first")?;
    let trace = batch.column(0).as_string::<i32>().value(0).to_owned();
    let service = batch.column(1).as_string::<i32>().value(0).to_owned();
    Ok((trace, service))
}

/// Runs the suite and returns one result per query.
///
/// # Errors
/// Connection failure, or an empty table.
pub async fn run(
    params: QueryParams,
) -> Result<Vec<QueryResult>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = Channel::from_shared(format!("http://{}", params.endpoint))?
        .connect()
        .await?;
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token(params.token.clone());
    let (trace, service) = sample_ids(&mut client).await?;

    let suite: Vec<(&str, String)> = vec![
        ("trace by id", format!("select * from spans where trace_id = decode('{trace}', 'hex')")),
        (
            "span list, 1 service, 15 min, promoted attribute",
            format!("select timestamp, name, duration_nano, http_route from spans where service_name = '{service}' and timestamp > now() - interval '15 minutes' and http_method = 'GET' order by timestamp desc limit 100"),
        ),
        (
            "p99 latency per service, 24 h",
            "select service_name, approx_percentile_cont(duration_nano, 0.99) as p99, count(*) as n from spans where timestamp > now() - interval '24 hours' group by service_name order by n desc".into(),
        ),
        (
            "dashboard: rate and errors per service, last 5 min",
            "select service_name, count(*) as n, sum(case when has_error then 1 else 0 end) as errors from spans where timestamp > now() - interval '5 minutes' group by service_name".into(),
        ),
        (
            "top operations by p95, 1 h",
            "select service_name, name, approx_percentile_cont(duration_nano, 0.95) as p95 from spans where timestamp > now() - interval '1 hour' group by service_name, name order by p95 desc limit 20".into(),
        ),
    ];

    let mut results = Vec::new();
    for (name, sql) in suite {
        let mut samples = Samples::default();
        let mut rows = 0;
        let mut error = None;
        for _ in 0..params.iterations {
            let start = Instant::now();
            match run_one(&mut client, &sql).await {
                Ok(batches) => {
                    rows = batches.iter().map(RecordBatch::num_rows).sum();
                    samples.push(start.elapsed());
                }
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            }
        }
        results.push(QueryResult {
            name: name.to_owned(),
            sql,
            rows,
            p50_ms: samples.percentile(50.0).as_secs_f64() * 1e3,
            p95_ms: samples.percentile(95.0).as_secs_f64() * 1e3,
            max_ms: samples.percentile(100.0).as_secs_f64() * 1e3,
            error,
        });
    }
    Ok(results)
}

/// Prints the suite as a markdown table.
pub fn print(results: &[QueryResult]) {
    println!("| query | rows | p50 | p95 | max |");
    println!("|---|---:|---:|---:|---:|");
    for r in results {
        match &r.error {
            Some(e) => println!("| {} | error | {e} | | |", r.name),
            None => println!(
                "| {} | {} | {} | {} | {} |",
                r.name,
                r.rows,
                fmt(Duration::from_secs_f64(r.p50_ms / 1e3)),
                fmt(Duration::from_secs_f64(r.p95_ms / 1e3)),
                fmt(Duration::from_secs_f64(r.max_ms / 1e3))
            ),
        }
    }
}
