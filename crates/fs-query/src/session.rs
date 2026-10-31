//! Tenant-scoped DataFusion sessions with a read-only SQL surface.

use std::sync::Arc;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::common::tree_node::TreeNode;
use datafusion::config::Dialect;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionConfig;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use fs_catalog::{Signal, TenantId};
use fs_schema::attribute_keys::{self, KeyTable};
use fs_schema::{service_graph, trace_summary};

use crate::QueryError;
use crate::derived_table::{DerivedTable, VerifiedPaths};
use crate::files::FileCache;
use crate::signal_table::SignalTable;

/// Catalog and schema names every tenant sees.
pub const CATALOG: &str = "fusescope";
/// Schema name.
pub const SCHEMA: &str = "public";
/// Name of the spans table in every session.
pub const SPANS_TABLE: &str = "spans";
/// Name of the logs table in every session.
pub const LOGS_TABLE: &str = "logs";

/// Builds a session that only sees `tenant`'s tables and cannot write.
///
/// # Errors
/// DataFusion registration failure.
#[allow(clippy::too_many_arguments)]
pub fn tenant_session(
    tenant: TenantId,
    cache: Arc<FileCache>,
    hot: Arc<crate::hot::HotWindow>,
    trace_index: Arc<crate::trace_index::TraceIndexCache>,
    warnings: Arc<crate::querier::QueryWarnings>,
    runtime: Arc<RuntimeEnv>,
    store_url: &ObjectStoreUrl,
    bytes_budget: u64,
    batch_size: usize,
    verified: VerifiedPaths,
) -> Result<SessionContext, QueryError> {
    let config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG, SCHEMA)
        .with_information_schema(true)
        .with_batch_size(batch_size)
        .set_str("datafusion.sql_parser.dialect", "postgresql")
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.enable_page_index", true)
        .set_bool("datafusion.execution.parquet.bloom_filter_on_read", true)
        .set_bool("datafusion.execution.parquet.pruning", true);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        // No table functions: nothing may name a path to read.
        .with_table_function_list(Vec::new())
        .build();
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(MemorySchemaProvider::new());
    schema.register_table(
        SPANS_TABLE.into(),
        Arc::new(
            SignalTable::spans(tenant, Arc::clone(&cache), store_url.clone(), bytes_budget)
                .with_hot_window(Arc::clone(&hot), Arc::clone(&warnings))
                .with_trace_index(trace_index),
        ),
    )?;
    schema.register_table(
        LOGS_TABLE.into(),
        Arc::new(
            SignalTable::logs(tenant, Arc::clone(&cache), store_url.clone(), bytes_budget)
                .with_hot_window(hot, warnings),
        ),
    )?;
    schema.register_table(
        attribute_keys::TABLE.into(),
        Arc::new(DerivedTable::new(
            tenant,
            Arc::clone(&cache),
            store_url.clone(),
            Signal::AttributeKeys,
            attribute_keys::table_schema(),
            |schema| attribute_keys::check_schema(schema, KeyTable::Spans).map(|_| ()),
            Arc::clone(&verified),
        )),
    )?;
    schema.register_table(
        KeyTable::Logs.name().into(),
        Arc::new(DerivedTable::new(
            tenant,
            Arc::clone(&cache),
            store_url.clone(),
            Signal::LogAttributeKeys,
            KeyTable::Logs.session_schema(),
            |schema| attribute_keys::check_schema(schema, KeyTable::Logs).map(|_| ()),
            Arc::clone(&verified),
        )),
    )?;
    schema.register_table(
        trace_summary::TABLE.into(),
        Arc::new(
            DerivedTable::new(
                tenant,
                Arc::clone(&cache),
                store_url.clone(),
                Signal::TraceSummary,
                trace_summary::schema(),
                trace_summary::check_schema,
                Arc::clone(&verified),
            )
            // The catalog range is deliberately based on span starts. A bad
            // end timestamp must not keep this hourly contribution forever,
            // so only start predicates can prune whole files here.
            .with_time_columns(&[trace_summary::columns::START]),
        ),
    )?;
    schema.register_table(
        service_graph::TABLE.into(),
        Arc::new(
            DerivedTable::new(
                tenant,
                cache,
                store_url.clone(),
                Signal::ServiceGraph1m,
                service_graph::schema(),
                service_graph::check_schema,
                verified,
            )
            .with_time_columns(&[service_graph::columns::MINUTE]),
        ),
    )?;
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema(SCHEMA, schema)?;
    ctx.register_catalog(CATALOG, catalog);
    ctx.register_udaf(crate::tdigest::quantile_tdigest());
    Ok(ctx)
}

/// Parses and plans `sql`, refusing anything that is not a read: the
/// statement is checked before planning, through every `EXPLAIN` it is
/// wrapped in, and the plan is walked whole afterwards, so a write never
/// reaches execution however it is dressed.
///
/// # Errors
/// Non-query statements, or planning failures.
pub async fn plan_read_only(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan, QueryError> {
    let state = ctx.state();
    let statement = state.sql_to_statement(sql, &Dialect::PostgreSQL)?;
    check_statement(&statement)?;
    let plan = state.statement_to_plan(statement).await?;
    let writes = plan.exists(|node| Ok(!reads(node)))?;
    if writes {
        return Err(QueryError::NotAllowed(
            "statement writes or changes state".into(),
        ));
    }
    Ok(plan)
}

/// True for a plan node that only reads. Every variant is named, so a
/// node DataFusion adds later fails to compile here and gets reviewed
/// instead of passing; `Extension` is refused because it is anything.
fn reads(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_) => true,
        LogicalPlan::Statement(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_) => false,
    }
}

/// Refuses anything but a query, looking through `EXPLAIN` and
/// `EXPLAIN ANALYZE`, which would describe or run what they wrap.
fn check_statement(statement: &Statement) -> Result<(), QueryError> {
    match statement {
        Statement::Statement(inner) => check_sql_statement(inner),
        Statement::Explain(explain) => check_statement(&explain.statement),
        Statement::CreateExternalTable(_) => {
            Err(QueryError::NotAllowed("CREATE EXTERNAL TABLE".into()))
        }
        Statement::CopyTo(_) => Err(QueryError::NotAllowed("COPY".into())),
        Statement::Reset(_) => Err(QueryError::NotAllowed("RESET".into())),
    }
}

fn check_sql_statement(statement: &SqlStatement) -> Result<(), QueryError> {
    match statement {
        SqlStatement::Query(_) | SqlStatement::ExplainTable { .. } => Ok(()),
        SqlStatement::Explain { statement, .. } => check_sql_statement(statement),
        other => Err(QueryError::NotAllowed(statement_kind(other).to_owned())),
    }
}

fn statement_kind(statement: &SqlStatement) -> &'static str {
    match statement {
        SqlStatement::Insert(_) => "INSERT",
        SqlStatement::Update { .. } => "UPDATE",
        SqlStatement::Delete(_) => "DELETE",
        SqlStatement::CreateTable(_) | SqlStatement::CreateView { .. } => "CREATE",
        SqlStatement::Drop { .. } => "DROP",
        SqlStatement::AlterTable { .. } => "ALTER",
        SqlStatement::Copy { .. } => "COPY",
        SqlStatement::Set(_) => "SET",
        _ => "non-query statement",
    }
}
