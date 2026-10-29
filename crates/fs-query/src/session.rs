//! Tenant-scoped DataFusion sessions with a read-only SQL surface.

use std::sync::Arc;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
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
use fs_schema::attribute_keys;

use crate::QueryError;
use crate::derived_table::DerivedTable;
use crate::files::FileCache;
use crate::spans_table::SpansTable;

/// Catalog and schema names every tenant sees.
pub const CATALOG: &str = "fusescope";
/// Schema name.
pub const SCHEMA: &str = "public";
/// Name of the spans table in every session.
pub const SPANS_TABLE: &str = "spans";

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
            SpansTable::new(tenant, Arc::clone(&cache), store_url.clone(), bytes_budget)
                .with_hot_window(hot, warnings)
                .with_trace_index(trace_index),
        ),
    )?;
    schema.register_table(
        attribute_keys::TABLE.into(),
        Arc::new(DerivedTable::new(
            tenant,
            cache,
            store_url.clone(),
            Signal::AttributeKeys,
            attribute_keys::attribute_keys_schema_v1(),
            attribute_keys::check_schema,
        )),
    )?;
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema(SCHEMA, schema)?;
    ctx.register_catalog(CATALOG, catalog);
    Ok(ctx)
}

/// Parses and plans `sql`, refusing anything that is not a read.
///
/// # Errors
/// Non-query statements, or planning failures.
pub async fn plan_read_only(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan, QueryError> {
    let state = ctx.state();
    let statement = state.sql_to_statement(sql, &Dialect::PostgreSQL)?;
    match &statement {
        Statement::Statement(inner) => match inner.as_ref() {
            SqlStatement::Query(_)
            | SqlStatement::Explain { .. }
            | SqlStatement::ExplainTable { .. } => {}
            other => return Err(QueryError::NotAllowed(statement_kind(other).to_owned())),
        },
        Statement::Explain(_) => {}
        Statement::CreateExternalTable(_) => {
            return Err(QueryError::NotAllowed("CREATE EXTERNAL TABLE".into()));
        }
        Statement::CopyTo(_) => return Err(QueryError::NotAllowed("COPY".into())),
        Statement::Reset(_) => return Err(QueryError::NotAllowed("RESET".into())),
    }
    let plan = state.statement_to_plan(statement).await?;
    if !read_only(&plan) {
        return Err(QueryError::NotAllowed(
            "statement writes or changes state".into(),
        ));
    }
    Ok(plan)
}

fn read_only(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::Ddl(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Statement(_)
    )
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
