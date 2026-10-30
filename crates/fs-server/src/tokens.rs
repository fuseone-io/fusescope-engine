//! The `admin token` commands' output and their audit events. A token is
//! named by the id its listing shows, never by its secret, which is printed
//! once when issued and kept only hashed.

use fs_catalog::TenantToken;
use tracing::info;

/// Default overlap of `admin token rotate`: long enough to roll a fleet of
/// collectors, short enough that a leaked token does not live on.
pub const DEFAULT_ROTATION_OVERLAP_SECS: u64 = 3600;

/// One `key=value` line per field of `token`, its secret never among them.
pub(crate) fn describe(token: &TenantToken) -> String {
    let state = match token.revoked_at {
        None => "live".to_owned(),
        Some(at) if at > chrono::Utc::now() => format!("stops_at={}", at.to_rfc3339()),
        Some(at) => format!("revoked_at={}", at.to_rfc3339()),
    };
    format!(
        "id={} tenant_id={} name={} scope={} created_at={} {state}",
        token.id,
        token.tenant_id.0,
        token.name.escape_debug(),
        token.scope.name(),
        token.created_at.to_rfc3339()
    )
}

/// Logs an operator's action on `token` under the audit target.
pub(crate) fn audit(action: &'static str, token: &TenantToken) {
    info!(
        target: "fusescope::audit",
        action,
        tenant = %token.tenant_id.0,
        token_id = %token.id,
        name = %token.name.escape_debug(),
        scope = token.scope.name(),
        stops_at = ?token.revoked_at,
        "token"
    );
}
