//! Change notifications over `LISTEN engine_file_changed`.

use sqlx::postgres::{PgListener, PgPool};
use uuid::Uuid;

use crate::CatalogError;
use crate::types::{Signal, TenantId};

/// Channel the `engine_file` trigger notifies on.
pub const CHANNEL: &str = "engine_file_changed";

/// A file of this tenant and signal was inserted, updated or deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileChange {
    /// Tenant whose files changed.
    pub tenant_id: TenantId,
    /// Signal whose files changed.
    pub signal: Signal,
}

impl FileChange {
    /// Parses the trigger payload `"<tenant uuid>:<signal code>"`.
    pub fn parse(payload: &str) -> Option<Self> {
        let (tenant, signal) = payload.split_once(':')?;
        let tenant_id = TenantId(Uuid::parse_str(tenant).ok()?);
        let signal = Signal::from_code(signal.parse().ok()?).ok()?;
        Some(Self { tenant_id, signal })
    }
}

/// A subscription to file changes. Holds one connection taken from the pool
/// for as long as it lives, so drop it before closing the pool.
///
/// Notifications can be lost across reconnects, so a cache built on this must
/// also refresh on a timer (DESIGN.md: every 5 s).
pub struct Watcher {
    listener: PgListener,
}

impl Watcher {
    pub(crate) async fn connect(pool: &PgPool) -> Result<Self, CatalogError> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(CHANNEL).await?;
        Ok(Self { listener })
    }

    /// Waits for the next change. Payloads that do not parse are skipped.
    ///
    /// # Errors
    /// Connection failure.
    pub async fn next(&mut self) -> Result<FileChange, CatalogError> {
        loop {
            let notification = self.listener.recv().await?;
            if let Some(change) = FileChange::parse(notification.payload()) {
                return Ok(change);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_trigger_payload() {
        let id = Uuid::new_v4();
        let change = FileChange::parse(&format!("{id}:2")).unwrap();
        assert_eq!(
            change,
            FileChange {
                tenant_id: TenantId(id),
                signal: Signal::Logs
            }
        );
        assert_eq!(FileChange::parse("nonsense"), None);
        assert_eq!(FileChange::parse(&format!("{id}:99")), None);
        assert_eq!(FileChange::parse("not-a-uuid:1"), None);
    }
}
