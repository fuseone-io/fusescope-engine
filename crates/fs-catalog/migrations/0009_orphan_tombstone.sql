-- A path whose object the sweep deleted stays refused to registration as a
-- tombstone, so a writer that never saw the refusal before the delete
-- cannot write the path again under a delete still in flight.

alter table engine_orphan add column deleted_at timestamptz;

drop index if exists engine_orphan_retry_idx;
create index engine_orphan_retry_idx on engine_orphan (retry_after) where deleting and deleted_at is null;
create index engine_orphan_tombstone_idx on engine_orphan (deleted_at) where deleted_at is not null;
