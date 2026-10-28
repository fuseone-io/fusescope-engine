-- A marked orphan whose delete fails waits before it is tried again, so the
-- ones that always fail do not keep every later one from being tried.

alter table engine_orphan
    add column attempts integer not null default 0,
    add column retry_after timestamptz not null default now();

drop index if exists engine_orphan_due_idx;
create index engine_orphan_due_idx on engine_orphan (first_seen) where not deleting;
create index engine_orphan_retry_idx on engine_orphan (retry_after) where deleting;
