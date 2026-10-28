-- An orphan is marked before its object is deleted, under its row lock, so
-- a registration racing the sweep either commits first, which stops the
-- mark, or finds the mark and is refused.

alter table engine_orphan add column deleting boolean not null default false;

create index engine_orphan_due_idx on engine_orphan (first_seen) where not deleting;
