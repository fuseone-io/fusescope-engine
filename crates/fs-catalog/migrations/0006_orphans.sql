-- Objects seen in the bucket without a file row. An object is deleted only
-- once it has been seen without a row for longer than the orphan TTL, so a
-- flush or compaction still between its upload and its registration keeps
-- its object.

create table engine_orphan (
    path        text primary key,
    first_seen  timestamptz not null default now()
);
