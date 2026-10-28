-- The id a flush was first assigned, kept when the flush changes id. The hot
-- window announces it, so a querier excludes the file however it was
-- registered.

alter table engine_file add column origin uuid;
update engine_file set origin = id;
alter table engine_file alter column origin set not null;
create index engine_file_origin_idx on engine_file (origin);
