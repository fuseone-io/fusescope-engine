-- A file's registration time is the moment its row was written, not the
-- start of a transaction that may have waited on a lock: the compactor reads
-- it as when the hot window started retaining the file. An origin names one
-- logical flush, so one visible file at most carries it.

alter table engine_file alter column created_at set default clock_timestamp();

drop index if exists engine_file_origin_idx;
create unique index engine_file_origin_idx on engine_file (origin);
