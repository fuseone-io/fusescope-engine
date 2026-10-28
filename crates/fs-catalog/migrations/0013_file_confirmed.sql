-- A file is cold for compaction once the writer that registered it saw the
-- registration committed and began its hot retention. created_at is taken
-- before the commit, which may come much later. A writer of the previous
-- version does not confirm: the default keeps its files on the registration
-- stamp, as before.

alter table engine_file add column confirmed_at timestamptz;
update engine_file set confirmed_at = created_at;
alter table engine_file alter column confirmed_at set default clock_timestamp();
