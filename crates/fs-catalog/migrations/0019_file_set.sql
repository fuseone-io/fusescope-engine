-- The two files a metric flush writes, samples (signal 3) and their
-- series (signal 4), share a set: registered, confirmed, expired and
-- quarantined together. Every metric file has one, no other file does,
-- and a set holds one file of each signal.
alter table engine_file add column file_set uuid;

alter table engine_file
    add constraint engine_file_set_by_signal
    check ((signal in (3, 4)) = (file_set is not null));

create unique index engine_file_set_signal
    on engine_file (tenant_id, file_set, signal)
    where file_set is not null;
