-- A confirmation changes only confirmed_at, which queriers do not read:
-- the change notification is for the columns they do.

drop trigger engine_file_changed on engine_file;
create trigger engine_file_changed
    after insert or delete
        or update of tenant_id, signal, level, path, size_bytes, row_count,
                     ts_min, ts_max, services, summary, state, quarantined_at, origin
    on engine_file
    for each row execute function engine_file_notify();
