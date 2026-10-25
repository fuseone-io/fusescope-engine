-- Engine catalog, version 1. See DESIGN.md section 3.2.
-- The catalog is the single source of truth for which files exist in the bucket.

create table engine_tenant (
    id              uuid primary key,
    name            text not null unique,
    retention_days  integer not null default 15 check (retention_days > 0),
    max_keys_hour   integer not null default 5000 check (max_keys_hour > 0),
    bytes_budget    bigint not null default 10737418240 check (bytes_budget > 0),
    qps_limit       integer not null default 50 check (qps_limit > 0),
    created_at      timestamptz not null default now(),
    deleted_at      timestamptz
);

create table engine_file (
    id              uuid primary key,
    tenant_id       uuid not null references engine_tenant(id),
    signal          smallint not null,
    level           smallint not null check (level between 0 and 2),
    path            text not null unique,
    size_bytes      bigint not null check (size_bytes >= 0),
    row_count       bigint not null check (row_count >= 0),
    ts_min          timestamptz not null,
    ts_max          timestamptz not null check (ts_max >= ts_min),
    services        text[] not null default '{}',
    summary         bytea,
    state           smallint not null check (state in (0, 1, 2)),
    created_at      timestamptz not null default now(),
    quarantined_at  timestamptz
);

-- Querier lookups: visible files of one tenant and signal overlapping a time range.
create index engine_file_visible_idx
    on engine_file (tenant_id, signal, ts_min, ts_max)
    where state = 1;

-- Compactor purge: quarantined files past the in-flight read TTL.
create index engine_file_quarantined_idx
    on engine_file (quarantined_at)
    where state = 2;

create table engine_compaction (
    id          uuid primary key,
    tenant_id   uuid not null references engine_tenant(id),
    signal      smallint not null,
    inputs      uuid[] not null,
    outputs     uuid[] not null,
    created_at  timestamptz not null default now()
);

create table engine_ingester (
    id              text primary key,
    address         text not null,
    last_heartbeat  timestamptz not null default now()
);

-- Queriers refresh their in-memory file list on this channel. The payload
-- identifies the tenant and signal so a querier refreshes only what changed.
create function engine_file_notify() returns trigger
language plpgsql as $$
begin
    perform pg_notify(
        'engine_file_changed',
        coalesce(new.tenant_id, old.tenant_id)::text || ':' || coalesce(new.signal, old.signal)::text
    );
    return null;
end
$$;

create trigger engine_file_changed
    after insert or update or delete on engine_file
    for each row execute function engine_file_notify();
