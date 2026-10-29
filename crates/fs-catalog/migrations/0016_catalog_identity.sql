-- One identity per catalog, given once: an engine marks the bucket prefix
-- it writes with it, so two catalogs never share one prefix and one never
-- sweeps the other's objects.

create table engine_catalog (
    singleton   boolean primary key default true check (singleton),
    id          uuid not null,
    created_at  timestamptz not null default now()
);

insert into engine_catalog (singleton, id) values (true, gen_random_uuid());
