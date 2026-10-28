-- Named leases, one row each: the compactor takes `compactor` so a single
-- instance per deployment rewrites files. A lease is taken or renewed in one
-- statement, conditioned on being free, expired or already the holder's.

create table engine_lease (
    name        text primary key,
    holder      text not null,
    acquired_at timestamptz not null default now(),
    expires_at  timestamptz not null
);
