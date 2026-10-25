-- Ingest tokens. Only the sha256 of the token is stored; the plaintext is shown
-- once at creation. Revocation is a timestamp so audits keep the row.

create table engine_tenant_token (
    token_hash  bytea primary key check (octet_length(token_hash) = 32),
    tenant_id   uuid not null references engine_tenant(id),
    name        text not null,
    created_at  timestamptz not null default now(),
    revoked_at  timestamptz
);

create index engine_tenant_token_tenant_idx on engine_tenant_token (tenant_id);
