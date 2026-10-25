-- Tokens carry a scope so collectors cannot query and query clients cannot
-- ingest. Existing tokens were all issued for ingest.

alter table engine_tenant_token
    add column scope text not null default 'ingest'
    check (scope in ('ingest', 'query'));
