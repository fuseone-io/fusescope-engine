-- A lease generation grows by one each time the lease changes hands, so a
-- holder can fence its writes: a swap carrying an old generation is refused.

alter table engine_lease add column generation bigint not null default 0;
