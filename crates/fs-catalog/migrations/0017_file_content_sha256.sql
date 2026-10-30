-- The sha256 of the object a row names, written by the engine that uploaded
-- it. A replay that meets an object already at the path compares its bytes
-- against this digest instead of its size alone. Rows registered before
-- this version stay without one; queriers never read the column, so the
-- change notification leaves it out.

alter table engine_file add column content_sha256 bytea
    check (content_sha256 is null or octet_length(content_sha256) = 32);
