-- Tombstones are permanent and nothing looks them up by deleted_at.

drop index if exists engine_orphan_tombstone_idx;
