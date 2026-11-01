-- Whether the rows of a file were counted into usage_1h. A file leaving L0
-- is counted by the pass that takes it, which publishes the counts and
-- registers its outputs as counted in one transaction, so every row is
-- counted exactly once. Rows registered before this version read false and
-- are counted by the first pass over them. Retention never expires a file
-- of spans or logs that is not counted. Queriers never read the column, so
-- the change notification leaves it out.

alter table engine_file add column usage_accounted boolean not null default false;
