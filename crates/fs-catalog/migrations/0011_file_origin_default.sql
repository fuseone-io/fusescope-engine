-- A server of the previous version inserts files without an origin while a
-- newer one has already migrated: the origin defaults to the id, which is
-- what every file registered before origins had.

create function engine_file_origin_default() returns trigger
language plpgsql as $$
begin
    if new.origin is null then
        new.origin := new.id;
    end if;
    return new;
end
$$;

create trigger engine_file_origin_default
    before insert on engine_file
    for each row execute function engine_file_origin_default();
