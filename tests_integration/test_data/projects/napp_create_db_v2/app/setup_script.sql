CREATE OR ALTER VERSIONED SCHEMA core;

create or replace procedure core.create_db()
    returns boolean
    language sql
    as $$
        begin
            create or replace database DB_NAME_PLACEHOLDER;
            return true;
        end;
    $$;
