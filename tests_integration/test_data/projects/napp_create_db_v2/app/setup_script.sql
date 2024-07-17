-- This is the v2 version of the napp_create_db_v1 project

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
