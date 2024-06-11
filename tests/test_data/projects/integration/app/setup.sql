create application role app_public;
create or alter versioned schema core;

    create or replace procedure core.echo(inp varchar)
    returns varchar
    language sql
    immutable
    as
    $$
    begin
        return inp;
    end;
    $$;

    grant usage on procedure core.echo(varchar) to application role app_public;

    create or replace view core.shared_view as select * from my_shared_content.shared_table;

    grant select on view core.shared_view to application role app_public;
