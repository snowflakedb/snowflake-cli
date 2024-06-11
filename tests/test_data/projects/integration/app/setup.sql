/*
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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
