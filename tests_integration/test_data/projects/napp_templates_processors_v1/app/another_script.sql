-- This file uses old templates syntax
CREATE OR REPLACE TABLE &{ ctx.env.schema_name }.&{ ctx.env.table_name } (
    name STRING
);

insert into &{ ctx.env.schema_name }.&{ ctx.env.table_name } values ('&{ ctx.env.value }');
