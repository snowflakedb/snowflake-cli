-- package script (2/2)

create or replace table &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX }.my_shared_content.shared_table (
  col1 number,
  col2 varchar
);

insert into &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX }.my_shared_content.shared_table (col1, col2)
  values (1, 'hello');

grant select on table &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX }.my_shared_content.shared_table
  to share in application package &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX };
