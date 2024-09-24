-- package script (1/2)

create schema if not exists &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX }.my_shared_content;
grant usage on schema &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX }.my_shared_content
  to share in application package &{ ctx.entities.pkg.identifier }&{ ctx.env.SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX };
