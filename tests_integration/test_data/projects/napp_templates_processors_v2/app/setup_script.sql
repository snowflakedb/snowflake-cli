-- This is the setup script that runs while installing a Snowflake Native App in a consumer account.
-- To write this script, you can familiarize yourself with some of the following concepts:
-- Application Roles
-- Versioned Schemas
-- UDFs/Procs
-- Extension Code
-- Refer to https://docs.snowflake.com/en/developer-guide/native-apps/creating-setup-script for a detailed understanding of this file. 

CREATE OR ALTER VERSIONED SCHEMA <% ctx.env.schema_name %>;
EXECUTE IMMEDIATE from '/another_script.sql';
select 'ctx.entities.pkg.identifier: <% ctx.entities.pkg.identifier %>';
