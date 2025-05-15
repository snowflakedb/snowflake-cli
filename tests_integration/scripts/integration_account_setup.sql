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
CREATE USER IF NOT EXISTS IDENTIFIER('<% user %>');

-- BASE SETUP
CREATE ROLE IF NOT EXISTS <% role %>;
CREATE ROLE IF NOT EXISTS test_role;
GRANT CREATE ROLE ON ACCOUNT TO ROLE <% role %>;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <% role %>;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE <% role %>;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE <% role %>;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE <% role %>;
GRANT CREATE APPLICATION ON ACCOUNT TO ROLE <% role %>;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <% role %> WITH GRANT OPTION;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <% role %>;
GRANT MANAGE EVENT SHARING ON ACCOUNT TO ROLE <% role %>;
GRANT ROLE <% role %> TO USER IDENTIFIER('<% user %>');
GRANT ROLE test_role TO USER IDENTIFIER('<% user %>');

-- WAREHOUSE SETUP
CREATE WAREHOUSE IF NOT EXISTS <% warehouse %> WAREHOUSE_SIZE=XSMALL;
GRANT ALL ON WAREHOUSE <% warehouse %> TO ROLE <% role %>;

CREATE WAREHOUSE IF NOT EXISTS snowpark_tests WITH
  WAREHOUSE_SIZE = 'XSMALL'
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  RESOURCE_CONSTRAINT = 'MEMORY_1X_X86';
GRANT ALL ON WAREHOUSE snowpark_tests TO ROLE <% role %>;

-- MAIN DATABASES SETUP
CREATE DATABASE IF NOT EXISTS <% main_database %>;
GRANT ALL ON DATABASE <% main_database %> TO ROLE <% role %>;
GRANT ALL ON SCHEMA <% main_database %>.PUBLIC TO ROLE <% role %>;
USE DATABASE <% main_database %>;

-- CREATE SECOND DATABASE
CREATE DATABASE IF NOT EXISTS snowcli_db_2;

-- STAGES SETUP
CREATE STAGE IF NOT EXISTS <% main_database %>.PUBLIC.SNOWCLI_STAGE DIRECTORY = ( ENABLE = TRUE );

-- CONTAINERS SETUP
CREATE IMAGE REPOSITORY IF NOT EXISTS <% main_database %>.PUBLIC.SNOWCLI_REPOSITORY;
GRANT READ, WRITE ON IMAGE REPOSITORY <% main_database %>.PUBLIC.SNOWCLI_REPOSITORY TO ROLE <% role %>;

CREATE COMPUTE POOL IF NOT EXISTS snowcli_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS;

GRANT USAGE ON COMPUTE POOL snowcli_compute_pool TO ROLE <% role %>;
GRANT MONITOR ON COMPUTE POOL snowcli_compute_pool TO ROLE <% role %>;

ALTER COMPUTE POOL snowcli_compute_pool SUSPEND;

-- EXTERNAL ACCESS INTEGRATION
CREATE NETWORK RULE IF NOT EXISTS snowflake_docs_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('docs.snowflake.com');

CREATE SECRET IF NOT EXISTS test_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'test'; -- provide password
GRANT READ ON SECRET test_secret TO ROLE <% role %>;

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS snowflake_docs_access_integration
  ALLOWED_NETWORK_RULES = (snowflake_docs_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (test_secret)
  ENABLED = true;
GRANT USAGE ON INTEGRATION snowflake_docs_access_integration TO ROLE <% role %>;

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS cli_test_integration
  ALLOWED_NETWORK_RULES = (snowflake_docs_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (test_secret)
  ENABLED = true;
GRANT USAGE ON INTEGRATION cli_test_integration TO ROLE <% role %>;

-- API INTEGRATION FOR SNOWGIT
CREATE API INTEGRATION IF NOT EXISTS snowcli_testing_repo_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/snowflakedb/')
  ALLOWED_AUTHENTICATION_SECRETS = ()
  ENABLED = true;
GRANT USAGE ON INTEGRATION snowcli_testing_repo_api_integration TO ROLE <% role %>;

-- NOTEBOOKS SETUP
CREATE DATABASE IF NOT EXISTS NOTEBOOK;

-- CORTEX SEARCH SETUP
CREATE TABLE IF NOT EXISTS transcripts (
  transcript_text VARCHAR,
  region VARCHAR,
  agent_id VARCHAR
);

-- INSERT IF NOT EXISTS
MERGE INTO transcripts AS t USING (
  VALUES('Ah, I see you have the machine that goes "ping!". This is my favourite.', 'Meaning of Life', '01'),
        ('First shalt thou take out the Holy Pin. Then shalt thou count to three, no more, no less.', 'Holy Grail', '02'),
        ('And the beast shall be huge and black, and the eyes thereof red with the blood of living creatures', 'Life of Brian', '03'),
        ('This parrot is no more! It has ceased to be! It`s expired and gone to meet its maker!', 'Flying Circus', '04')
  ) AS s (c1, c2, c3) ON t.agent_id = s.c3
  WHEN NOT MATCHED THEN
    INSERT (transcript_text, region, agent_id) VALUES (s.c1, s.c2, s.c3);

CREATE CORTEX SEARCH SERVICE IF NOT EXISTS test_service
  ON transcript_text
  ATTRIBUTES region
  WAREHOUSE = <% warehouse %>
  TARGET_LAG = '1 day'
  AS (
    SELECT
        transcript_text,
        region,
        agent_id
    FROM transcripts
);

-- Artifact repository
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE PUBLIC;
