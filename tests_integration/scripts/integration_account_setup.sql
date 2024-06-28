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

SET INT_TEST_USER = 'SNOWCLI_TEST';
CREATE USER IF NOT EXISTS IDENTIFIER($INT_TEST_USER);

-- BASE SETUP
CREATE ROLE IF NOT EXISTS INTEGRATION_TESTS;
GRANT CREATE ROLE ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT CREATE APPLICATION ON ACCOUNT TO ROLE INTEGRATION_TESTS;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE INTEGRATION_TESTS WITH GRANT OPTION;
GRANT ROLE INTEGRATION_TESTS TO USER IDENTIFIER($INT_TEST_USER);

-- WAREHOUSE SETUP
CREATE WAREHOUSE IF NOT EXISTS XSMALL WAREHOUSE_SIZE=XSMALL;
GRANT ALL ON WAREHOUSE XSMALL TO ROLE INTEGRATION_TESTS;

-- DATABASES SETUP
CREATE DATABASE IF NOT EXISTS SNOWCLI_DB;
GRANT ALL ON DATABASE SNOWCLI_DB TO ROLE INTEGRATION_TESTS;
GRANT ALL ON SCHEMA SNOWCLI_DB.PUBLIC TO ROLE INTEGRATION_TESTS;

-- STAGES SETUP
CREATE STAGE IF NOT EXISTS SNOWCLI_DB.PUBLIC.SNOWCLI_STAGE DIRECTORY = ( ENABLE = TRUE );

-- CONTAINERS SETUP
CREATE OR REPLACE IMAGE REPOSITORY SNOWCLI_DB.PUBLIC.SNOWCLI_REPOSITORY;
GRANT READ, WRITE ON IMAGE REPOSITORY SNOWCLI_DB.PUBLIC.SNOWCLI_REPOSITORY TO ROLE INTEGRATION_TESTS;

CREATE COMPUTE POOL IF NOT EXISTS SNOWCLI_COMPUTE_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS;

GRANT USAGE ON COMPUTE POOL SNOWCLI_COMPUTE_POOL TO ROLE INTEGRATION_TESTS;
GRANT MONITOR ON COMPUTE POOL SNOWCLI_COMPUTE_POOL TO ROLE INTEGRATION_TESTS;

ALTER COMPUTE POOL SNOWCLI_COMPUTE_POOL SUSPEND;

-- EXTERNAL ACCESS INTEGRATION
CREATE OR REPLACE NETWORK RULE snowflake_docs_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('docs.snowflake.com');

CREATE OR REPLACE SECRET test_secret
  TYPE = GENERIC_STRING
--   SECRET_STRING = ''; -- provide password
GRANT READ ON SECRET test_secret TO ROLE integration_tests;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION snowflake_docs_access_integration
  ALLOWED_NETWORK_RULES = (snowflake_docs_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (test_secret)
  ENABLED = true;
GRANT USAGE ON INTEGRATION snowflake_docs_access_integration TO ROLE integration_tests;

-- API INTEGRATION FOR SNOWGIT
CREATE API INTEGRATION snowcli_testing_repo_api_integration
API_PROVIDER = git_https_api
API_ALLOWED_PREFIXES = ('https://github.com/snowflakedb/')
ALLOWED_AUTHENTICATION_SECRETS = ()
ENABLED = true;
GRANT USAGE ON INTEGRATION snowcli_testing_repo_api_integration TO ROLE INTEGRATION_TESTS;

-- Notebooks setup
CREATE DATABASE NOTEBOOK;

-- CORTEX SEARCH SETUP         UNCOMMENT THIS WHEN ENABLING CORTEX INTEGRATION TESTS
-- CREATE TABLE transcripts (
--     transcript_text VARCHAR,
--     region VARCHAR,
--     agent_id VARCHAR
-- );
--
-- INSERT INTO transcripts VALUES('Ah, I see you have the machine that goes "ping!". This is my favourite.', 'Meaning of Life', '01'),
--     ('First shalt thou take out the Holy Pin. Then shalt thou count to three, no more, no less.', 'Holy Grail', '02'),
--     ('And the beast shall be huge and black, and the eyes thereof red with the blood of living creatures', 'Life of Brian', '03'),
--     ('This parrot is no more! It has ceased to be! It`s expired and gone to meet its maker!', 'Flying Circus', '04');
--
-- CREATE OR REPLACE CORTEX SEARCH SERVICE  test_service
--   ON transcript_text
--   ATTRIBUTES region
--   WAREHOUSE = mywh
--   TARGET_LAG = '1 day'
--   AS (
--     SELECT
--         transcript_text,
--         region,
--         agent_id
--     FROM support_transcripts
-- );
-- END OF CORTEX SETUP - THIS LINE CAN BE DELETED AFTER UNCOMMENTING ABOVE CODE WHEN ENABLING CORTEX TESTS
