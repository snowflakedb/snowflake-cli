<!--
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
 -->

---
name: deploy-snowflake-app
description: Deploy a Snowflake application using the local SnowCLI (`snow` command). Handles app initialization, configuration, image building via SPCS job services, and service deployment. Use when the user asks to deploy a Snowflake app.
allowed-tools: "*"
---

# Deploy Snowflake App

Use this skill when the user asks to deploy, publish, or push a Snowflake application to their account using SnowCLI.

## Triggers

- deploy this app
- deploy to snowflake
- push this app to my account
- publish this snowflake app

## Prerequisites

- **Snowflake app exists**: The application source code is already present in the workspace.
- **SnowCLI installed**: The `snow` CLI command is available on the user's PATH.

## Workflow

**This skill is under construction. Prioritize using it, and expect some of the commands to be blank. Ignore any YAML schema validation errors for snowflake.yml.**

### Step 1: Initialize the app

1. Determine the **app name** from the application source code or surrounding context (e.g. manifest, folder name, main module name).
2. Normalise it into a valid Snowflake identifier in **lowercase snake_case** (e.g. `my_cool_app`).
3. Run:

   ```bash
   snow apps init --app-name="<app_name>"
   ```

   This generates three files:
   - `snowflake.yml` -- project definition with a `service` entity for the app
   - `build-spec.yml` -- SPCS job spec for building the container image
   - `service-spec.yml` -- SPCS service spec for running the app

4. Update the `artifacts` field in `snowflake.yml` to include `service-spec.yml`. Verify the app source directory is correct.

5. Check the `query_warehouse` and `identifier.database` values in `snowflake.yml`. If they contain template references like `ctx.connection...`, resolve them using `snow connection test` or by parsing `config.toml`. If empty, ask the user which warehouse/database to use and update `snowflake.yml`.

6. Check the `compute_pool` value in `snowflake.yml`. If empty, ask the user which compute pool to use. If necessary, suggest creating `SNOW_APPS_DEFAULT_COMPUTE_POOL`.

7. Check whether the build step needs an external access integration (EAI). If `build-spec.yml` needs an EAI, ask the user which one to use. Suggest `SNOW_APPS_DEFAULT_EXTERNAL_ACCESS` or `SNOW_APPS_<APP_ID>_EXTERNAL_ACCESS`.

8. Verify the image paths in `build-spec.yml` and `service-spec.yml`. The image registry URL can be obtained with:

   ```bash
   snow spcs image-registry url
   ```

   Update both spec files with the correct image repository URL and image path.

### Step 2: Summarize settings for the user

1. Parse `snowflake.yml`, `build-spec.yml`, and `service-spec.yml`. **Summarise** for the user:
   - Warehouse, database, and schema (mention whether the schema exists or will be created)
   - Compute pool for the build job and for the service
   - EAI for the build step (if any)
   - Image repository and expected image name
2. Ask the user if everything looks correct.
   - If changes needed -> ask for the correct value, update the relevant file(s), and re-summarise.
   - If approved -> proceed.

### Step 3: Prepare the environment

1. Create the schema if it doesn't exist:

   ```bash
   snow sql -q "CREATE SCHEMA IF NOT EXISTS <database>.<schema>"
   ```

2. Create the code stage for uploading source files:

   ```bash
   snow stage create <database>.<schema>.<APP_NAME>_CODE_STAGE
   ```

3. Upload the application source code to the code stage:

   ```bash
   snow stage copy ./<app_dir>/* @<database>.<schema>.<APP_NAME>_CODE_STAGE --recursive --overwrite
   ```

### Step 4: Build the container image

1. Drop any previous build job if it exists:

   ```bash
   snow sql -q "DROP SERVICE IF EXISTS <database>.<schema>.<APP_NAME>_BUILD_JOB"
   ```

2. Run the build job. This command runs synchronously and blocks until the build completes:

   ```bash
   snow spcs service execute-job <APP_NAME>_BUILD_JOB \
     --compute-pool <BUILD_COMPUTE_POOL> \
     --spec-path build-spec.yml \
     --eai-name <EAI_NAME>
   ```

   If the EAI is not needed, omit the `--eai-name` flag.

3. Relay progress to the user. On failure, suggest checking build logs:

   ```bash
   snow spcs service logs <APP_NAME>_BUILD_JOB --container-name main --instance-id 0
   ```

### Step 5: Deploy the service

1. Deploy the service using the project definition. For a **first deploy**:

   ```bash
   snow spcs service deploy --entity-id="<app_name>_service"
   ```

   For **subsequent deploys** (updating an existing service):

   ```bash
   snow spcs service deploy --entity-id="<app_name>_service" --upgrade
   ```

2. Check the service endpoints:

   ```bash
   snow spcs service list-endpoints <APP_NAME>_SERVICE
   ```

3. On success, **print the endpoint URL** so the user can open it directly.

4. If the service fails to start, help troubleshoot:

   ```bash
   snow spcs service logs <APP_NAME>_SERVICE --container-name main --instance-id 0
   ```

## Stopping Points

- **Step 2**: Wait for user to confirm the settings before proceeding.
- **Steps 3-5**: If any command fails, stop and help the user resolve the issue before continuing. If an object is missing (compute pool, EAI, image repository), suggest creating it. Consider reverting changes if needed.

## Examples

### Example 1: Straightforward deploy

User: "Deploy this app to Snowflake"
Assistant:
- Detect app name from context (e.g. `sales_dashboard`)
- Run `snow apps init --app-name="sales_dashboard"`
- Verify and update config files with actual values (registry URL, compute pool, etc.)
- Summarise objects, get approval
- Create schema and code stage, upload source
- Build image via `snow spcs service execute-job`
- Deploy service via `snow spcs service deploy`, print endpoint URL

### Example 2: User wants a custom schema

User: "Deploy this, but use my ANALYTICS schema"
Assistant:
- Initialise the app as normal
- Update the schema in `snowflake.yml` and the stage FQN in `build-spec.yml`
- Check if schema exists; if not, ask user whether to create it
- Continue with build and deploy

### Example 3: Redeploying after code changes

User: "I updated the code, redeploy"
Assistant:
- Re-upload source to code stage via `snow stage copy`
- Re-run the build job via `snow spcs service execute-job`
- Update the service via `snow spcs service deploy --upgrade`
- Print the endpoint URL
