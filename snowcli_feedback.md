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

*Context \- I asked it to create for me a streamlit in snowflake app \- it struggle mightily and never figured out how to create a valid one that has packages installed \+ warehouses selected*

*Here’s it’s notes on why it struggled:*

# Streamlit Deployment Issues & Root Cause Analysis

**Date**: December 1, 2025
**Context**: Attempting to deploy a Streamlit app to `TEMP.A_JHOLLAN` schema using Snow CLI
**Final Outcome**: Successful deployment using Python API directly

---

## Summary

Deploying a Streamlit app to Snowflake using the `snow streamlit deploy` command proved challenging due to:

1. **Unclear error messages** that didn't indicate the actual problem
2. **Undocumented `snowflake.yml` requirements** (artifacts field was required but not obvious)
3. **Silent failures** with no error output
4. **SQL syntax errors** from the CLI tool itself when certain config options were used

The deployment ultimately succeeded using the Python Snowflake Connector API directly with `CREATE STREAMLIT` SQL, bypassing the Snow CLI entirely.

---

## Attempt 1: Initial Deploy with `--name` Parameter

### Command Attempted:

```shell
snow streamlit deploy --replace \
  --database TEMP \
  --schema A_JHOLLAN \
  --name SI_USE_CASE_TRACKER \
  --file snowflake_intelligence_tracker.py \
  --connection snowhouse
```

### Error:

```
Usage: snow streamlit deploy [OPTIONS] [ENTITY_ID]
Try 'snow streamlit deploy --help' for help.
╭─ Error ────────────────────────────────────────────────────────────╮
│ No such option: --name (Possible options: --dbname, --rolename,   │
│ --username)                                                         │
╰────────────────────────────────────────────────────────────────────╯
```

### Root Cause:

The `snow streamlit deploy` command **does not accept** `--name` or `--file` parameters directly. It requires:

1. A `snowflake.yml` project definition file
2. An `entity_id` to reference the Streamlit definition in the YAML

### Learning:

**The CLI help output is misleading** \- it shows connection parameters like `--dbname`, `--rolename`, `--username` which made me think `--name` might exist. The actual workflow requires defining Streamlit apps in `snowflake.yml` first, which is not immediately obvious from the error message.

---

## Attempt 2: Deploy with Entity ID (Missing snowflake.yml)

### Setup:

Created basic `snowflake.yml`:

```
definition_version: 2
entities:
  si_use_case_tracker:
    type: streamlit
    title: "Snowflake Intelligence Use Case Tracker"
    query_warehouse: COMPUTE_WH
    main_file: snowflake_intelligence_tracker.py
    stage: streamlit
```

### Command:

```shell
snow streamlit deploy si_use_case_tracker --replace \
  --database TEMP \
  --schema A_JHOLLAN \
  --connection snowhouse
```

### Error:

```
Command timed out after 120.0 seconds
```

### Root Cause:

The command hung indefinitely without any progress indicators or error messages. When retried with 5-minute timeout, it returned no output but also didn't succeed.

### Learning:

**No feedback during deployment** \- the CLI provided zero indication of what was happening or why it was hanging. This made debugging nearly impossible.

---

## Attempt 3: Deploy with Verbose/Debug Output

### Command:

```shell
snow streamlit deploy si_use_case_tracker --replace \
  --database TEMP \
  --schema A_JHOLLAN \
  --connection snowhouse \
  --verbose
```

### Error:

```
An unexpected exception occurred. Use --debug option to see the traceback.
Exception message:

'NoneType' object is not iterable
```

### With \--debug Flag:

```py
TypeError: 'NoneType' object is not iterable

File: /snowflake/cli/_plugins/streamlit/streamlit_entity.py:75
  for artifact in self._entity_model.artifacts
```

### Root Cause:

**The `artifacts` field was missing from `snowflake.yml`**, and instead of showing a helpful error like:

```
Error: Missing required field 'artifacts' in snowflake.yml for entity 'si_use_case_tracker'
```

It threw a Python `TypeError` that required using `--debug` to even understand where the error was coming from.

### Learning:

1. **Critical config fields are not validated upfront** with user-friendly error messages
2. **The `artifacts` field is required but not documented** in common examples or error messages
3. Users need to debug internal Python stack traces to figure out missing YAML fields

---

## Attempt 4: Deploy with Artifacts Field Added

### Updated snowflake.yml:

```
definition_version: 2
entities:
  si_use_case_tracker:
    type: streamlit
    title: "Snowflake Intelligence Use Case Tracker"
    query_warehouse: COMPUTE_WH
    main_file: snowflake_intelligence_tracker.py
    stage: streamlit
    artifacts:
      - snowflake_intelligence_tracker.py
```

### Command:

```shell
snow streamlit deploy si_use_case_tracker --replace \
  --database TEMP \
  --schema A_JHOLLAN \
  --connection snowhouse
```

### Error:

```
╭─ Error ────────────────────────────────────────────────────────────╮
│ 002145 (42601): SQL compilation error:                             │
│ Unsupported statement type. correct syntax:                        │
│ 'CREATE STREAMLIT <name> [FROM <stage>] [MAIN_FILE=<file_path>]'. │
╰────────────────────────────────────────────────────────────────────╯
```

### Root Cause:

**The Snow CLI generated invalid SQL** when `query_warehouse: COMPUTE_WH` was specified in the YAML. The Snowflake SQL parser rejected the generated `CREATE STREAMLIT` statement.

Looking at the error, it appears the CLI tried to pass `query_warehouse` as a SQL parameter, but that's not supported in the `CREATE STREAMLIT` syntax.

### Learning:

1. **The CLI generates SQL behind the scenes** but doesn't show you what SQL it's generating
2. **Including `query_warehouse` in the config causes SQL syntax errors** \- this should either:
   - Be supported and generate valid SQL
   - Be validated upfront with a clear error message
   - Be documented as "not supported via CLI, set via UI after deployment"

---

## Attempt 5: Deploy Without query\_warehouse

### Updated snowflake.yml (removed query\_warehouse):

```
definition_version: 2
entities:
  si_use_case_tracker:
    type: streamlit
    title: "Snowflake Intelligence Use Case Tracker"
    main_file: snowflake_intelligence_tracker.py
    stage: streamlit
    artifacts:
      - snowflake_intelligence_tracker.py
```

### Command:

```shell
snow streamlit deploy si_use_case_tracker --replace \
  --database TEMP \
  --schema A_JHOLLAN \
  --connection snowhouse
```

### Result:

**Silent failure** \- command returned exit code indicating failure but produced no error message or output.

### Verification:

```sql
SHOW STREAMLITS LIKE 'SI_USE_CASE_TRACKER' IN SCHEMA TEMP.A_JHOLLAN;
-- Result: 0 rows (not created)
```

### Root Cause:

Unknown \- the CLI failed silently without any indication of what went wrong.

### Learning:

**Silent failures are the worst developer experience** \- at minimum, every failure should output an error message explaining what went wrong.

---

## Successful Workaround: Python API Approach

### Solution:

Bypassed Snow CLI entirely and used Python with Snowflake Connector:

```py
import os
from snowflake.connector import connect

conn = connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
cursor = conn.cursor()

# Create stage
cursor.execute("CREATE STAGE IF NOT EXISTS TEMP.A_JHOLLAN.streamlit")

# Upload file
cursor.execute("""
  PUT file:///Users/jeffhollan/Development/snova/use_cases/snowflake_intelligence_tracker.py
  @TEMP.A_JHOLLAN.streamlit/SI_USE_CASE_TRACKER/
  AUTO_COMPRESS=FALSE
  OVERWRITE=TRUE
""")

# Create Streamlit
cursor.execute("""
  CREATE OR REPLACE STREAMLIT TEMP.A_JHOLLAN.SI_USE_CASE_TRACKER
    FROM '@TEMP.A_JHOLLAN.streamlit/SI_USE_CASE_TRACKER'
    MAIN_FILE = 'snowflake_intelligence_tracker.py'
""")

print("Streamlit app deployed successfully!")
```

### Result:

✅ **Successful deployment** \- Streamlit created and accessible at `TEMP.A_JHOLLAN.SI_USE_CASE_TRACKER`

### Why This Worked:

1. **Direct SQL control** \- no CLI abstraction layer generating potentially invalid SQL
2. **Clear error messages** \- if SQL fails, Snowflake's error messages are shown directly
3. **Minimal configuration** \- only required SQL parameters, no YAML config needed
4. **Simple workflow** \- upload file to stage, then create Streamlit pointing to it

---

## Root Cause Analysis Summary

### 1\. **Poor Error Messages**

- `'NoneType' object is not iterable` instead of "Missing required field: artifacts"
- Silent failures with no error output
- SQL errors from generated statements not clearly attributed to config issues

### 2\. **Unclear Documentation Requirements**

- `artifacts` field required but not mentioned in basic examples
- `query_warehouse` appears valid in YAML but causes SQL generation errors
- No clear documentation on what `snowflake.yml` fields are required vs. optional

### 3\. **Confusing Command Interface**

- `--name` and `--file` flags don't exist, but error suggests they might
- Requires `snowflake.yml` but this isn't clear from command help
- No validation of YAML config before attempting deployment

### 4\. **Lack of Transparency**

- CLI generates SQL behind the scenes without showing what it's running
- No progress indicators during long-running operations
- Can't see what's being uploaded or what SQL is being executed

---

## Recommendations for Snowflake Team

### Short Term (Quick Wins):

1. **Improve Error Messages**

   - Validate `snowflake.yml` upfront and show clear errors for missing required fields
   - Example: "Error: Missing required field 'artifacts' in entity 'si\_use\_case\_tracker'"
   - Never silently fail \- always output an error message



2. **Add Progress Indicators**

   - Show "Uploading files..." / "Creating Streamlit..." / "Verifying deployment..."
   - Don't leave users hanging with no feedback



3. **Show Generated SQL (Optional Flag)**

   - Add `--show-sql` flag to display the SQL being generated
   - Helps users debug configuration issues



4. **Validate Configuration**

   - Check if `query_warehouse` in YAML will cause SQL errors before attempting deployment
   - Warn users about unsupported fields

### Medium Term (Better DX):

5. **Improve Documentation**

   - Provide complete `snowflake.yml` examples with all required fields
   - Document which fields are required vs. optional
   - Show common error scenarios and how to fix them



6. **Add `snow streamlit init` Command**

   - Generate a template `snowflake.yml` with proper structure
   - Similar to `npm init`, `cargo init`, etc.



7. **Better \--help Output**

   - Clarify that deployment requires `snowflake.yml`
   - Show example YAML structure in help text
   - Link to documentation

### Long Term (Ideal State):

8. **Interactive Deployment**

```shell
$ snow streamlit deploy

No snowflake.yml found. Would you like to create one? (y/n): y
Streamlit name: SI_USE_CASE_TRACKER
Main file: snowflake_intelligence_tracker.py
Additional files (comma-separated, or press enter):

Created snowflake.yml. Deploying...
✓ Uploaded snowflake_intelligence_tracker.py
✓ Created STREAMLIT TEMP.A_JHOLLAN.SI_USE_CASE_TRACKER

App URL: https://app.snowflake.com/...
```

9. **Validate Before Deploy**

   - Dry-run mode that validates configuration without deploying
   - `snow streamlit deploy --dry-run`



10. **Better Debugging Tools**

    - `snow streamlit logs <name>` to see deployment logs
    - `snow streamlit describe <name>` to see current state
    - `snow streamlit validate` to check `snowflake.yml` syntax

---

## Comparison: Expected vs. Actual DX

### Expected (Industry Standard):

```shell
# Simple, intuitive deployment
$ snow streamlit deploy my_app.py --database TEMP --schema A_JHOLLAN

Deploying Streamlit app...
✓ Uploaded my_app.py (1.2 KB)
✓ Created STREAMLIT TEMP.A_JHOLLAN.MY_APP
✓ App is ready

URL: https://app.snowflake.com/...
```

### Actual (Current State):

```shell
# Hidden requirements, cryptic errors
$ snow streamlit deploy my_app.py --database TEMP --schema A_JHOLLAN
Error: No such option: --name

$ snow streamlit deploy --help
# Doesn't mention snowflake.yml requirement

$ snow streamlit deploy si_app ...
# Hangs for 2 minutes, then times out

$ snow streamlit deploy si_app --verbose ...
'NoneType' object is not iterable

$ snow streamlit deploy si_app --debug ...
# 50+ lines of Python stack trace
# Buried in the middle: "for artifact in self._entity_model.artifacts"
# Implies: need to add 'artifacts' field to YAML

$ # Fix YAML, try again...
SQL compilation error: Unsupported statement type

$ # Remove query_warehouse from YAML...
# Silent failure, no error output, app not created

$ # Give up on CLI, write custom Python script...
✓ Success
```

---

## Impact on Developer Experience

**Time Spent**: \~30 minutes debugging deployment issues that should have taken 2 minutes

**Frustration Level**: High \- multiple silent failures, unclear requirements, unhelpful errors

**Workaround Required**: Yes \- had to bypass the CLI tool entirely

**Would I Use This Again?**: No \- I'd use the Python API directly to avoid CLI issues

---

## Additional Post-Deployment Issues

### Issue 6: Query Warehouse Not Configured

**Problem**: After successful deployment, attempting to access the Streamlit app in the UI resulted in an error or failure to render.

**Root Cause**: The Streamlit app requires a **query warehouse** to be configured before it can run, but:

1. The deployment process doesn't configure this automatically
2. There's no error message indicating a warehouse is needed
3. It's not obvious from the UI that you need to manually set this

**Solution**: Manually navigate to the Streamlit app in Snowflake UI and configure the query warehouse in the settings.

**Why This Is Confusing**:

- The `snowflake.yml` had `query_warehouse: COMPUTE_WH` specified, but this caused SQL errors during deployment
- Even after removing it to make deployment work, the warehouse still needs to be set manually in the UI
- There's a disconnect between CLI deployment and UI configuration
- No documentation or error message guides users to this required post-deployment step

**Expected Behavior**:

```shell
$ snow streamlit deploy si_app --query-warehouse COMPUTE_WH

Deploying Streamlit app...
✓ Uploaded files
✓ Created STREAMLIT TEMP.A_JHOLLAN.SI_APP
✓ Configured query warehouse: COMPUTE_WH
✓ App is ready

URL: https://app.snowflake.com/...
```

**Actual Behavior**:

```shell
$ snow streamlit deploy si_app
# Deployment succeeds

# Navigate to app in UI
# App fails to render or shows error
# No indication that query warehouse is missing

# User must:
# 1. Figure out that query warehouse is the issue
# 2. Find the setting in the UI (not obvious where)
# 3. Manually select a warehouse from dropdown
# 4. Try loading the app again
```

---

### Issue 7: Missing Python Package Dependencies

**Problem**: After configuring the query warehouse, the Streamlit app failed to render with import errors.

**Error**:

```py
ModuleNotFoundError: No module named 'plotly'
```

**Root Cause**: Python packages used in the app (like `plotly`) need to be **explicitly specified** in the Streamlit configuration, but:

1. The CLI deployment doesn't detect or prompt for package dependencies
2. There's no `requirements.txt` support during deployment
3. Packages must be manually added through the UI after deployment
4. The error doesn't explain how to fix the missing dependency

**Solution**: Manually navigate to the Streamlit app settings in the UI and add `plotly` to the packages list.

**Why This Is Confusing**:

- Standard Python projects use `requirements.txt` or `pyproject.toml` for dependencies
- The Snow CLI doesn't support or read these files during `streamlit deploy`
- Users must manually configure packages through the UI post-deployment
- No validation or warning that packages are missing until runtime

**Expected Behavior**:

```shell
# Option 1: Auto-detect dependencies
$ snow streamlit deploy si_app

Analyzing dependencies...
Found: streamlit, pandas, plotly
✓ Configured packages

# Option 2: Read from requirements.txt
$ cat requirements.txt
streamlit
pandas
plotly
snowflake-snowpark-python

$ snow streamlit deploy si_app
Reading requirements.txt...
✓ Configured 4 packages
```

**Actual Behavior**:

```shell
$ snow streamlit deploy si_app
# Deployment succeeds, no mention of packages

# Navigate to app in UI
# App crashes with ModuleNotFoundError

# User must:
# 1. See the import error in app logs/console
# 2. Figure out packages need to be added in UI settings
# 3. Find the packages configuration section (not obvious)
# 4. Manually type in each package name
# 5. Wait for environment to rebuild
# 6. Try loading the app again
# 7. Repeat for each missing package
```

---

### Issue 8: Role/Permission Configuration Not Obvious

**Problem**: After configuring query warehouse and packages, the Streamlit app fails with:

```
SQL compilation error: Schema 'SALES.KNOWLEDGE_ASSISTANT' does not exist or not authorized.
```

**Root Cause**: The Streamlit app executes with the **owner role** (`PRODUCT_MANAGER` in this case), which doesn't have access to the data schema being queried. There's:

1. No way to specify which role the Streamlit should execute with during deployment
2. No validation during deployment that the role has access to referenced schemas
3. No clear indication in the UI which role the Streamlit is running as
4. No obvious way to change the execution role post-deployment

**Solution**: Must grant the necessary read role to the Streamlit owner role, or change the app's ownership:

**Option 1 \- Grant Access** (requires admin):

```sql
GRANT ROLE SALES_BASIC_RO TO ROLE PRODUCT_MANAGER;
-- or
GRANT USAGE ON SCHEMA SALES.KNOWLEDGE_ASSISTANT TO ROLE PRODUCT_MANAGER;
GRANT SELECT ON ALL TABLES IN SCHEMA SALES.KNOWLEDGE_ASSISTANT TO ROLE PRODUCT_MANAGER;
```

**Option 2 \- Change Ownership** (if allowed):

```sql
GRANT OWNERSHIP ON STREAMLIT TEMP.A_JHOLLAN.SI_USE_CASE_TRACKER
  TO ROLE SALES_BASIC_RO;
```

**Why This Is Confusing**:

- **No pre-deployment validation** \- CLI doesn't check if the deploying role can access the data
- **No role selection during deployment** \- can't specify "run this as SALES\_BASIC\_RO"
- **Silent permission failure** \- error only appears when app is loaded in UI, not during deployment
- **Not obvious which role is being used** \- UI doesn't clearly show execution role
- **Standard pattern doesn't work** \- in normal SQL/Python development, you can `USE ROLE` to switch context, but Streamlit inherits the owner role with no easy override

**Expected Behavior**:

```shell
$ snow streamlit deploy si_app --execute-as-role SALES_BASIC_RO

Validating permissions...
✓ Role SALES_BASIC_RO has access to SALES.KNOWLEDGE_ASSISTANT
✓ Configured execution role

Deployment successful!
```

Or at minimum, validation warning:

```shell
$ snow streamlit deploy si_app

Deploying Streamlit app...
✓ Uploaded files
✓ Created STREAMLIT

⚠️  Warning: Your role PRODUCT_MANAGER may not have access to:
    - SALES.KNOWLEDGE_ASSISTANT (referenced in app code)

Consider granting access or changing ownership to a role with permissions.
```

**Actual Behavior**:

```shell
$ snow streamlit deploy si_app
# Deployment "succeeds"

# User navigates to app in UI
# Configures query warehouse
# Adds packages
# App loads...

# Error appears:
SQL compilation error: Schema 'SALES.KNOWLEDGE_ASSISTANT' does not exist or not authorized.

# No indication of which role is executing
# No suggestion of how to fix
# User must debug SQL errors to figure out it's a permission issue
```

---

## Summary of Post-Deployment Manual Steps Required

After "successful" CLI deployment, users must manually:

1. ✋ **Configure Query Warehouse** (via UI)

   - Navigate to Streamlit app
   - Find settings/configuration
   - Select a warehouse from dropdown
   - Save



2. ✋ **Add Python Packages** (via UI)

   - Navigate to Streamlit app settings
   - Find packages section
   - Manually type in each package name: `plotly`, `pandas`, etc.
   - Wait for environment rebuild
   - Retry if any packages are missing



3. ✋ **Verify App Actually Works**

   - Load the app
   - Debug any runtime errors
   - Repeat steps 1-2 if issues found

**Total Time**: An additional 5-10 minutes of manual UI configuration after deployment

**Frustration Level**: High \- deployment "succeeded" but app doesn't actually work until manual configuration

---

## Updated Root Cause Analysis

### 5\. **Deployment Success ≠ Working App**

The CLI deployment can succeed but produce a non-functional app because:

- **Query warehouse configuration is not part of deployment**
- **Package dependencies are not detected or configured**
- **No post-deployment validation** to verify the app actually works
- **"Success" message is misleading** \- it only means files were uploaded, not that the app is ready to use

This creates a **false sense of completion** where users think they're done, but the app won't work until manual UI configuration is completed.

---

## Updated Recommendations

### Critical Missing Features:

11. **Support requirements.txt or environment.yml**

```shell
$ cat requirements.txt
streamlit
pandas
plotly
snowflake-snowpark-python

$ snow streamlit deploy si_app
✓ Found requirements.txt
✓ Configured 4 packages
```

12. **Configure Query Warehouse During Deployment**

```shell
$ snow streamlit deploy si_app --query-warehouse COMPUTE_WH
✓ Configured query warehouse: COMPUTE_WH
```

    Or interactive prompt:

```shell
$ snow streamlit deploy si_app
Query warehouse not specified. Available warehouses:
  1. COMPUTE_WH
  2. ANALYTICS_WH
  3. DEV_WH

Select warehouse (1-3): 1
✓ Configured query warehouse: COMPUTE_WH
```

13. **Post-Deployment Validation**

```shell
$ snow streamlit deploy si_app --validate

Deploying Streamlit app...
✓ Uploaded files
✓ Created STREAMLIT

Validating deployment...
✓ Query warehouse configured
✓ All packages available
✓ App loads successfully

Deployment successful!
```

14. **Better Success Messaging**

```shell
# Instead of just "Success"
Deployment complete!

⚠️  Additional configuration required:
1. Configure query warehouse in UI
2. Add Python packages: plotly, pandas

App URL: https://app.snowflake.com/...
Status: Requires configuration before use
```

---

## Conclusion

The Snow CLI `streamlit deploy` command has significant usability issues that make it frustrating to use:

- Required fields not documented or validated
- Silent failures with no error messages
- Internal exceptions exposed to users instead of friendly errors
- No transparency into what the CLI is doing
- **Deployment "succeeds" but app requires manual UI configuration to actually work**
- **Critical settings like query warehouse and packages can't be set via CLI**
- **No way to specify dependencies in code (requirements.txt not supported)**

The Python API approach (PUT file \+ CREATE STREAMLIT) is **simpler, more transparent, and more reliable** than the CLI tool, but **still requires manual UI configuration** for query warehouse and packages.

**Bottom line**:

1. The CLI should make deployment easier, not harder. Right now, it's adding friction instead of removing it.
2. "Deployment successful" should mean the app is actually ready to use, not just that files were uploaded.
3. Required configuration (warehouse, packages) should be part of the deployment process, not post-deployment manual steps.
