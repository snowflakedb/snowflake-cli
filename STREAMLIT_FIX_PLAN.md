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

# Comprehensive Plan to Fix ALL `snow streamlit` Issues

**Date**: December 24, 2025
**Branch**: fix/streamlit-deployment-issues
**Based on**: Detailed feedback document from team director
**Estimated Effort**: 3-5 days

---

## 📋 Executive Summary

The `snow streamlit deploy` command has **8 distinct failure scenarios** stemming from **5 root causes**. This plan addresses EVERY issue mentioned in the feedback document:

1. **Cryptic error messages** → Clear, actionable errors with context
2. **Silent failures** → Progress indicators and explicit error output
3. **No package support** → Auto-detect requirements.txt/environment.yml
4. **Query warehouse issues** → Validation, warnings, and configuration help
5. **Misleading success messages** → Status checklist showing what's actually configured

**Expected Outcome**: Reduce deployment time from 30+ minutes to < 2 minutes, eliminate frustration.

---

## 📋 ALL Issues from Feedback Document

### **Attempt 1: Misleading Error Messages**
- **Issue**: `--name` and `--file` flags don't exist, but error suggests similar flags
- **Root Cause**: Error message shows `--dbname`, `--rolename` which misleads users
- **Fix**: Improve error message to explain snowflake.yml requirement

### **Attempt 2: Silent Timeout/Hanging**
- **Issue**: Command hung for 120s with zero feedback
- **Root Cause**: No progress indicators, unclear what's happening
- **Fix**: Add progress messages at each step

### **Attempt 3: Cryptic NoneType Error**
- **Issue**: `'NoneType' object is not iterable` at line 76 (artifacts iteration)
- **Root Cause**: `artifacts` field was None but code tried to iterate without validation
- **Fix**: Validate YAML upfront, provide user-friendly error

### **Attempt 4: SQL Syntax Error from query_warehouse**
- **Issue**: `SQL compilation error: Unsupported statement type` when query_warehouse in YAML
- **Root Cause**: Invalid SQL generated (note: current code looks correct, need to verify)
- **Fix**: Verify SQL generation is working correctly now, add --show-sql flag

### **Attempt 5: Silent Failure**
- **Issue**: Command returned failure code but NO error message
- **Root Cause**: Exception caught somewhere but not logged/displayed
- **Fix**: Ensure ALL exceptions are caught and displayed

### **Issue 6: Query Warehouse Not Configured Post-Deployment**
- **Issue**: App deployed but won't run - warehouse must be set manually in UI
- **Root Cause**: query_warehouse in YAML caused errors, removing it means app incomplete
- **Fix**: Make query_warehouse work properly, validate it's set, warn if missing

### **Issue 7: Missing Python Packages**
- **Issue**: App crashes with `ModuleNotFoundError` - must manually add packages in UI
- **Root Cause**: No requirements.txt support, no package detection
- **Fix**: Add support for reading requirements.txt/environment.yml and deploying packages

### **Issue 8: Role/Permission Issues**
- **Issue**: App fails with `Schema does not exist or not authorized`
- **Root Cause**: Streamlit runs with owner role, no validation of permissions
- **Fix**: Add permission validation warnings (optional - advanced feature)

---

## 🎯 Implementation Plan (Organized by Priority)

### **PHASE 1: Critical Error Handling & Validation**
**Fixes**: Attempts 1-5
**Priority**: P0 (Must Have)
**Effort**: 1-2 days

#### 1.1 **Fix NoneType Iteration Error** (Attempt 3)
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py` line 76

**Current Code**:
```python
def bundle(self, output_dir: Optional[Path] = None) -> BundleMap:
    artifacts = list(self._entity_model.artifacts or [])  # Line 76
```

**Problem**: This line ALREADY handles None with `or []`, but error says line 75 has issue

**Action**:
- Check if there's a different code path in older version or if line numbers shifted
- Add explicit validation before bundle() is called
- Add try-catch with helpful error message
- Wrap the artifacts iteration in validation

**Implementation**:
```python
def bundle(self, output_dir: Optional[Path] = None) -> BundleMap:
    try:
        artifacts = list(self._entity_model.artifacts or [])
    except (TypeError, AttributeError) as e:
        raise CliError(
            "Failed to process artifacts configuration.\n"
            f"Please check your snowflake.yml has a valid 'artifacts' field.\n"
            f"Example:\n"
            f"  artifacts:\n"
            f"    - streamlit_app.py\n"
            f"    - requirements.txt"
        )
```

#### 1.2 **Add YAML Validation Before Deployment**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Add New Method**:
```python
def _validate_configuration(self):
    """Validate snowflake.yml configuration before deployment."""
    errors = []
    warnings = []

    # Check main_file is specified
    if not self._entity_model.main_file:
        errors.append("Missing required field: 'main_file'")
    else:
        # Check main_file exists
        main_file_path = self.root / self._entity_model.main_file
        if not main_file_path.exists():
            errors.append(f"Main file not found: {self._entity_model.main_file}")

    # Check artifacts (if provided) exist
    if self._entity_model.artifacts:
        for artifact in self._entity_model.artifacts:
            src_path = self.root / artifact.src
            if not src_path.exists():
                errors.append(f"Artifact not found: {artifact.src}")

    # Warn if query_warehouse not set
    if not self._entity_model.query_warehouse:
        warnings.append(
            "No query_warehouse specified. The Streamlit app will not run until you configure a warehouse in Snowsight."
        )

    # Display warnings
    for warning in warnings:
        self._workspace_ctx.console.warning(f"⚠️  {warning}")

    # Raise errors if any
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  • {e}" for e in errors)
        raise CliError(error_msg)
```

**Call Location**: In `deploy()` method, add as first step:
```python
def deploy(self, action_context: ActionContext, ...):
    # VALIDATE FIRST
    self._validate_configuration()

    if bundle_map is None:
        bundle_map = self.bundle()
    # ... rest of deploy logic
```

#### 1.3 **Add Progress Indicators Throughout Deploy**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Update `deploy()` method**:
```python
def deploy(self, action_context: ActionContext, ...):
    console = self._workspace_ctx.console

    console.step("Validating configuration...")
    self._validate_configuration()

    console.step("Bundling artifacts...")
    if bundle_map is None:
        bundle_map = self.bundle()

    console.step(f"Checking if object exists")
    object_exists = self._object_exists()
    # ... existing logic ...
```

**Update `_deploy_legacy()` method**:
```python
def _deploy_legacy(self, bundle_map: BundleMap, replace: bool = False, prune: bool = False):
    console = self._workspace_ctx.console

    console.step(f"Uploading artifacts to stage {self.model.stage}")
    # ... existing upload logic ...

    console.step(f"Creating Streamlit object {self.model.fqn.sql_identifier}")
    self._execute_query(...)

    console.step("Configuring permissions...")
    StreamlitManager(connection=self._conn).grant_privileges(self.model)

    console.message("✓ Deployment complete!")
```

**Update `_deploy_versioned()` method** similarly.

#### 1.4 **Ensure No Silent Failures**
**Files**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`, `src/snowflake/cli/_plugins/streamlit/commands.py`

**Action in streamlit_entity.py**:
```python
# Wrap critical operations in try-catch
def deploy(self, action_context: ActionContext, ...):
    try:
        console.step("Validating configuration...")
        self._validate_configuration()

        console.step("Bundling artifacts...")
        if bundle_map is None:
            bundle_map = self.bundle()

        # ... rest of deploy logic ...

    except ProgrammingError as e:
        # SQL errors from Snowflake
        error_code = getattr(e, 'errno', 'Unknown')
        raise CliError(
            f"Snowflake SQL error ({error_code}): {str(e)}\n"
            f"This may indicate:\n"
            f"  • Invalid query_warehouse name\n"
            f"  • Missing permissions\n"
            f"  • Invalid stage path\n"
            f"Use --show-sql flag to see the generated SQL."
        )
    except ClickException:
        # Re-raise ClickExceptions as-is
        raise
    except Exception as e:
        # Catch-all for unexpected errors
        log.exception("Unexpected error during deployment")
        raise CliError(
            f"Deployment failed: {str(e)}\n"
            f"Use --debug flag for full traceback."
        )
```

**Action in commands.py**:
```python
@app.command("deploy", requires_connection=True)
def streamlit_deploy(...):
    try:
        url = streamlit.perform(EntityActions.DEPLOY, ...)
        return MessageResult(...)
    except NoProjectDefinitionError as e:
        # Friendly message for missing snowflake.yml
        raise ClickException(
            "No snowflake.yml found in current directory.\n\n"
            "Streamlit deployment requires a project definition file.\n"
            "Create one with:\n"
            "  snow streamlit init <app_name>\n\n"
            "Or see documentation: https://docs.snowflake.com/streamlit"
        )
    except Exception as e:
        # Ensure exception message is displayed
        log.exception("Streamlit deployment failed")
        raise
```

#### 1.5 **Add --show-sql Flag**
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Add Flag**:
```python
@app.command("deploy", requires_connection=True)
def streamlit_deploy(
    replace: bool = ReplaceOption(...),
    prune: bool = PruneOption(),
    entity_id: str = entity_argument("streamlit"),
    open_: bool = OpenOption,
    legacy: bool = LegacyOption,
    show_sql: bool = typer.Option(
        False,
        "--show-sql",
        help="Display the SQL commands that will be executed",
    ),
    **options,
) -> CommandResult:
```

**Pass to Entity**:
```python
url = streamlit.perform(
    EntityActions.DEPLOY,
    ActionContext(get_entity=lambda *args: None),
    _open=open_,
    replace=replace,
    legacy=legacy,
    prune=prune,
    show_sql=show_sql,  # NEW
)
```

**In streamlit_entity.py**:
```python
def deploy(self, action_context: ActionContext, ..., show_sql: bool = False, **kwargs):
    # ... existing logic ...

    if legacy:
        self._deploy_legacy(bundle_map=bundle_map, replace=replace, prune=prune, show_sql=show_sql)
    else:
        self._deploy_versioned(bundle_map=bundle_map, replace=replace, prune=prune, show_sql=show_sql)

def _deploy_legacy(self, ..., show_sql: bool = False):
    # ... existing logic ...

    sql = self.get_deploy_sql(replace=replace, from_stage_name=stage_root, legacy=True)

    if show_sql:
        self._workspace_ctx.console.message("\nGenerated SQL:")
        self._workspace_ctx.console.message(f"```sql\n{sql}\n```")

    self._execute_query(sql)
```

---

### **PHASE 2: Package & Dependency Support**
**Fixes**: Issue 7
**Priority**: P0 (Must Have)
**Effort**: 1-2 days

#### 2.1 **Add Package Detection**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Add New Method**:
```python
def _detect_packages(self) -> Optional[List[str]]:
    """Detect packages from requirements.txt or environment.yml."""
    packages = []

    # Check for requirements.txt
    req_file = self.root / "requirements.txt"
    if req_file.exists():
        self._workspace_ctx.console.step("Found requirements.txt, reading packages...")
        with open(req_file) as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    # Parse package name (handle versions like pandas==1.0.0, pandas>=1.0.0)
                    pkg = line.split('==')[0].split('>=')[0].split('<=')[0].split('~=')[0].strip()
                    if pkg:
                        packages.append(pkg)
        self._workspace_ctx.console.message(f"  Found {len(packages)} packages")

    # Check for environment.yml
    env_file = self.root / "environment.yml"
    if env_file.exists():
        self._workspace_ctx.console.step("Found environment.yml")
        try:
            import yaml
            with open(env_file) as f:
                env = yaml.safe_load(f)
                if env and 'dependencies' in env:
                    for dep in env['dependencies']:
                        if isinstance(dep, str):
                            pkg = dep.split('==')[0].split('>=')[0].split('<=')[0].split('~=')[0].strip()
                            if pkg:
                                packages.append(pkg)
            self._workspace_ctx.console.message(f"  Found {len(packages)} packages in environment.yml")
        except Exception as e:
            self._workspace_ctx.console.warning(f"  Failed to parse environment.yml: {e}")

    # Remove duplicates and common non-package entries
    packages = list(set(packages))
    # Filter out common non-Snowflake-compatible packages
    packages = [p for p in packages if p not in ['python', 'pip']]

    return packages if packages else None
```

#### 2.2 **Update Bundle to Include environment.yml**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Update `bundle()` method**:
```python
def bundle(self, output_dir: Optional[Path] = None) -> BundleMap:
    artifacts = list(self._entity_model.artifacts or [])

    # Ensure main_file is included
    main_file = self._entity_model.main_file
    if main_file and not any(artifact.src == main_file for artifact in artifacts):
        artifacts.insert(0, PathMapping(src=main_file))

    # NEW: Detect and generate environment.yml if packages found
    detected_packages = self._detect_packages()
    if detected_packages:
        output_path = output_dir or bundle_root(self.root, "streamlit") / self.entity_id
        output_path.mkdir(parents=True, exist_ok=True)

        # Check if environment.yml already exists in artifacts
        has_env_yml = any(artifact.src == "environment.yml" for artifact in artifacts)

        if not has_env_yml:
            # Generate environment.yml with detected packages
            env_yml_path = output_path / "environment.yml"
            env_content = {
                'name': 'streamlit',
                'channels': ['snowflake'],
                'dependencies': detected_packages
            }
            import yaml
            with open(env_yml_path, 'w') as f:
                yaml.dump(env_content, f, default_flow_style=False)

            self._workspace_ctx.console.message(f"✓ Generated environment.yml with {len(detected_packages)} packages")

            # Note: environment.yml will be uploaded as part of stage sync

    return build_bundle(
        self.root,
        output_dir or bundle_root(self.root, "streamlit") / self.entity_id,
        [PathMapping(src=artifact.src, dest=artifact.dest, processors=artifact.processors)
         for artifact in artifacts],
    )
```

#### 2.3 **Add Warning if Packages Detected but Not in Artifacts**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**In `_validate_configuration()` method**:
```python
def _validate_configuration(self):
    # ... existing validation ...

    # Check for packages
    detected_packages = self._detect_packages()
    if detected_packages:
        warnings.append(
            f"Detected {len(detected_packages)} packages in requirements.txt/environment.yml. "
            f"These will be included in the deployment."
        )
```

---

### **PHASE 3: Query Warehouse Configuration**
**Fixes**: Issue 6
**Priority**: P0 (Must Have)
**Effort**: 0.5 days

#### 3.1 **Verify query_warehouse SQL Generation**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Current code** (lines 195-201):
```python
if self.model.query_warehouse:
    query += f"\nQUERY_WAREHOUSE = {self.model.query_warehouse}"
else:
    self._workspace_ctx.console.warning(
        "[Deprecation] In next major version we will remove default query_warehouse='streamlit'."
    )
    query += f"\nQUERY_WAREHOUSE = 'streamlit'"
```

**Action**:
- Code looks correct - QUERY_WAREHOUSE should work in CREATE STREAMLIT
- Test to ensure this works correctly
- The feedback document says it caused errors, but may have been a different issue or old version

#### 3.2 **Add Warehouse Validation**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Add New Method**:
```python
def _validate_warehouse(self, warehouse_name: str) -> bool:
    """Check if warehouse exists and is accessible."""
    try:
        result = self._execute_query(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
        if result.rowcount > 0:
            return True
        else:
            return False
    except Exception as e:
        log.debug(f"Failed to validate warehouse: {e}")
        return False
```

**Call in `_validate_configuration()`**:
```python
def _validate_configuration(self):
    # ... existing validation ...

    # Validate warehouse if specified
    if self._entity_model.query_warehouse:
        warehouse_name = self._entity_model.query_warehouse
        if not self._validate_warehouse(warehouse_name):
            warnings.append(
                f"Warehouse '{warehouse_name}' may not exist or is not accessible. "
                f"Deployment may fail if warehouse is invalid."
            )
```

#### 3.3 **Improve Warning Message for Missing Warehouse**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Update in `_validate_configuration()`**:
```python
# Warn if query_warehouse not set
if not self._entity_model.query_warehouse:
    warnings.append(
        "⚠️  IMPORTANT: No query_warehouse specified!\n"
        "   Your Streamlit app will NOT run until you:\n"
        "   1. Add 'query_warehouse: YOUR_WAREHOUSE' to snowflake.yml, OR\n"
        "   2. Configure a warehouse manually in Snowsight after deployment"
    )
```

---

### **PHASE 4: Post-Deployment Validation & Better Messaging**
**Fixes**: Success Messaging (Issue 6, 7)
**Priority**: P1 (Should Have)
**Effort**: 0.5 days

#### 4.1 **Add Post-Deployment Validation**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Add New Method**:
```python
def _post_deploy_validation(self) -> Dict[str, Any]:
    """Validate app is properly configured after deployment."""
    checks = {
        'streamlit_exists': False,
        'warehouse_configured': False,
        'warehouse_name': None,
    }

    try:
        result = self.describe().fetchone()
        if result:
            checks['streamlit_exists'] = True

            # Check query warehouse is set
            warehouse = result.get('query_warehouse')
            if warehouse and warehouse.strip():
                checks['warehouse_configured'] = True
                checks['warehouse_name'] = warehouse

    except Exception as e:
        log.debug(f"Post-deployment validation failed: {e}")

    return checks
```

#### 4.2 **Add --validate Flag**
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Add Flag**:
```python
@app.command("deploy", requires_connection=True)
def streamlit_deploy(
    # ... existing params ...
    validate: bool = typer.Option(
        False,
        "--validate",
        help="Validate deployment after completion",
    ),
    **options,
) -> CommandResult:
```

**Pass to Entity**:
```python
url = streamlit.perform(
    EntityActions.DEPLOY,
    ActionContext(get_entity=lambda *args: None),
    _open=open_,
    replace=replace,
    legacy=legacy,
    prune=prune,
    show_sql=show_sql,
    validate=validate,  # NEW
)
```

**Return Validation Results in streamlit_entity.py**:
```python
def deploy(self, action_context: ActionContext, ..., validate: bool = False, **kwargs):
    # ... existing deploy logic ...

    url = self.perform(EntityActions.GET_URL, action_context, *args, **kwargs)

    # NEW: Post-deployment validation
    validation_results = None
    if validate:
        console.step("Validating deployment...")
        validation_results = self._post_deploy_validation()

    return {
        'url': url,
        'validation': validation_results
    }
```

#### 4.3 **Improve Success Messaging**
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Update Return Statement**:
```python
@app.command("deploy", requires_connection=True)
def streamlit_deploy(...) -> CommandResult:
    # ... existing logic ...

    result = streamlit.perform(EntityActions.DEPLOY, ...)

    # Build status message
    status_lines = []
    status_lines.append("=" * 60)
    status_lines.append("✓ Streamlit Deployment Complete!")
    status_lines.append("=" * 60)

    # Always show URL
    if isinstance(result, dict):
        url = result.get('url')
        validation = result.get('validation')
    else:
        url = result
        validation = None

    status_lines.append(f"\nApp URL: {url}")

    # Show validation results if available
    if validation:
        status_lines.append("\nConfiguration Status:")
        status_lines.append(f"  {'✓' if validation['streamlit_exists'] else '✗'} Streamlit object created")

        if validation['warehouse_configured']:
            status_lines.append(f"  ✓ Query warehouse configured: {validation['warehouse_name']}")
        else:
            status_lines.append("  ⚠️  Query warehouse NOT configured")

        # Show warnings/actions needed
        if not validation['warehouse_configured']:
            status_lines.append("\n⚠️  Action Required to Run App:")
            status_lines.append("  1. Open the app in Snowsight (URL above)")
            status_lines.append("  2. Click Settings → Configure query warehouse")
            status_lines.append("  3. Select a warehouse from the dropdown")
    else:
        # No validation, show generic message
        status_lines.append("\nNext Steps:")
        status_lines.append("  • Open the app in Snowsight")
        status_lines.append("  • Configure query warehouse if not set")
        status_lines.append("  • Use --validate flag to check configuration status")

    status_lines.append("")

    if open_:
        typer.launch(url)

    return MessageResult("\n".join(status_lines))
```

---

### **PHASE 5: Enhanced Help & Documentation**
**Priority**: P2 (Nice to Have)
**Effort**: 0.5 days

#### 5.1 **Improve --help Output**
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Update Docstring**:
```python
@app.command("deploy", requires_connection=True)
@with_project_definition()
@with_experimental_behaviour()
def streamlit_deploy(...) -> CommandResult:
    """
    Deploys a Streamlit app defined in snowflake.yml.

    REQUIREMENTS:
      • A snowflake.yml file in the current directory
      • Entity definition for your Streamlit app

    EXAMPLE snowflake.yml:

      definition_version: 2
      entities:
        my_streamlit:
          type: streamlit
          main_file: streamlit_app.py
          query_warehouse: COMPUTE_WH
          artifacts:
            - streamlit_app.py
            - requirements.txt

    DEPLOYMENT PROCESS:
      1. Validates configuration
      2. Bundles artifacts (including auto-detected packages)
      3. Uploads files to Snowflake stage
      4. Creates Streamlit object
      5. Configures permissions

    PACKAGE SUPPORT:
      The command automatically detects packages from:
      • requirements.txt
      • environment.yml

      Packages are included in the deployment automatically.

    FLAGS:
      --show-sql      Display generated SQL commands
      --validate      Check configuration after deployment
      --replace       Replace existing Streamlit app
      --open          Open app in browser after deployment

    For more information: https://docs.snowflake.com/streamlit
    """
```

#### 5.2 **Add snow streamlit init Command** (Optional)
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Add New Command**:
```python
@app.command("init", requires_connection=False)
def streamlit_init(
    name: str = typer.Argument(..., help="Name for the Streamlit app"),
    main_file: str = typer.Option("streamlit_app.py", help="Main Streamlit file"),
    warehouse: Optional[str] = typer.Option(None, help="Query warehouse for the app"),
):
    """
    Initialize a new Streamlit project with snowflake.yml template.

    Creates:
      • snowflake.yml with Streamlit configuration
      • Main Streamlit file (if doesn't exist)
      • requirements.txt template
    """
    import yaml
    from pathlib import Path

    project_dir = Path.cwd()

    # Check if snowflake.yml already exists
    yml_path = project_dir / "snowflake.yml"
    if yml_path.exists():
        raise ClickException(
            "snowflake.yml already exists in current directory.\n"
            "Use a different directory or remove the existing file."
        )

    # Create snowflake.yml
    config = {
        'definition_version': 2,
        'entities': {
            name: {
                'type': 'streamlit',
                'main_file': main_file,
                'artifacts': [main_file, 'requirements.txt'],
            }
        }
    }

    if warehouse:
        config['entities'][name]['query_warehouse'] = warehouse

    with open(yml_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    console = CliConsole()
    console.message(f"✓ Created snowflake.yml")

    # Create main file if doesn't exist
    main_path = project_dir / main_file
    if not main_path.exists():
        with open(main_path, 'w') as f:
            f.write('''import streamlit as st

st.title("Hello Snowflake!")
st.write("Welcome to your Streamlit app.")

# Example: Query Snowflake
# df = session.sql("SELECT CURRENT_VERSION()").to_pandas()
# st.dataframe(df)
''')
        console.message(f"✓ Created {main_file}")

    # Create requirements.txt if doesn't exist
    req_path = project_dir / "requirements.txt"
    if not req_path.exists():
        with open(req_path, 'w') as f:
            f.write('''# Python package dependencies
streamlit
''')
        console.message(f"✓ Created requirements.txt")

    console.message(f"\nProject initialized! Next steps:")
    console.message(f"  1. Edit {main_file} with your Streamlit code")
    console.message(f"  2. Add package dependencies to requirements.txt")
    if not warehouse:
        console.message(f"  3. Add query_warehouse to snowflake.yml")
    console.message(f"  4. Run: snow streamlit deploy {name}")

    return MessageResult("Streamlit project initialized successfully!")
```

#### 5.3 **Add Better Error for Missing snowflake.yml**
**File**: `src/snowflake/cli/_plugins/streamlit/commands.py`

**Update Exception Handling**:
```python
@app.command("deploy", requires_connection=True)
def streamlit_deploy(...) -> CommandResult:
    try:
        # ... existing logic ...

    except NoProjectDefinitionError as e:
        raise ClickException(
            "╭─ Error: No snowflake.yml Found ─────────────────────────╮\n"
            "│                                                          │\n"
            "│  Streamlit deployment requires a project definition.    │\n"
            "│                                                          │\n"
            "│  Quick start:                                            │\n"
            "│    snow streamlit init my_streamlit                      │\n"
            "│                                                          │\n"
            "│  Or manually create snowflake.yml:                       │\n"
            "│    https://docs.snowflake.com/streamlit                 │\n"
            "│                                                          │\n"
            "╰──────────────────────────────────────────────────────────╯"
        )
```

---

### **PHASE 6: Role/Permission Validation (Advanced)**
**Fixes**: Issue 8
**Priority**: P3 (Could Have)
**Effort**: 1 day

#### 6.1 **Add Schema Access Detection** (Optional)
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**Add New Method**:
```python
def _detect_referenced_schemas(self) -> List[str]:
    """Parse main_file to detect referenced schemas."""
    schemas = set()

    try:
        main_file_path = self.root / self._entity_model.main_file
        if not main_file_path.exists():
            return []

        with open(main_file_path, 'r') as f:
            content = f.read()

        # Pattern 1: session.table("DATABASE.SCHEMA.TABLE")
        import re
        pattern1 = r'session\.table\(["\']([^"\']+)["\']'
        matches1 = re.findall(pattern1, content)
        for match in matches1:
            parts = match.split('.')
            if len(parts) >= 2:
                schema = '.'.join(parts[:-1])  # DATABASE.SCHEMA
                schemas.add(schema)

        # Pattern 2: Direct SQL with FROM clause
        pattern2 = r'FROM\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)'
        matches2 = re.findall(pattern2, content, re.IGNORECASE)
        schemas.update(matches2)

    except Exception as e:
        log.debug(f"Failed to detect schemas: {e}")

    return list(schemas)
```

#### 6.2 **Add Permission Validation Warning**
**File**: `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`

**In `deploy()` After Successful Deployment**:
```python
def deploy(self, action_context: ActionContext, ...):
    # ... existing deploy logic ...

    # NEW: Check for schema references
    schemas = self._detect_referenced_schemas()
    if schemas:
        console.warning(
            f"\n⚠️  Detected Schema References:\n" +
            "\n".join(f"  • {schema}" for schema in schemas) +
            f"\n\n  Ensure your role has access to these schemas.\n"
            f"  The Streamlit app runs with the owner role: {self._conn.role}"
        )

    return url
```

---

## 🔧 Testing Plan

### Unit Tests to Add/Update:

1. **test_validate_configuration**
   - Test missing main_file
   - Test missing artifacts
   - Test missing query_warehouse (should warn, not error)
   - Test invalid file paths

2. **test_detect_packages**
   - Test requirements.txt parsing
   - Test environment.yml parsing
   - Test version specifiers (==, >=, ~=)
   - Test with no package files

3. **test_bundle_with_packages**
   - Test environment.yml generation
   - Test with existing environment.yml
   - Test package deduplication

4. **test_deploy_with_validation**
   - Test successful deploy with validation
   - Test validation results structure
   - Test warehouse validation

5. **test_error_messages**
   - Test NoneType error handling
   - Test SQL error formatting
   - Test missing snowflake.yml error

6. **test_show_sql_flag**
   - Test SQL output formatting
   - Test with legacy deployment
   - Test with versioned deployment

### Integration Tests:

1. **test_full_deploy_with_requirements**
   - Create test project with requirements.txt
   - Run deploy
   - Verify environment.yml generated
   - Verify Streamlit created with packages

2. **test_deploy_missing_artifacts**
   - Create test project with invalid artifacts
   - Run deploy
   - Verify clear error message

3. **test_deploy_without_warehouse**
   - Create test project without query_warehouse
   - Run deploy
   - Verify warning message displayed

4. **test_deploy_with_show_sql**
   - Run deploy with --show-sql
   - Verify SQL output

5. **test_deploy_with_validate**
   - Run deploy with --validate
   - Verify validation results in output

6. **test_streamlit_init_command**
   - Run init command
   - Verify files created
   - Verify snowflake.yml structure

### Manual Testing Checklist:

- [ ] Test Attempt 1 scenario (missing snowflake.yml)
- [ ] Test Attempt 2 scenario (timeout with feedback)
- [ ] Test Attempt 3 scenario (NoneType with clear error)
- [ ] Test Attempt 4 scenario (query_warehouse works)
- [ ] Test Attempt 5 scenario (no silent failures)
- [ ] Test Issue 6 scenario (warehouse warning)
- [ ] Test Issue 7 scenario (packages auto-detected)
- [ ] Test with real Snowflake account
- [ ] Test with various requirements.txt formats
- [ ] Test --show-sql output
- [ ] Test --validate output

---

## 📊 Summary of Changes by File

### `src/snowflake/cli/_plugins/streamlit/streamlit_entity_model.py`
**Changes**: None required (artifacts already Optional)

### `src/snowflake/cli/_plugins/streamlit/streamlit_entity.py`
**New Methods**:
- `_validate_configuration()` - Validate YAML before deployment
- `_detect_packages()` - Parse requirements.txt/environment.yml
- `_validate_warehouse()` - Check warehouse exists
- `_post_deploy_validation()` - Validate after deployment
- `_detect_referenced_schemas()` - Parse schema references (optional)

**Modified Methods**:
- `bundle()` - Add environment.yml generation, error handling
- `deploy()` - Add validation, progress indicators, error handling
- `_deploy_legacy()` - Add progress indicators, show-sql support
- `_deploy_versioned()` - Add progress indicators, show-sql support

**Total**: ~200 lines added

### `src/snowflake/cli/_plugins/streamlit/commands.py`
**New Flags**:
- `--show-sql` - Display generated SQL
- `--validate` - Post-deployment validation
- `--interactive` - Interactive prompts (optional)

**New Commands**:
- `snow streamlit init` - Initialize project (optional)

**Modified**:
- `streamlit_deploy()` - Better error handling, improved success message
- Docstring updates

**Total**: ~150 lines added/modified

### Test Files to Update
- `tests/streamlit/test_streamlit_entity.py` - Add validation tests
- `tests/streamlit/test_streamlit_commands.py` - Add command tests
- `tests_integration/test_streamlit.py` - Add integration tests

**Total**: ~300 lines of tests

---

## ⚠️ Breaking Changes

**NONE** - All changes are backwards compatible:
- New flags are optional
- Existing behavior preserved
- Only adds warnings and better errors
- Package detection is automatic but non-breaking

---

## ✅ Success Criteria

After implementing all phases, the following should be true:

### Error Handling:
- ✅ No NoneType iteration errors
- ✅ No silent failures
- ✅ All errors have clear, actionable messages
- ✅ Stack traces only shown with --debug

### User Experience:
- ✅ Progress indicators at every step
- ✅ Clear validation before deployment
- ✅ Warnings for missing/invalid configuration
- ✅ Success message shows actual status

### Package Support:
- ✅ requirements.txt automatically detected
- ✅ environment.yml automatically detected
- ✅ Packages included in deployment
- ✅ User notified about package configuration

### Query Warehouse:
- ✅ query_warehouse in YAML works correctly
- ✅ Warning if warehouse missing
- ✅ Validation checks warehouse exists
- ✅ Post-deployment confirms configuration

### Transparency:
- ✅ --show-sql displays generated SQL
- ✅ --validate checks deployment status
- ✅ Clear indication of what's configured vs what's needed

### Metrics:
- ⏱️ **Time to successful deployment**: < 2 minutes (from 30+ minutes)
- 😊 **Frustration level**: Low (from High)
- ❌ **Silent failures**: 0 (from multiple)
- ✅ **First-try success rate**: > 80% (from < 20%)

---

## 📅 Implementation Timeline

**Total Estimated Effort**: 3-5 days

### Day 1: Phase 1 (Critical Error Handling)
- Morning: Validation and error messages
- Afternoon: Progress indicators and silent failure fixes

### Day 2: Phase 2 (Package Support)
- Morning: Package detection
- Afternoon: Bundle updates and testing

### Day 3: Phase 3 & 4 (Warehouse + Validation)
- Morning: Warehouse validation
- Afternoon: Post-deployment validation and messaging

### Day 4: Testing
- Morning: Unit tests
- Afternoon: Integration tests

### Day 5: Phase 5 & 6 (Polish)
- Morning: Documentation and help improvements
- Afternoon: Optional features (init command, schema detection)

---

## 🚀 Rollout Plan

1. **Internal Testing** (Day 6)
   - Test with various projects
   - Validate all 8 scenarios from feedback
   - Get team feedback

2. **Beta Release** (Week 2)
   - Release to limited users
   - Gather feedback
   - Fix any issues

3. **General Availability** (Week 3)
   - Release to all users
   - Update documentation
   - Announce improvements

---

## 📝 Documentation Updates Needed

1. **Update CLI docs** with new flags
2. **Add troubleshooting guide** addressing common issues
3. **Create quickstart** with snow streamlit init
4. **Add examples** for requirements.txt/environment.yml
5. **Update error reference** with new error messages

---

## 🎯 Success Metrics (3 Months Post-Release)

- 📉 Support tickets related to Streamlit deployment: -80%
- ⭐ User satisfaction with deployment: > 4.5/5
- ⏱️ Average deployment time: < 5 minutes
- ✅ First-try success rate: > 85%
- 📝 Documentation clarity rating: > 4.0/5

---

## Appendix: Current vs Expected User Experience

### Current Experience:
```
$ snow streamlit deploy my_app
Error: No such option: --name
$ snow streamlit deploy --help
# ... no mention of snowflake.yml ...
$ snow streamlit deploy entity_id
# ... hangs for 2 minutes ...
'NoneType' object is not iterable
# ... user gives up, uses Python API instead ...
```

### Expected Experience After Fixes:
```
$ snow streamlit deploy my_streamlit

Validating configuration...
  ⚠️  No query_warehouse specified. Configure in Snowsight after deployment.
Bundling artifacts...
  Found requirements.txt, reading packages...
  Found 3 packages
  ✓ Generated environment.yml with 3 packages
Uploading artifacts to stage...
  • streamlit_app.py
  • environment.yml
Creating Streamlit object...
  ✓ Created STREAMLIT DB.SCHEMA.MY_STREAMLIT
Configuring permissions...
  ✓ Granted privileges
Deployment complete!

============================================================
✓ Streamlit Deployment Complete!
============================================================

App URL: https://app.snowflake.com/.../my_streamlit

Next Steps:
  • Open the app in Snowsight
  • Configure query warehouse if not set
  • Use --validate flag to check configuration status

# Time elapsed: 45 seconds
# Frustration level: None
# Success: Yes
```

---

**End of Plan**
