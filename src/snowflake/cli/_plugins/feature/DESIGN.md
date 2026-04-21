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

# Feature Plugin — Design Document

## Overview

The `snow feature` CLI plugin provides a declarative workflow for managing
Snowflake feature-store objects (online feature tables, offline backing tables,
and related pipelines). It follows the same pluggy + Typer architecture used
by all other built-in snowflake-cli plugins.

---

## Plugin Architecture

```
_plugins/feature/
├── __init__.py          # Empty namespace package marker
├── plugin_spec.py       # Pluggy hook registration
├── commands.py          # Typer command definitions (CLI surface)
└── manager.py           # FeatureManager (business logic / SQL execution)
```

The plugin is registered in:
```
_app/commands_registration/builtin_plugins.py
```
under the key `"feature"`.

### Registration flow

1. `builtin_plugins.py` imports `plugin_spec as feature_plugin_spec`.
2. `CommandPluginsLoader.register_builtin_plugins()` iterates the dict and
   registers each spec via pluggy.
3. `plugin_spec.command_spec()` (decorated with `@plugin_hook_impl`) returns
   a `CommandSpec` pointing at `commands.app.create_instance()`.
4. The Typer app is mounted at the root Snow CLI command path as a sub-group.

---

## Command → FeatureManager mapping

| CLI command             | Manager method       | Notes                                |
|-------------------------|----------------------|--------------------------------------|
| `snow feature init`     | `init()`             | Creates schema/tags, scaffolds dirs  |
| `snow feature apply`    | `apply()`            | Orchestrates full load→plan→execute  |
| `snow feature plan`     | `apply(dry_run=True)`| Alias for apply in dry-run mode      |
| `snow feature list`     | `list_specs()`       | Files → from file; no args → Snowflake |
| `snow feature describe` | `describe()`         | Single-object metadata lookup        |
| `snow feature drop`     | `drop()`             | Drops one or more objects            |
| `snow feature convert`  | `convert()`          | Python DSL → YAML or JSON            |

---

## FeatureManager and the decl library

`FeatureManager` extends `SqlExecutionMixin` so it can call
`self.execute_query(sql)` against the active Snowflake connection.

It imports the shared library at module level with a try/except guard:

```python
try:
    from snowflake.ml.feature_store.decl import api as decl_api
except ImportError:
    decl_api = None
```

All calls to `decl_api.*` are wrapped in individual `try/except` blocks that
catch `NotImplementedError` (raised by Phase 0 stubs) and any other
exceptions. When caught, the method returns a placeholder dict so the CLI
remains functional during parallel Phase 1 development.

### apply() orchestration

```
1. Expand file globs
2. decl_api.load_specs(files, config)       → SpecBatch
3. execute_query("SHOW ONLINE FEATURE TABLES ...") → raw state
4. decl_api.fetch_applied_state(raw_results) → AppliedState
5. decl_api.validate_specs(batch, state)    → ValidationResult[]
6. decl_api.generate_plan(batch, state, opts) → Plan
7. Display plan ops
8. If not dry_run: execute each op's SQL
9. Return result dict
```

---

## --json output

All commands return a `MessageResult` wrapping a JSON-serialised dict.
The global `--format json` flag (injected by `global_options_with_connection`)
controls whether the Snow CLI output layer emits a table or raw JSON.
No additional work is needed inside the plugin.

---

## Parameter naming constraints

The Snow CLI global options inject the following parameter names into every
`requires_connection=True` command automatically. Plugin commands **must not**
define parameters with these names:

- **GLOBAL_CONNECTION_OPTIONS**: `connection`, `host`, `port`, `account`,
  `user`, `password`, `authenticator`, `workload_identity_provider`,
  `private_key_file`, `session_token`, `master_token`, `token`,
  `token_file_path`, `database`, `schema`, `role`, `warehouse`,
  `temporary_connection`, `mfa_passcode`, `enable_diag`, `diag_log_path`,
  `diag_allowlist_path`, and others.
- **GLOBAL_OPTIONS**: `format`, `verbose`, `debug`, `silent`,
  `enhanced_exit_codes`.

The `describe` and `drop` commands accept `--database`/`--schema` via the
global connection flags (not as custom parameters). The `convert` command uses
`--file-format` (not `--format`) to avoid conflicting with the global output
format flag.

---

## How to add a new command

1. Add a new method to `FeatureManager` in `manager.py` with the desired
   orchestration logic, wrapping all `decl_api` calls in try/except.
2. Add a new `@app.command(requires_connection=True)` function in
   `commands.py`. Use `Optional[List[str]]` for variadic arguments, and avoid
   parameter names reserved by global options (see above).
3. Add failing tests in `tests/feature/test_commands.py` and
   `tests/feature/test_manager.py` before writing the implementation (TDD).
4. Run `python -m pytest tests/feature/` to confirm red/green cycle.

---

## Phase 3 integration

Phase 3 will replace the `NotImplementedError`-catching stubs with real calls
once the `decl` library (Phase 1) is complete. No changes to `commands.py`
are expected — only `manager.py` wiring will be updated.
