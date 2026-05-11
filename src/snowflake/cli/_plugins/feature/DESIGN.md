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
| `snow feature apply`    | `apply()`            | Pure plan-file consumer (auto-discovers latest `feature_plan_*.json` or honours `--plan`) |
| `snow feature plan`     | `plan()`             | Validate + generate_plan; persists JSON via `write_plan()` on success |
| `snow feature list`     | `list_specs()`       | Files → from file; no args → Snowflake |
| `snow feature describe` | `describe()`         | Single-object metadata lookup        |

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

### Session priming

Every Snowflake-bound `FeatureManager` entry point (`apply_specs`,
`write_plan`, `list_specs` (only when `input_files` is empty),
`describe`, `export_specs`, `get_status`, `initialize_service`,
and `destroy_service`) calls
`self._ensure_session_setup()` as its first step. The helper is
gated by `self._session_setup_done` so each `FeatureManager`
instance primes once. It delegates to
`decl_api.ensure_session_setup(self.execute_query)`, which runs the
idempotent `ALTER SESSION SET ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION = TRUE`
priming statement that lives in `decl/session_setup.py`. On
failure, `decl_api.SessionSetupError` propagates — the CLI does not
catch it, so Click renders it as a normal command failure and no
further state SQL runs.

### apply() orchestration (pure plan-file consumer)

`apply()` no longer re-plans from source.  It reads a serialised
`Plan` from disk (auto-discovered or supplied via ``--plan``) and
hands it to ``decl_api.execute_plan`` for execution:

```
1. self._ensure_session_setup()                                             → primes the session
2. plan_file = explicit --plan path OR _discover_unapplied_plan()           → str (path)
3. plan = decl_api.deserialize_plan(open(plan_file).read())                 → Plan
4. Verify plan.target_database / target_schema match the active connection (L6)
5. decl_api.execute_plan(
       plan, session,
       database=plan.target_database,
       schema=plan.target_schema,
       warehouse=ctx.connection.warehouse,   # Bug C: warehouse from connection, not the plan
       options=...,
   )
   — the executor primes the borrowed Snowpark session via
     apply_session_setup_to_session(session) before constructing
     FeatureStore(...)
6. On success, rename plan_file → plan_file + ".applied" (L4)
7. On failure, leave plan_file untouched so the operator can retry (L5)
8. Return result dict
```

`status` values returned to the CLI:

- `"applied"`  — every op executed successfully; plan file renamed `.applied` (L4).
- `"refused"`  — the plan carried at least one `destructive=True` op
  and `--allow-recreate` was not set; **no op was executed**, and the
  plan file stays unrenamed under L5 so a follow-up
  `snow feature apply --allow-recreate <path>` consumes the same file.
  The gate is enforced inside `decl_api.execute_plan` (single source
  of truth); the manager simply threads the status through.  `errors`
  carries a single human-readable directive naming the destructive op
  count and the `--allow-recreate` remediation.
- `"target_mismatch"`  — the plan's `target_database` /
  `target_schema` does not match the active connection (L6); plan
  file untouched.
- `"validation_failed"`  — planner-side ERROR severities surfaced
  (only reachable when `apply` is in a hypothetical re-plan path; the
  pure-consumer `apply` returns this only when `deserialize_plan`
  itself raises).
- `"partial_failure"`  — one or more ops raised at execution time;
  plan file untouched (L5).
- `"no_plan"`  — auto-discovery found no unapplied plan under
  `<cwd>/.snowflake/plans/` (L1).

### plan() orchestration

`plan()` is the read-only validate-then-plan path that backs
`snow feature plan`.  It never touches Snowflake side-effecting SQL:

```
1. self._ensure_session_setup()                                             → primes the session
2. Expand file globs
3. decl_api.load_specs(files, config)                                       → SpecBatch
4. decl_api.resolve_datasource_columns(batch)                               → mutates batch in place
5. queries = decl_api.state_queries(database, schema)                       → fetch applied state
6. decl_api.fetch_applied_state(...)                                        → AppliedState
7. decl_api.validate_specs(batch, state, target_database=..., target_schema=...)
   → if any ERROR results, return {status: "validation_failed", errors: [...], ops: []}
8. decl_api.generate_plan(batch, state, opts, database=..., schema=...)     → Plan
9. Return {**target_info, status: "ready", ops: [...], executed: 0, warnings: [...], errors: []}
```

`commands.plan` then forwards the result to `manager.write_plan(...)`
on success, persisting the same `Plan` object to disk for `apply()`
to consume.  The two paths share the same `resolve_datasource_columns`
→ `validate_specs` → `generate_plan` chain — the parity invariant
between the UI op stream and the disk plan is structural, not
trip-wire-policed.

### list_specs() flow (Snowflake-backed mode)

```
queries = decl_api.list_state_queries(database, schema)
oft_rows     = execute_query(queries["show_ofts"])
entity_rows  = execute_query(queries["show_entities"])
for row in oft_rows:
    sql = queries["describe_specification_template"].format(name=row["name"])
    spec = decl_api.parse_specification_rows(execute_query(sql))
    if spec is not None:
        specification_map[row["name"]] = spec

rows = decl_api.enrich_list_results(
    oft_show_rows=oft_rows,
    entity_show_rows=entity_rows,
    specification_map=specification_map,
    describe_map=describe_columns_map,  # legacy column fallback
)
return {**target_info, "source": "snowflake", "specs": rows}
```

The returned `rows` are a single ordered list with a leading `type` column
(`FeatureView` / `Entity` / `Datasource`), a uniform `name` column, plus
kind-specific `details`. `commands._TABLE_DISPLAY_COLUMNS` projects this into
the table view; the same dict is returned verbatim under `--format json`.

#### Strict spec-only Entity & Datasource rows

All Entity / Datasource rows are authoritative — entities come from
`SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'` and datasources are
derived from `spec.sources[]` recovered via
`DESCRIBE … TYPE = SPECIFICATION`. There is no inference fallback
that synthesizes rows from FV PK columns or `source`-string parsing
when the SPECIFICATION call fails. The session-priming gate at the
top of every Snowflake-bound entry point makes the parameter
available before any state SQL runs; if priming fails the command
exits with `decl_api.SessionSetupError` and no list rows are
produced. See
[`docs/ARCHITECTURE.md`](../../../../../../docs/ARCHITECTURE.md)
"Session setup (DESCRIBE TYPE = SPECIFICATION priming)" for the full
contract.

### export_specs() orchestration

`snow feature export` is a strict, full-fidelity-only command. Every
emitted YAML carries the authoritative spec recovered via
`DESCRIBE ONLINE FEATURE TABLE … TYPE = SPECIFICATION`, or the entire
command aborts with a clear error naming the OFT(s) for which the
SPECIFICATION JSON could not be recovered. There is no
column-DESCRIBE-only fallback and no reduced / flagged YAML output —
every emitted spec is full-fidelity or the command fails.

```
1. self._ensure_session_setup()                                             → primes the session
2. queries = decl_api.export_queries(database, schema)
3. applied_state, specification_map = self._fetch_oft_state(
       queries=queries,
       database=database,
       schema=schema,
   )                                                                         → reuses the LIST-path helper
4. yaml_files = decl_api.export_specs(
       applied_state,
       specification_map=specification_map,
   )                                                                         → strict full-fidelity render
5. Write each YAML file under the output directory
```

`_fetch_oft_state` is the same helper that backs `list_specs()` — it
runs the SHOW queries, fans the per-OFT
`DESCRIBE … TYPE = SPECIFICATION` template out, parses each result
via `decl_api.parse_specification_rows(...)`, and returns
`(applied_state, specification_map)` in a single pass. There is no
separate column-DESCRIBE loop in `export_specs`; the previous
fallback that built partial YAML from column metadata alone has been
removed.

`decl_api.export_specs(...)` raises if `specification_map[oft]` is
missing or empty for any FV `AppliedObject`. `SessionSetupError` and
the strict-export error both propagate without being caught — Click
renders them as normal command failures, and no YAML files are
written when the command aborts.

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

The `describe` command accepts `--database`/`--schema` via the global
connection flags (not as custom parameters).

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
