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

All non-`init` Snowflake-bound commands take `--from <project_root>` (default cwd) and `--target <manifest target>` (default `manifest.default_target`). There are no positional spec-file arguments — the manifest is the single project descriptor. `--variable / -D key=value` repeats override the manifest's `templating:` block.

| CLI command             | Manager method       | Notes                                |
|-------------------------|----------------------|--------------------------------------|
| `snow feature init`     | `init()`             | Auto-derives `manifest.yml` from the active connection (D6: queries `get_account_identifier(connection)` for the canonical `<ORG>-<ACCOUNT>` form, copies `database`/`schema`/`role`); scaffolds `sources/{entities,datasources,feature_views}/` + `out/plan/.gitkeep`; calls `FeatureStore(creation_mode=CREATE_IF_NOT_EXIST)`. The global `--database` / `--schema` flags are forwarded to `init(database=..., schema=...)`: on a fresh manifest they win over the connection profile and are baked into the new target; on a re-init they MUST match the resolved manifest target's stored values, otherwise `init` aborts with `CliError` (the manifest is the source of truth on re-init — preserved bytes-identical, no `--force`). |
| `snow feature apply`    | `apply()`            | Pure plan-file consumer (auto-discovers latest `feature_plan_*.json` under `<project_root>/out/plan/` or honours `--plan`). L6 now checks BOTH (a) `target.account_identifier` matches `get_account_identifier(connection)` and (b) plan envelope's `target_name` matches `--target` (D4-ext). |
| `snow feature plan`     | `plan()` + `write_plan()` | Validate + generate_plan; persists JSON to `<project_root>/out/plan/feature_plan_<ts>.json` on success. `--dev` threads through to `decl_api.validate_specs(dev_mode=...)` so version invariants are properly relaxed. |
| `snow feature list`     | `list_specs()`       | Lists Snowflake state for the resolved manifest target. |
| `snow feature describe` | `describe()`         | Single-object metadata lookup        |

### Manifest discovery + target resolution (`_resolve_project`)

Every Snowflake-bound entry point that needs a target (`apply`, `plan`,
`list_specs`, `describe`, `export_specs`) goes through
`FeatureManager._resolve_project(from_dir, target_name)` first, which:

1. Calls `decl_api.discover_project(from_dir)` to walk up to
   `manifest.yml`.  Raises `ManifestNotFoundError` (rendered by the
   CLI as a `CliError`) when no manifest exists.
2. Calls `decl_api.resolve_target(manifest, target_name)`.  When
   `target_name` is None, falls back to `manifest.default_target`;
   raises if neither is set.
3. Asserts `AccountIdentifier.from_string(target.account_identifier)
   == get_account_identifier(connection)` (D4 match-account-override-rest).
   On mismatch, returns `target_mismatch` to the caller before any
   state SQL runs — the operator picks a different connection or
   fixes the manifest.

`_resolve_project` returns `(FSProjectPaths, FSManifest, TargetContext)`.  The caller threads the target's `database` / `schema` / `role` through every downstream `decl_api.*` call (D4 override-rest); `warehouse` is read from `ctx.connection.warehouse` (plan files are warehouse-agnostic by design).

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

### Session priming (removed)

The declarative client no longer issues an `ALTER SESSION` to enable
`ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION` — the parameter is
enabled by default at the account level. See
[`declarative_feature_store/ARCHITECTURE.md`](../../../../../../declarative_feature_store/ARCHITECTURE.md)
("Strict spec contract") and `snowml/.../decl/DESIGN.md` ("no
client-side session priming is issued"). The
`_ensure_session_setup()` helper, the `_session_setup_done` gate, and
`decl_api.ensure_session_setup` / `SessionSetupError` are all gone;
every Snowflake-bound entry point goes straight to its state SQL
after the manifest resolution + init-first guard.

### apply() orchestration (pure plan-file consumer)

`apply()` no longer re-plans from source.  It reads a serialised
`Plan` from disk (auto-discovered under `<project_root>/out/plan/`,
or supplied via ``--plan``) and hands it to ``decl_api.execute_plan``
for execution:

```
1. paths, manifest, target = self._resolve_project(from_dir, target_name)
   — manifest discovery + L6 account-match assertion (D4)
2. plan_file = explicit --plan path OR _discover_unapplied_plan(paths.plans_dir)
3. plan = decl_api.deserialize_plan(open(plan_file).read())                 → Plan
4. Verify plan.target_name == target.name (D4-ext name-strict)
5. decl_api.execute_plan(
       plan, session,
       database=target.database,            # from manifest, not the plan envelope
       schema=target.schema,                # from manifest, not the plan envelope
       warehouse=ctx.connection.warehouse,  # warehouse-agnostic plan files
       options=...,
   )
   — the executor constructs FeatureStore(...) directly against the
     borrowed Snowpark session; no session priming is issued
6. On success, rename plan_file → plan_file + ".applied" (L4)
7. On failure, leave plan_file untouched so the operator can retry (L5)
8. Return result dict
```

`status` values returned to the CLI:

- `"applied"`  — every op executed successfully; plan file renamed `.applied` (L4).
- `"refused"`  — the plan carried at least one `destructive=True` op
  and `--allow-recreate` was not set; **no op was executed**, and the
  plan file stays unrenamed under L5 so a follow-up
  `snow feature apply --allow-recreate` consumes the same file.
  The gate is enforced inside `decl_api.execute_plan` (single source
  of truth); the manager simply threads the status through.  `errors`
  carries a single human-readable directive naming the destructive op
  count and the `--allow-recreate` remediation.
- `"target_mismatch"`  — either (a) the manifest target's
  `account_identifier` does not match `get_account_identifier(connection)`
  (D4), or (b) the plan envelope's `target_name` differs from the
  requested `--target` (D4-ext); plan file untouched.
- `"validation_failed"`  — planner-side ERROR severities surfaced
  (only reachable when `apply` is in a hypothetical re-plan path; the
  pure-consumer `apply` returns this only when `deserialize_plan`
  itself raises).
- `"partial_failure"`  — one or more ops raised at execution time;
  plan file untouched (L5).
- `"no_plan"`  — auto-discovery found no unapplied plan under
  `<project_root>/out/plan/` (L1).

### plan() orchestration

`plan()` is the read-only validate-then-plan path that backs
`snow feature plan`.  It never touches Snowflake side-effecting SQL:

```
1. paths, manifest, target = self._resolve_project(from_dir, target_name)
   — manifest discovery + L6 account-match assertion (D4)
2. queries = decl_api.state_queries(target.database, target.schema)         → fetch applied state
3. decl_api.fetch_applied_state(...)                                        → AppliedState
4. batch = decl_api.load_project(
       paths.project_root,
       target=target,
       runtime_vars=_parse_variables(variables) or None,
   )                                                                        → SpecBatch
   — walks sources/{entities,datasources,feature_views}/, applies
     templating with merged precedence, skips UDF companion .py files
5. decl_api.resolve_datasource_columns(batch)                               → mutates batch in place
6. decl_api.validate_specs(batch, state,
       target_database=target.database,
       target_schema=target.schema,
       dev_mode=dev_mode,
   )
   → if any ERROR results, return {status: "validation_failed", errors: [...], ops: []}
7. decl_api.generate_plan(batch, state, opts,
       database=target.database, schema=target.schema)                      → Plan
8. Return {**target_info, status: "ready", ops: [...], executed: 0, warnings: [...], errors: []}
```

`commands.plan` then forwards the result to `manager.write_plan(...)`
on success, persisting the same `Plan` object to
`<project_root>/out/plan/feature_plan_<UTC ts>.json` (or to `--out
<path>` if specified) with `target_name` populated in the envelope
for `apply()` to consume.  The two paths share the same
`load_project` → `resolve_datasource_columns` → `validate_specs` →
`generate_plan` chain — the parity invariant between the UI op
stream and the disk plan is structural, not trip-wire-policed.

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
when the SPECIFICATION call fails. `DESCRIBE … TYPE = SPECIFICATION`
is enabled by default at the account level — no client-side `ALTER
SESSION` is issued. See
[`declarative_feature_store/ARCHITECTURE.md`](../../../../../../declarative_feature_store/ARCHITECTURE.md)
("Strict spec contract") for the full contract.

### export_specs() orchestration

`snow feature export` is a strict, full-fidelity-only command. Every
emitted YAML carries the authoritative spec recovered via
`DESCRIBE ONLINE FEATURE TABLE … TYPE = SPECIFICATION`, or the entire
command aborts with a clear error naming the OFT(s) for which the
SPECIFICATION JSON could not be recovered. There is no
column-DESCRIBE-only fallback and no reduced / flagged YAML output —
every emitted spec is full-fidelity or the command fails.

```
1. show_rows, applied_state, entity_rows, feature_group_rows, spec_map =
       self._fetch_applied_state_bundle(target)                              → shared with plan / write_plan
2. yaml_files = decl_api.export_specs(
       show_rows,
       {},
       project_root,
       database,
       schema,
       specification_map=spec_map,                                            → authoritative for non-FV-kind OFTs
       entity_rows=entity_rows,                                                 (e.g. FeatureGroup-backing rows)
       feature_group_rows=feature_group_rows,
       applied_state=applied_state,                                          → recovered BatchFV sources / advanced
       layout="sources",                                                       fields / offline-only BFVs land here
   )
3. Write each YAML file under the output directory
```

The `_fetch_applied_state_bundle` helper is the same bundle the planner
consumes: it issues `state_queries(...)` (which includes
`show_dynamic_tables` for BatchFV source recovery), fans the per-OFT
`DESCRIBE … TYPE = SPECIFICATION` template out, hands the DT text
through `state._inject_batch_fv_source_from_dt_text` +
`_inject_advanced_bfv_fields_from_dt_text`, and surfaces offline-only
BFVs via `state._build_offline_fv_object`.  The `applied_state` kwarg
forwarded into `export_specs` is what makes init-time YAML carry the
same `sources[]` / advanced-field / offline-only payloads the planner
would have computed for a `snow feature plan` against the same
runtime.

`_fetch_oft_state` is the same helper that backs `list_specs()` — it
runs the SHOW queries, fans the per-OFT
`DESCRIBE … TYPE = SPECIFICATION` template out, parses each result
via `decl_api.parse_specification_rows(...)`, and returns
`(applied_state, specification_map)` in a single pass. There is no
separate column-DESCRIBE loop in `export_specs`; the previous
fallback that built partial YAML from column metadata alone has been
removed.

`decl_api.export_specs(...)` raises if `specification_map[oft]` is
missing or empty for any FV `AppliedObject`. The strict-export error
propagates without being caught — Click renders it as a normal
command failure, and no YAML files are written when the command
aborts.

---

## Stream-source applied-state read path

`StreamingSource` is registered metadata in Snowflake — `FeatureStore.list_stream_sources()` (backed by `SYSTEM$LIST_FEATURE_STORE_OBJECTS('STREAM_SOURCE')`) is the authoritative read surface, and the declarative library hides it behind `decl_api.fetch_stream_source_rows(session, database, schema, warehouse="")`. The CLI manager threads the runtime rows into `decl_api.fetch_applied_state(stream_source_rows=...)` so the planner's four-way source-side decision (`NO_CHANGE` / `UPDATE_SOURCE` / `RECREATE_SOURCE` / `CREATE_SOURCE`) sees runtime-authoritative `Datasource` AppliedObjects instead of falling through to the FV-derivation pre-pass and re-emitting `CREATE_SOURCE` on every plan. Without this threading, an unchanged `StreamingSource` YAML keeps producing `CREATE_SOURCE` ops, and `snow feature apply` keeps materialising the historical `UserWarning: StreamSource <name> already exists. Skip registration.` noise.

The plumbing is intentionally lean: a single private helper on `FeatureManager`, invoked at the two `decl_api.fetch_applied_state(...)` call sites and nowhere else.

### `init --python`: Python-form export

When `snow feature init --python` is passed, `init()` routes to
`decl_api.export_specs_as_python()` instead of `decl_api.export_specs()`.
The exported `.py` files contain module-level Pydantic constructor calls for
every deployed object, loadable by `loader.load_python_file()` and planning
as `NO_CHANGE` on round-trip.  No YAML files are written when `--python` is set.

The dispatch lives in `_export_into_sources()`:

```python
_export_fn = decl_api.export_specs_as_python if python else decl_api.export_specs
return _export_fn(
    show_rows, {}, str(project_root), database, schema,
    specification_map=specification_map,
    entity_rows=entity_rows,
    feature_group_rows=feature_group_rows,
    applied_state=applied_state,
    layout="sources",
)
```

The `layout="sources"` argument is unchanged — files are still written under
`sources/{entities,datasources,feature_views}/` just with `.py` extension.

---



```python
def _fetch_stream_source_rows(self, target: FSTarget) -> list[dict[str, Any]]:
    """Fetch registered stream-source rows for *target*.

    Mirror of :func:`_fetch_entity_rows` for the imperative
    ``FeatureStore.list_stream_sources()`` enumeration that
    :func:`decl_api.fetch_stream_source_rows` wraps.  Without this
    wiring the planner has no runtime-authoritative view of
    ``Datasource(kind="StreamingSource")`` registrations, so a
    plain re-plan of an unchanged YAML re-emits a spurious
    ``CREATE_SOURCE`` (and any subsequent ``CREATE_FV`` that
    consumes it re-issues a defensive ``register_stream_source``
    ``UserWarning``).  See ``plans/stream_source_contract.md`` §8.
    """
    from snowflake.ml.feature_store.decl.errors import (
        FeatureStoreNotInitializedError,
    )

    ctx = get_cli_context()
    try:
        session = self._build_session()
        return decl_api.fetch_stream_source_rows(
            session,
            target.database,
            target.schema,
            ctx.connection.warehouse or "",
        )
    except FeatureStoreNotInitializedError:
        raise
    except Exception as exc:  # noqa: BLE001 — defensive permission/error swallow
        log.debug("fetch_stream_source_rows failed (treating as empty): %s", exc)
        return []
```

The contract has three operationally important guarantees:

1. **Init-first propagation.** `FeatureStoreNotInitializedError` is *not* swallowed — it is re-raised so the command-level wrapper in `commands.py` converts it into a `ClickException` whose message directs the operator at `snow feature init`. This matches the `_fetch_entity_rows` / `_fetch_feature_view_rows` / `_fetch_feature_group_rows` precedent so a partially-initialised schema fails fast and consistently across every `snow feature` entry point that issues state SQL.
2. **Graceful-degrade-on-permission-error.** Any other exception (most commonly a privilege gap on `SYSTEM$LIST_FEATURE_STORE_OBJECTS('STREAM_SOURCE')`) is debug-logged and downgraded to an empty list. The downstream `fetch_applied_state` merge treats `stream_source_rows=[]` identically to `stream_source_rows=None` (FV-derived-only), so a missing privilege downgrades fidelity rather than crashing the bundle.
3. **Stateless.** The helper does not cache between calls; each `plan()` / `write_plan()` invocation re-issues `list_stream_sources()`. This is symmetric with the rest of the bundle and avoids stale-cache surprises across long-running CLI sessions.

### Threading into `fetch_applied_state` at both `plan()` and `write_plan()` call sites

Both methods load the runtime rows alongside the existing entity / FV / FG rows and forward them through `decl_api.fetch_applied_state(stream_source_rows=...)`. The two call sites are intentionally byte-equivalent so the UI op stream and the disk plan see the same applied-state snapshot — preserving the parity invariant pinned by `scripts/verify_plan_ui_parity.sh`:

```python
# manager.plan(...) — UI op stream (≈ L880)
entity_rows         = self._fetch_entity_rows(target)
feature_view_rows   = self._fetch_feature_view_rows(target)
feature_group_rows  = self._fetch_feature_group_rows(target)
stream_source_rows  = self._fetch_stream_source_rows(target)
applied_state = decl_api.fetch_applied_state(
    raw_show, raw_tables,
    specification_map=specification_map,
    dt_text_map=dt_text_map,
    entity_rows=entity_rows,
    feature_view_rows=feature_view_rows,
    feature_group_rows=feature_group_rows,
    stream_source_rows=stream_source_rows,
    default_database=target.database,
    default_schema=target.schema,
)
```

```python
# manager.write_plan(...) — disk JSON (≈ L1010)
entity_rows         = self._fetch_entity_rows(target)
feature_view_rows   = self._fetch_feature_view_rows(target)
feature_group_rows  = self._fetch_feature_group_rows(target)
stream_source_rows  = self._fetch_stream_source_rows(target)
applied_state = decl_api.fetch_applied_state(
    raw_show, raw_tables,
    specification_map=specification_map,
    dt_text_map=dt_text_map,
    entity_rows=entity_rows,
    feature_view_rows=feature_view_rows,
    feature_group_rows=feature_group_rows,
    stream_source_rows=stream_source_rows,
    default_database=target.database,
    default_schema=target.schema,
)
```

`apply()` is unaffected — it is a pure plan-file consumer and never re-runs `fetch_applied_state`. The plan envelope already carries the four-way source decision the planner emitted at `snow feature plan` time, so the executor only needs to dispatch ops by `payload["kind"]`. See `decl/imperative_executor.py` for the StreamingSource-vs-BatchSource branch contract (StreamingSource calls `register_stream_source` / `update_stream_source` / `delete_stream_source`; BatchSource records informational no-ops).

**Bundle-tuple shape is unchanged.** The shared `_fetch_applied_state_bundle(target)` helper continues to return `(raw_show, applied_state, entity_rows, feature_group_rows, specification_map)` — `stream_source_rows` is consumed inside `fetch_applied_state` and never escapes into a tuple field, mirroring the contract for `feature_view_rows`. Callers downstream of the bundle (the exporter, `_export_into_sources`, the round-trip helpers) are unaffected.

### Test coverage

`snowflake-cli/tests/feature/test_manager.py` carries regression coverage that:

- `_fetch_stream_source_rows(target)` is invoked exactly once per `plan()` and per `write_plan()` invocation;
- the resulting list is forwarded into `decl_api.fetch_applied_state` as the `stream_source_rows=` keyword (mirroring the existing `entity_rows=` / `feature_group_rows=` threading tests);
- `FeatureStoreNotInitializedError` propagates from `_fetch_stream_source_rows` and is converted to a `ClickException` by the command-level wrapper;
- a generic exception inside `decl_api.fetch_stream_source_rows` is debug-logged and downgraded to `[]` so a privilege gap does not break the plan.

The contract source of truth is `plans/stream_source_contract.md` §8.

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
