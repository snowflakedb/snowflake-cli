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
Snowflake feature-store objects. It is a thin adapter: the CLI owns argument
parsing, project-file discovery, and output rendering, while all feature-store
logic and SQL is provided by the `snowflake-ml-python[feature_store]` library
(`snowflake.ml.feature_store.decl.*`, referred to here as `decl_api`). The
plugin issues no feature-store SQL itself — every `SHOW` / `DESCRIBE` / state
query is generated and executed by the library.

### Lifecycle and dependencies

`snow feature` is in **public preview**. Per the CLI lifecycle
(`docs/contributing/lifecycle.md`) the command group is registered as a
built-in but **hidden by default** behind the `ENABLE_FEATURE_STORE`
feature flag (`src/snowflake/cli/api/feature_flags.py`, default `False`).
Users opt in with `SNOWFLAKE_CLI_FEATURES_ENABLE_FEATURE_STORE=true` or
`[cli.features] enable_feature_store = true` in `config.toml`.

The `snowflake-ml-python[feature_store]` library is an **optional** runtime
dependency. When it is absent the plugin still imports and registers (so
`snow feature --help` and each subcommand's `--help` work), and any actual
command invocation fails fast with
`Error: 'snow feature' requires the snowflake-ml-python[feature_store] library`.
This guard, together with the yellow `WARNING: 'snow feature' is in public
preview.` banner (written to stderr, kept off structured stdout), runs from the
per-command `feature_store_preflight` decorator in `commands.py` — applied to
every command rather than the group callback, so `--help` never triggers it.

---

## Plugin Architecture

```
_plugins/feature/
├── __init__.py          # Namespace package marker
├── plugin_spec.py       # Pluggy hook registration
├── commands.py          # Typer command definitions (CLI surface)
├── manager.py           # FeatureManager (adapter to decl_api)
├── models.py            # FSManifest — parsed like DCMManifest
└── exceptions.py        # ManifestNotFound / Invalid / Configuration
```

The plugin is registered in
`_app/commands_registration/builtin_plugins.py` under the key `"feature"`.

### Registration flow

1. `builtin_plugins.py` imports `plugin_spec as feature_plugin_spec`.
2. `CommandPluginsLoader.register_builtin_plugins()` registers each spec via
   pluggy.
3. `plugin_spec.command_spec()` (decorated with `@plugin_hook_impl`) returns a
   `CommandSpec` pointing at `commands.app.create_instance()`.
4. The Typer app is mounted at the root Snow CLI command path as a sub-group.

---

## Commands

All commands take the standard global connection flags. Non-`init` commands
that resolve a project also accept `--from <project_root>` (default cwd,
`SecurePath` via `LocalDirectoryType`, same as `snow dcm`) and
`--target <manifest target>` (default `manifest.default_target`); `manifest.yml`
is the single project descriptor.

`--variable / -D key=value` repeats override the manifest's `templating:`
block, and the flag lives on `plan` **only**. Templating is resolved at plan
time (`_parse_variables` → `decl_api.load_project(..., runtime_vars=...)`) and
the rendered values are baked into the `out/plan/*.json` that `apply` consumes.
The other commands never load or render local specs — `apply` is a pure
plan-file consumer; `list` / `describe` read deployed state (describe reads
local YAML only for example enrichment, not templating); `ingest` / `query`
go straight to the Online Service — so they do not accept `--variable`. Typer
rejects it there rather than silently discarding an override that could never
be honored.

Deletion detection on `plan` is **off by default** (`no_delete=True` →
`PlanOptions(full_directory_mode=False)`): objects deployed in the target but
absent from the manifest are left untouched. Pass `--delete` to run a full
sync that emits `DROP_*` ops for orphans. The paired flag `--no-delete/--delete`
makes the safe default explicit and reversible; `apply` inherits the behavior
via the serialized plan file (deletions are already baked into `out/plan/*.json`
at plan time, so `apply` carries no deletion flag of its own).

| CLI command             | Purpose                                                    |
|-------------------------|------------------------------------------------------------|
| `snow feature init`     | Bootstrap a feature-store project and pull deployed artifacts. |
| `snow feature sync`     | Pull deployed feature-store objects into the local sources tree. |
| `snow feature apply`    | Apply the discovered (or explicit) plan against Snowflake. |
| `snow feature plan`     | Show what would change if the project were applied (read-only). |
| `snow feature list`     | List deployed feature-store objects from Snowflake.        |
| `snow feature describe` | Describe a single feature-store object.                    |
| `snow feature online-service status` | Show the feature store online service runtime status. |
| `snow feature online-service create` | Create and initialize the feature store online service. |
| `snow feature online-service drop` | Destroy the online service and all Online Feature Tables. |
| `snow feature ingest`   | Ingest records into a streaming feature source via the Online Service. |
| `snow feature query`    | Query online features for a feature view via the Online Service. |

Each command maps to a `FeatureManager` method of the same intent. The
manager resolves the project, threads the target's `database` / `schema` /
`role` (and the connection's `warehouse`) into the corresponding `decl_api`
call, and returns a result dict for rendering. Plan files are
warehouse-agnostic; `warehouse` always comes from the active connection.

---

## Manifest parsing (aligned with DCM)

`manifest.yml` is parsed **in this plugin**, not in `snowflake-ml-python`.
The load path is the same as `snow dcm` (`DCMManifest` in
`_plugins/dcm/models.py`):

1. Resolve `--from` to a `SecurePath` (`LocalDirectoryType`, default cwd).
2. `--from` / cwd **must itself contain** `manifest.yml`. `FSProjectPaths.discover`
   delegates the actual lookup to the module-level `find_project_root`, which
   checks only that one directory. Ancestors are **not** searched
   (`DCMManifest.load` parity), so a nested cwd never silently binds to a parent
   project's manifest; a missing file raises `ManifestNotFoundError`.
3. `FSManifest.load(SecurePath(project_root))` opens the file with
   `SecurePath.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB)` and
   `yaml.safe_load`.
4. `from_dict` canonicalizes target and configuration names to UPPER,
   derives `default_target` when there is exactly one target, then
   `validate()`.
5. `get_effective_target(--target)` applies the same lookup as DCM
   (`get_target` → unknown-config check → required-field check).

Malformed files raise `InvalidManifestError`; semantic issues (unknown
target, missing `database` / `schema` / `account_identifier`, forbidden
`warehouse:`, unknown `templating_config`) raise
`ManifestConfigurationError`; a missing file raises
`ManifestNotFoundError`. `FeatureManager._resolve_project` maps those
to `CliError`, matching `_resolve_target_context` in the DCM plugin.

`from_dict` type-checks the manifest shape before touching values:
`targets` must be a mapping of string name to a per-target mapping, and each
target's string fields (`account_identifier` / `database` / `schema` /
`role` / `templating_config`) must be strings (a missing/null field keeps its
default). A wrong YAML type raises `InvalidManifestError` instead of leaking a
raw `AttributeError` / `TypeError` from a later `.upper()`. Likewise
`_resolve_project` wraps `AccountIdentifier.from_string(target.account_identifier)`
so a target whose account_identifier cannot be parsed surfaces as `CliError`,
not an unmapped traceback (this is distinct from `AccountMismatchError`, which
is only raised when a *parsed* account does not match the connection).

The account-match guard is different: a connection whose account differs
from the target's `account_identifier` raises the dedicated
`AccountMismatchError` (a `CliError` subclass in
`_plugins/feature/exceptions.py`, mirroring DCM's
`QueryStatusUnavailableCliError`). This lets `apply` catch **only** the
account case and surface it as a structured `target_mismatch` status (L6),
while a real manifest error (missing file, malformed YAML, unknown
`--target`) propagates with its own message instead of being mislabeled as
an account mismatch. `_resolve_service_target` likewise re-raises
`AccountMismatchError` even when no `--target` was given, so a reachable
project with a mismatched account never silently falls back to managing a
runtime against the connection's account.

The connection fallback in `_resolve_service_target` is opt-in and
fail-closed: it applies only when the caller passes
`allow_connection_fallback=True`, no explicit `--target` was given, and no
manifest is reachable. Only `online-service create` opts in, so a runtime
can be stood up before a project exists (the create flow also polls
`get_status` with the flag while waiting for `RUNNING`). Every other caller
is strict — a bare status read surfaces the manifest error as an
`{status: error}` envelope, which is a terminal failure routed through
`_fail_terminal` (full envelope, non-zero exit), and the destructive
`online-service drop` path raises rather than ever resolving to whatever the
connection happens to point at.

Target **fields** stay feature-store-specific (`account_identifier`,
`database`, `schema`, optional `role` / `templating_config`; type
`feature_store`, `manifest_version: 1`). `database`, `schema`, and
`role` are folded to uppercase on read (unquoted-identifier convention);
`account_identifier` is not. DCM targets use `project_name`
/ `project_owner` and `manifest_version: 2`. Templating is the shared
subset: `defaults` plus `configurations` (names uppercased on read).
`decl_api.load_project` still re-reads the on-disk file for Jinja
variable merge; the CLI parser is the source of truth for target
resolution and the account-match guard.

---

## FeatureManager and the decl library

`FeatureManager` extends `SqlExecutionMixin` and delegates all feature-store
logic to `decl_api`. It imports the library at module level behind a single
try/except so the plugin still imports (and registers) when the library is
absent. Manifest types are always available from this plugin:

The CLI owns the Snowpark `Session`. `_build_session()` returns the one Session
cached on `SqlExecutionMixin.snowpark_session` (built lazily over the active
connection, `self._conn`) rather than constructing a fresh Session per call, so
a single command — `plan` alone touches it via the init-first guard plus each
state-fetch facade (entities / feature views / stream sources / feature groups)
— reuses one Session instead of leaking several. It is deliberately **not**
closed by the manager: the Session wraps `self._conn`, which the process owns
for the command lifetime and which the manager also uses for `execute_query`
(`SHOW` / `DESCRIBE`); closing it would break the rest of the command. This
mirrors `SqlExecutionMixin` and the stage plugin, which cache and never close.

```python
from snowflake.cli._plugins.feature.models import FSManifest, FSTarget, FSProjectPaths

try:
    from snowflake.ml.feature_store.decl import api as decl_api
    _SNOWML_IMPORT_ERROR = None
except ImportError as _exc:
    decl_api = None
    _SNOWML_IMPORT_ERROR = _exc

def snowml_import_error():
    return _SNOWML_IMPORT_ERROR
```

`manager.py` owns the **single** library-availability signal: `decl_api` is
what every command path dereferences, and `snowml_import_error()` is `None`
iff that import succeeded. `commands._require_snowflake_ml` guards off this one
helper (not a separate probe of `decl.errors`), so a partial install — where
the heavier `decl.api` (executor / types / enums) fails while `decl.errors`
still imports — cannot pass preflight and then raise
`AttributeError: 'NoneType' object has no attribute 'load_project'`. The
init-required exception is likewise resolved lazily off
`manager.decl_api.FeatureStoreNotInitializedError` (re-exported from
`decl.api`), so `commands.py` imports the feature-store library through exactly
one entry point.

Before running any command the `feature_store_preflight` decorator raises an
actionable `CliError` when the library is missing, so a command never
reaches a `None` `decl_api`.

Two CLI-side responsibilities are worth calling out:

- **Project resolution.** Same sequence as DCM: `--from` / cwd must contain
  `manifest.yml` (ancestors are not searched), `FSManifest.load` +
  `get_effective_target`, then assert the target's `account_identifier`
  matches the active connection before any state query runs. On mismatch the
  command stops and asks the operator to pick a different connection or fix
  the manifest.
- **Local spec lookup for `describe` examples.** When `describe` enriches
  its output with ingest/query examples it reads the authored YAML from the
  resolved project only: `FSProjectPaths.feature_views_dir`
  (`<project_root>/sources/feature_views`) for the FeatureView spec and
  `FSProjectPaths.datasources_dir` (`.../sources/datasources`) for source
  columns. It never globs cwd-relative roots (no `.`, `feature_views/`,
  `specs/`, or `example_store/` fallbacks), so a coincidental sample tree in
  the operator's cwd cannot shadow the real project spec. When the OFT-
  resolved version is known, a spec whose `version` also matches wins;
  otherwise the first name match is used. Missing source `columns` are
  filled from the matching datasource onto a **deep copy** of the parsed
  FeatureView spec (`copy.deepcopy`), so the dict `_find_spec` returned is
  never mutated in place — a future memoized `_find_spec` cannot leak the
  injected columns into a later `describe`. The ingest/query example endpoints
  come from `get_status` against the **same** resolved `from_dir` /
  `target_name` as the OFT `DESCRIBE`, so `describe --from ../proj` reports
  URLs for that project's target rather than the process cwd or the
  connection fallback. Lookup stays best-effort: a missing/unreadable spec
  leaves examples unenriched rather than failing `describe`, but an
  unreadable/broken YAML is logged at `warning` (with its path) instead of
  being silently skipped.
- **`init` write order.** On a fresh init the resolved `database` / `schema`
  / `account_identifier` (from `--database` / `--schema` or the connection
  defaults) must be non-empty; a stripped-empty value raises `CliError`
  before anything is written, so `init` never persists a blank target such as
  `database: ""` (which would strand the project — `manifest_existed` then
  short-circuits the fresh-init branch on every retry). The order is scaffold
  `sources/` + `out/plan/` → bootstrap the runtime (`CREATE_IF_NOT_EXIST`) →
  write `manifest.yml` only when absent → export. The manifest is written only
  after the bootstrap succeeds, so a failed init leaves no partial project
  state. Re-init never rewrites an existing manifest.
- **Plan-file lifecycle.** `plan` validates the project and writes a plan file
  under `<project_root>/out/plan/`; `apply` is a pure consumer that reads the
  latest (or `--plan`) file and hands it to the library for execution. The plan
  file is renamed `.applied` on success and left in place on failure so it can
  be retried. `plan` generates **once**: `FeatureManager.plan()` returns
  `(envelope, plan)` and the command serializes that same `Plan` via
  `FeatureManager.write_plan_object()` (serialize-only), so the displayed ops
  and the persisted JSON come from one fetch — no double `DESCRIBE`, and no
  window where applied state could drift between what is shown and what `apply`
  will run. `FeatureManager.write_plan()` stays as a generate-then-serialize
  convenience for callers (tests/scripts) that do not already hold a `Plan`.
- **Online-service status banner.** `get_status` builds the rich TABLE banner
  itself: on a parsed status it calls `decl_api.format_status_display(...)`
  (threading the connection `user`, resolved `database` / `schema`, and the
  framework `--verbose` flag from the CLI context) and stashes the rendered
  string on `result["_display"]`. This is the same adapter shape `describe`
  uses (`_display` on the manager result); `commands.py` only routes the
  banner (`_write_display(result.pop("_display"))`) and never dereferences
  `decl_api` to format status. The formatter inputs are not leaked as payload
  keys, and an `error` envelope carries no `_display`. `commands.py` still
  pops the banner before returning a status dict to structured stdout (the
  `create` poll result included) so the free-form text never rides JSON/CSV.

All state semantics — what SQL to run, how applied state is diffed, and how
plans are generated and executed — live in the library, not the CLI.

### Applied-state inventory reads (fail loud)

The four imperative inventory facades the manager threads into
`decl_api.fetch_applied_state` — `list_entities` (`_fetch_entity_rows`),
`list_feature_views` (`_fetch_feature_view_rows`), `list_feature_groups`
(`_fetch_feature_group_rows`), and `list_stream_sources`
(`_fetch_stream_source_rows`) — are the **authoritative** applied-state
inventory. An empty result is read by the planner as "nothing deployed" and
turned into `CREATE_*` ops; `plan` persists those ops once and `apply` never
re-plans. A read that fails past the library's retry must therefore **not**
degrade to `[]` (which would confidently recreate objects that already
exist). All four route through the shared `_call_inventory_facade`, which:

- re-raises `FeatureStoreNotInitializedError` unchanged (the command layer
  rewraps it into the actionable `snow feature init` message), and
- rewraps any other post-retry failure as a `CliError` naming the object
  class (`registered entities` / `feature views` / `feature groups` /
  `stream sources`), the target `database.schema`, and the sanitized backend
  text — never reporting "unknown" as "none".

This is distinct from the recovery-*aid* reads (`_fetch_oft_state`'s per-OFT
`DESCRIBE`, `_fetch_dt_text_map`'s `SHOW DYNAMIC TABLES`), which are allowed
to skip / return empty because they only enrich rows the authoritative
facades already surfaced.

The decl `describe_specification_template` already wraps its `{name}` slot in
double quotes, so `_fetch_oft_state` fills it with the bare OFT name and
escapes any embedded `"` (doubled) — it does **not** wrap the name with
`to_identifier` / `to_quoted_identifier`, which would emit a second pair of
quotes into the already-quoted slot.

---

## Output formatting (`--format`)

Commands return a `CommandResult` and let the Snow CLI output layer render it
for the active `--format`. The plugin never JSON-encodes payloads itself; it
picks the right `CommandResult` subtype:

| Return type | TABLE render | `--format json` / `csv` |
|---|---|---|
| `CollectionResult([dict, ...])` | multi-column table (one column per key) | JSON array of objects |
| `ObjectResult(dict)` | two-column `key` / `value` table | nested JSON object |
| `MessageResult(text)` | raw text | `{"message": "<text>"}` |
| `EmptyResult()` | nothing | nothing |

`_is_structured_output()` returns `True` for JSON/CSV. If the CLI context
cannot be resolved, it also returns `True` (and logs the exception at DEBUG)
so a machine consumer never receives TABLE text on stdout. Commands that
print a human banner or header stay **format-aware**: the free-form text is
written to stderr in TABLE mode only, so structured stdout stays a clean,
parseable payload. For example, `online-service status` and `describe` write
their rich display to stderr and return `EmptyResult()` in TABLE mode, but
suppress the banner and return the structured object in JSON/CSV mode.

The rich multi-line banners themselves are library helpers
(`decl_api.format_status_display` / `format_describe_display`), but they are
invoked **only from `FeatureManager`**, which hands the rendered string back
on `result["_display"]`; `commands.py` never calls them. The CLI still owns
`--format`, the stderr-vs-stdout split, and the `CommandResult` choice. This
is distinct from `decl_api.format_op_display_row`, which returns a
JSON-serializable ordered **dict** (one plan/apply row) rather than a rendered
banner — a shared column/`type`-derivation facade `plan`, `apply`, and `list`
all consume, not a TABLE renderer.

### Mutually exclusive flags

Flags that cannot be combined (`init` / `sync` `--python` vs `--yaml`) are
rejected with `CliArgumentError`, not `typer.BadParameter`. It is a bad-flag
error, so it uses the `Cli*` exception family and a script sees one exit code:
`1` by default and `2` with `--enhanced-exit-codes` (the `BaseCliError`
contract in `api/exceptions.py`). `typer.BadParameter` is a Click
`UsageError` (always exit `2`), which diverged from the `--python` /
`--yaml` check; `CliArgumentError` standardizes them. Malformed option
*values* (invalid JSON for `ingest --data` / `query --keys`) stay
`typer.BadParameter` — those are value-parse errors, not mutually exclusive
flags.

### Terminal-failure statuses (non-zero exit)

A result envelope whose `status` is a **terminal failure** —
`target_mismatch`, `partial_failure`, `refused`, `validation_failed`, a
backend `error` (`list` SQL failure / `describe` not-found /
`online-service status` failure — unreachable manifest, empty
system-function response, or query error — and the `online-service create`
CREATE-request failure), an `online-service create` wait outcome
(`timeout`, `FAILED`, `SUSPENDED`, `ERROR`), or an `online-service drop`
teardown failure (`partial_failure` /
`failed`) — must both carry its populated `errors` (or singular `error`) array
to the caller **and** exit non-zero, the way `snow dcm` does.
`_is_terminal_failure()` classifies the status; the command then:

- **JSON/CSV:** prints the full envelope (`_to_object(result)` — `status` +
  `errors`/`error`, plus `ops` for `apply`/`plan`) to stdout via
  `_fail_terminal()`, then raises `CliError`. The structured payload is the
  machine surface; the `CliError` summary rides on stderr.
- **TABLE:** keeps the existing human rendering (status header + readable
  `_print_diagnostics` findings, and the ops table for `apply`), then raises
  the same `CliError`.

`CliError` is a `ClickException`, so on its own it prints only `Error: <msg>`
with no structured object — hence the plugin prints the envelope **before**
raising so JSON/CSV callers still get `status`/`errors`. The manager keeps
returning these as structured status dicts (never raising) so `apply`'s L6
target-mismatch stays script-branchable; the `CliError` mapping lives only in
`commands.py`.

### Terminal-output sanitization

Server- and config-sourced strings (object names, statuses, error text) may
carry ANSI / control sequences, so per `docs/contributing/conventions.md` they
are stripped with `sanitize_for_terminal` before printing. To avoid sprinkling
the call at every command (cf. DCM's `_check_account_identifier`), the plugin
sanitizes once at the shared aggregators: the stderr printers
(`_print_diagnostics`/`_write_finding_block`, `_print_warnings`, the status /
target / listing-scope headers, `_write_display`, the wait-progress handle) via
the `_term` helper, the `CliError` summary built by `_envelope_error_messages` /
`_terminal_failure_error`, and `_safe_value` (so ops/list tables are stripped on
`printing.py`'s multi-row path, which does not sanitize). Manager `CliError`
messages that interpolate identifiers (account mismatch, `--database` /
`--schema` conflict, describe-not-found, entity-fetch failure) sanitize those
dynamic pieces the same way.

`no_plan` is deliberately **not** terminal: it is an idle success (exit 0). It
still returns the full envelope in structured mode (so `status`/`errors` reach
the caller) instead of collapsing to `{"message": "Operations: 0"}`. The same
empty-ops-returns-envelope rule applies to a successful `apply`/`plan` with no
actionable ops; a success **with** ops still renders the ops array/table.

### `online-service create` startup wait

`online-service create` sends the CREATE command (the manager's `initialize_service`
returns immediately with `CREATING`). If that CREATE request instead fails
(the manager returns an `error` envelope: bad role, missing privileges,
warehouse unavailable), the command short-circuits through `_fail_terminal`
before the poll loop begins, so the wait outcomes below are only reached once
`CREATING` comes back. On `CREATING` it then polls `get_status` on a fixed
cadence (`_ONLINE_SERVICE_POLL_INTERVAL_S` = 5s) up to a deadline
(`_ONLINE_SERVICE_TIMEOUT_S` = 600s) via `_wait_online_service_running`. The
wait ends on any **terminal** state, not just success:

- `RUNNING` -> success (exit 0).
- `FAILED` / `SUSPENDED` / uppercase `ERROR` (the runtime's own terminal
  tokens, forwarded verbatim by `parse_service_status`) -> return that status
  immediately so a create that fails at t+20s reports the real state instead
  of burning the full 600s down to a generic timeout.
- Repeated poll failures -> a raised `get_status` **or** a manager
  query-failure envelope (lowercase `status: error`) is logged at `DEBUG` and
  counted toward `_SERVICE_WAIT_MAX_CONSECUTIVE_FAILURES` (5). A single blip
  that then resolves to `RUNNING` still succeeds; only a sustained run aborts,
  as `error`. In-flight statuses (`PENDING` / `CREATING` / `UPDATING` /
  `NOT_FOUND` / unknown) keep polling and reset the streak.
- Deadline elapsed while still in-flight -> `timeout`.

Every non-`RUNNING` outcome is routed through `_fail_terminal` (see
Terminal-failure statuses above), so it emits the full envelope and exits
non-zero. The wait bar is the transient Rich handle from
`_service_wait_progress` (stderr, suppressed under `--silent` / structured
output).

### `online-service drop` confirmation gate

`online-service drop` is destructive: it tears down every Online Feature Table
and the online-service runtime with no undo. It is gated the same way as
`snow dcm purge` (`InteractiveOption` / `ForceOption`):

- Non-interactive (a pipe / CI, or `--no-interactive`) without `--force`
  refuses outright (`CliError`: "Cannot drop the online service
  non-interactively without --force."), before `destroy_service` runs.
- Interactive (the default off a terminal) without `--force` requires a typed
  `drop <DATABASE.SCHEMA>` confirmation of the resolved target
  (`_confirm_drop`); `cancel` aborts.
- `--force` skips the prompt for scripts / CI.

The location shown in the prompt is the same `DATABASE.SCHEMA` that
`destroy_service` will use (`_resolve_service_target`, on its strict default
with no connection fallback). `online-service drop` therefore refuses outright
when no manifest is reachable — the pre-prompt resolution raises without `--force`,
and `destroy_service` raises with `--force` — rather than dropping whatever
the connection happens to point at. To tear down an orphan runtime whose
project directory is gone, recreate a minimal `manifest.yml` and pass
`--from` (or drop the objects via raw SQL).

### `online-service drop` teardown status

Once the gate passes, `online-service drop` calls `destroy_service`, which drops every
Online Feature Table then the runtime, collecting `dropped_ofts` / `errors`.
The manager sets `status` from that outcome rather than reporting `destroyed`
unconditionally: `destroyed` (exit 0) when nothing errored, `partial_failure`
when at least one OFT was dropped but something errored, and `failed` when
errors occurred and nothing was dropped. `partial_failure` / `failed` are
terminal failures, so the command routes them through `_fail_terminal` (full
envelope on structured stdout, non-zero exit); `destroyed` returns the
envelope with exit 0.

### State-fetch progress bar

`plan` and `list` fetch deployed state before rendering, which can be slow.
Because `plan` generates once (see Plan-file lifecycle above), the state-fetch
progress bar is drawn a single time per `snow feature plan` — serializing the
returned `Plan` via `write_plan_object` issues no further state SQL.
The manager renders a transient Rich progress bar bound to
`Console(file=sys.stderr)` (never stdout), gated on
`get_cli_context().silent` (True for `--format json`/`csv` and `--silent`).
The bar is driven by the optional `on_progress` callback the CLI passes into
the library's fetch facades — the library only invokes the callback and never
renders anything itself. Rich (`rich==14.0.0`) is already a CLI dependency.

---

## Parameter naming constraints

The Snow CLI global options inject reserved parameter names into every
`requires_connection=True` command. Plugin commands **must not** define
parameters with these names:

- **GLOBAL_CONNECTION_OPTIONS**: `connection`, `host`, `port`, `account`,
  `user`, `password`, `authenticator`, `private_key_file`, `session_token`,
  `token`, `database`, `schema`, `role`, `warehouse`, `temporary_connection`,
  `mfa_passcode`, and others.
- **GLOBAL_OPTIONS**: `format`, `verbose`, `debug`, `silent`,
  `enhanced_exit_codes`.

`--database` / `--schema` reach a command through these global flags rather
than as custom parameters.

---

## How to add a new command

1. Add a method to `FeatureManager` in `manager.py` that resolves the project
   and delegates the work to `decl_api`.
2. Add a `@app.command(requires_connection=True)` function in `commands.py`,
   decorated with `feature_store_preflight`. Avoid parameter names reserved by
   the global options (see above).
3. Add failing tests in `tests/feature/test_commands.py` and
   `tests/feature/test_manager.py` first (TDD).
4. Run `hatch run test tests/feature/` to confirm the red/green cycle.
