# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FeatureManager — thin CLI adapter delegating all logic to decl_api."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional, Sequence, Tuple

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor

try:
    from snowflake.ml.feature_store.decl import api as decl_api

    _HAS_DECL_API = True
except ImportError:
    decl_api = None  # type: ignore[assignment]
    _HAS_DECL_API = False

log = logging.getLogger(__name__)


def _rows_to_dicts(rows) -> list[dict[str, Any]]:
    """Convert DictCursor rows to plain dicts for JSON serialization."""
    return [dict(r) for r in rows]


def _to_positional_keys(
    keys: list[dict[str, Any]],
    join_key_order: list[str],
) -> list[list[Any]]:
    """Translate the CLI's dict-shaped key payload to snowml-core's
    positional list-of-list shape.

    snowml-core's ``FeatureStore.read_feature_view`` accepts ``keys``
    as a list of value-lists, one per row, with positional ordering
    matching the FeatureView's declared join-key sequence
    (``[jk for ent in fv.entities for jk in ent.join_keys]``).  The
    CLI accepts JSON-friendly dicts for human ergonomics, e.g.
    ``[{"USER_ID": "u1", "SESSION_ID": "s1"}]``.  This helper is the
    single place that bridges the two surfaces.

    Validation is strict: every input dict must carry every declared
    join-key column.  A missing column raises a ``ValueError``
    naming the column and the row index, so users get a precise
    diagnostic instead of a "wrong tuple length" error from the
    Online Service Query API.

    Args:
        keys: List of dicts as supplied by the CLI's ``--keys`` arg.
        join_key_order: Flat list of join-key column names in the
            order declared by the FeatureView's entities.

    Returns:
        List of positional value-lists ready to forward to
        ``read_feature_view(keys=...)``.

    Raises:
        ValueError: If any input dict is missing a join-key column.
    """
    positional: list[list[Any]] = []
    for i, row in enumerate(keys):
        try:
            positional.append([row[col] for col in join_key_order])
        except KeyError as exc:
            missing = exc.args[0]
            raise ValueError(
                f"Missing join key {missing!r} in keys[{i}]; "
                f"feature view declares join keys {join_key_order!r}"
            ) from exc
    return positional


class FeatureManager(SqlExecutionMixin):
    """Thin CLI adapter — delegates all business logic to decl_api."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._session_setup_done: bool = False

    def _ensure_session_setup(self) -> None:
        """Run session priming exactly once per FeatureManager instance.

        Delegates to :func:`decl_api.ensure_session_setup`, which owns the
        priming SQL and the strict-failure policy.  The CLI never builds
        SQL of its own here — it only supplies its bound
        :meth:`execute_query` callable.

        Raises:
            SessionSetupError: If the priming SQL cannot be executed. Propagates
                so the CLI command aborts before any state SQL runs.
        """
        if self._session_setup_done:
            return
        decl_api.ensure_session_setup(self.execute_query)
        self._session_setup_done = True

    def _target_info(self) -> dict[str, str]:
        """Return connection target db/schema/warehouse for result dicts."""
        ctx = get_cli_context()
        return {
            "target_database": ctx.connection.database or "",
            "target_schema": ctx.connection.schema or "",
            "target_warehouse": ctx.connection.warehouse or "",
        }

    # ------------------------------------------------------------------
    # init
    # ------------------------------------------------------------------

    def init(self, no_scaffold: bool = False) -> dict[str, Any]:
        """Initialize a feature store: create schema, tags, metadata tables.

        If *no_scaffold* is False, also creates local project directories
        ``entities/``, ``datasources/``, and ``feature_views/`` in the
        current working directory.
        """
        ctx = get_cli_context()
        db = ctx.connection.database
        schema = ctx.connection.schema
        wh = ctx.connection.warehouse or ""

        session = self._build_session()
        # Lazy import to avoid heavy deps at module level
        from snowflake.ml.feature_store.feature_store import (
            CreationMode,
            FeatureStore,
        )

        FeatureStore(
            session,
            db,
            schema,
            wh,
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )

        dirs_created: list[str] = []
        if not no_scaffold:
            for d in ["entities", "datasources", "feature_views"]:
                os.makedirs(d, exist_ok=True)
                dirs_created.append(d)

        return {
            "status": "initialized",
            "database": db,
            "schema": schema,
            "directories": dirs_created,
        }

    # ------------------------------------------------------------------
    # apply
    # ------------------------------------------------------------------

    def apply(
        self,
        input_files: Sequence[str],
        config: Optional[dict[str, Any]],
        dev_mode: bool,
        overwrite: bool,
        allow_recreate: bool,
        plan_file: Optional[str] = None,
        no_delete: bool = True,
    ) -> dict[str, Any]:
        """Apply a feature-store spec set to Snowflake.

        Apply is a *pure plan-file consumer*: either ``plan_file`` is
        given explicitly (the L7 escape hatch), or the manager
        auto-discovers the latest unapplied plan under
        ``<cwd>/.snowflake/plans/`` (L1–L4 invariants).  There is no
        "re-plan from source" branch — that path was the proximate
        cause of the phantom-``CREATE_*`` failure mode and was deleted
        in the "Apply Lifecycle Resilience" plan.

        Operators preview changes via ``snow feature plan`` (which
        runs ``manager.plan`` + ``manager.write_plan`` to validate
        specs against applied state and persist the resulting plan
        file to ``<cwd>/.snowflake/plans/`` for ``apply`` to consume).

        Args:
            input_files: Spec file paths or glob patterns.
            config: Jinja2 template variables, or ``None``.
            dev_mode: Apply dev-mode relaxed validation.
            overwrite: Force CREATE OR REPLACE semantics.
            allow_recreate: Permit recreate (drop-and-create) operations.
            plan_file: Optional explicit plan file to apply (L7).  When
                given, skips auto-discovery and target-mismatch checks
                still apply.
            no_delete: When True, deletion detection is disabled.  Only
                used when generating a plan from source (``manager.plan``);
                wet-run apply consumes a pre-generated plan file whose
                deletion ops are already baked in.

        Returns:
            A result dict with ``status`` / ``ops`` / ``executed`` /
            ``warnings`` / ``errors`` plus connection-target metadata.
        """
        self._ensure_session_setup()

        if plan_file is not None:
            return self._apply_from_plan_file(
                plan_file=plan_file,
                dev_mode=dev_mode,
                overwrite=overwrite,
                allow_recreate=allow_recreate,
            )

        # No explicit plan_file: discover the latest unapplied plan
        # under ``<cwd>/.snowflake/plans/`` (L1–L3).  If none exists,
        # return a structured ``no_plan`` result that points the
        # operator at ``snow feature plan`` — we deliberately do NOT
        # silently re-plan from source, because that was the parity-bug
        # surface the apply-lifecycle plan eliminated.
        discovered = self._discover_unapplied_plan()
        if discovered is None:
            return {
                **self._target_info(),
                "status": "no_plan",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    "No unapplied plan file found under "
                    "'.snowflake/plans/'. Run `snow feature plan <path>` "
                    "first to generate a plan, then re-run apply."
                ],
            }
        return self._apply_from_plan_file(
            plan_file=discovered,
            dev_mode=dev_mode,
            overwrite=overwrite,
            allow_recreate=allow_recreate,
        )

    # ------------------------------------------------------------------
    # _discover_unapplied_plan (private helper, L1–L3 invariants)
    # ------------------------------------------------------------------

    def _discover_unapplied_plan(self) -> Optional[str]:
        """Return the path of the newest unapplied plan, or ``None``.

        Mirrors the default ``--out`` location used by
        ``commands.py::plan_cmd`` (``<cwd>/.snowflake/plans/``).
        Identifies unapplied plans as files matching
        ``feature_plan_*.json`` whose names end in ``.json`` (i.e. NOT
        ``.applied`` or ``.discarded``).  Sorts lexicographically — the
        ``YYYYMMDDTHHMMSS`` UTC timestamp embedded in the filename is
        monotonic at one-second resolution, which is far below human
        workflow latency.

        Side effect (L3 — Discard-Older): when more than one unapplied
        plan exists, every plan except the newest is renamed to
        ``<name>.discarded`` *before* the function returns.  This keeps
        the plans directory in a normalised state — at most one
        unapplied plan when execution begins — and matches the
        operator-visible contract that ``apply`` commits to consume
        only the latest plan.

        Returns:
            Path string of the newest unapplied plan, or ``None`` if
            no candidates exist.
        """
        from pathlib import Path

        plans_dir = Path.cwd() / ".snowflake" / "plans"
        if not plans_dir.is_dir():
            return None

        candidates = sorted(
            p
            for p in plans_dir.glob("feature_plan_*.json")
            if p.is_file() and p.suffix == ".json"
        )
        if not candidates:
            return None

        newest = candidates[-1]
        for older in candidates[:-1]:
            older.rename(older.parent / (older.name + ".discarded"))
        return str(newest)

    # ------------------------------------------------------------------
    # _apply_from_plan_file (private helper)
    # ------------------------------------------------------------------

    def _apply_from_plan_file(
        self,
        plan_file: str,
        dev_mode: bool,
        overwrite: bool,
        allow_recreate: bool,
    ) -> dict[str, Any]:
        """Execute a pre-computed plan loaded from a JSON plan file.

        Implements L4–L6 of the apply-lifecycle contract:

        - **L4 (Mark-Applied):** on successful execution, rename the
          plan file to ``<name>.applied``.
        - **L5 (Mark-Failed-Stays-Unapplied):** on execution failure,
          leave the plan file at its original name so the operator can
          inspect, fix, and retry.
        - **L6 (Target-Match):** before executing, verify the plan's
          ``target_database`` / ``target_schema`` match the active
          connection.  Mismatch returns a structured
          ``status="target_mismatch"`` and skips execution.

        Also fixes Bug C (warehouse propagation): the *connection's*
        warehouse is forwarded to ``execute_plan`` (plan files are
        warehouse-agnostic by design — connection context owns
        warehouse selection so the same plan can run from any
        compatible warehouse).

        Args:
            plan_file: Path to the plan JSON file to apply.
            dev_mode: Apply dev-mode relaxed validation.
            overwrite: Force CREATE OR REPLACE semantics.
            allow_recreate: Permit recreate (drop-and-create) operations.

        Returns:
            A result dict with ``status`` / ``ops`` / ``executed`` /
            ``warnings`` / ``errors`` / ``plan_file`` (the latter is
            updated to the ``.applied`` path on success).
        """
        from pathlib import Path

        from snowflake.ml.feature_store.decl.types import PlanOptions

        plan_path = Path(plan_file)
        json_str = plan_path.read_text()
        pf = decl_api.deserialize_plan(json_str)
        plan = pf.plan
        plan_target_db = pf.target_database
        plan_target_schema = pf.target_schema

        ctx = get_cli_context()
        conn_db = ctx.connection.database or ""
        conn_schema = ctx.connection.schema or ""

        # L6 (Target-Match): refuse to apply a plan generated for a
        # different store than the one the active connection points at.
        if plan_target_db and plan_target_db != conn_db:
            return {
                **self._target_info(),
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"Plan was generated for database "
                    f"'{plan_target_db}' but the active connection "
                    f"points at '{conn_db}'. Re-run plan or switch "
                    f"connection."
                ],
                "plan_file": plan_file,
            }
        if plan_target_schema and plan_target_schema != conn_schema:
            return {
                **self._target_info(),
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"Plan was generated for schema "
                    f"'{plan_target_schema}' but the active "
                    f"connection points at '{conn_schema}'. Re-run "
                    f"plan or switch connection."
                ],
                "plan_file": plan_file,
            }

        options = PlanOptions(
            dev_mode=dev_mode,
            overwrite=overwrite,
            allow_recreate=allow_recreate,
        )

        session = self._build_session()
        # Bug C fix: warehouse comes from the *active connection*, not
        # the plan file (plan files are warehouse-agnostic by design).
        result = decl_api.execute_plan(
            plan,
            session,
            database=plan_target_db,
            schema=plan_target_schema,
            warehouse=ctx.connection.warehouse or "",
            options=options,
        )

        # L4 (Mark-Applied): rename plan file to .applied on success.
        # L5 (Mark-Failed-Stays-Unapplied): keep the original name on
        # failure (any non-"applied" status, or an exception that would
        # have already propagated above this line — Python's natural
        # exception flow is the L5 mechanism).
        result_plan_file = plan_file
        if result.status == "applied":
            applied_path = plan_path.parent / (plan_path.name + ".applied")
            plan_path.rename(applied_path)
            result_plan_file = str(applied_path)

        return {
            "status": result.status,
            "ops": result.ops,
            "executed": len([o for o in result.ops if o.get("status") == "success"]),
            "warnings": result.warnings,
            "errors": result.errors,
            "plan_file": result_plan_file,
        }

    # ------------------------------------------------------------------
    # write_plan
    # ------------------------------------------------------------------

    def write_plan(
        self,
        input_files: Sequence[str],
        config: Optional[dict[str, Any]],
        dev_mode: bool,
        out_path: str,
        no_delete: bool = True,
    ) -> str:
        """Generate a plan and write it as JSON to *out_path*.

        Args:
            input_files: Spec file paths or glob patterns.
            config: Jinja2 template variables, or ``None``.
            dev_mode: Apply dev-mode relaxed validation.
            out_path: Destination path for the JSON plan file.  Parent
                directories are created automatically.
            no_delete: When True, disable deletion detection.

        Returns:
            The absolute path to the written plan file.
        """
        from pathlib import Path

        from snowflake.ml.feature_store.decl.types import PlanOptions

        self._ensure_session_setup()

        ctx = get_cli_context()

        # Fetch applied state (full spec JSON via DESCRIBE TYPE = SPECIFICATION).
        sqls = decl_api.state_queries(ctx.connection.database, ctx.connection.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        specification_map = self._fetch_oft_state(raw_show, sqls)
        entity_rows = self._fetch_entity_rows(ctx)
        applied_state = decl_api.fetch_applied_state(
            raw_show,
            raw_tables,
            specification_map=specification_map,
            entity_rows=entity_rows,
            default_database=ctx.connection.database or "",
            default_schema=ctx.connection.schema or "",
        )

        # Load and expand specs
        all_files = self._expand_with_datasources(list(input_files))
        batch = decl_api.load_specs(all_files, config)

        options = PlanOptions(dev_mode=dev_mode, full_directory_mode=not no_delete)
        # Forward connection context so the planner qualifies bare specs
        # (single-file invocations, entity YAMLs without ``database:``)
        # against the active ``snow`` connection.  Skipping this is the
        # ``write_plan`` ⇄ ``apply(dry_run=True)`` parity bug: the
        # apply path qualifies via ``generate_apply_sql`` while
        # ``write_plan`` would otherwise hand unqualified spec keys to
        # the planner — so an unchanged repo would render as
        # ``NO_CHANGE`` in the terminal yet emit a wave of phantom
        # ``CREATE_*`` / ``DROP_*`` ops in the on-disk plan file.
        plan = decl_api.generate_plan(
            batch,
            applied_state,
            options,
            database=ctx.connection.database or "",
            schema=ctx.connection.schema or "",
        )

        json_str = decl_api.serialize_plan(
            plan,
            ctx.connection.database,
            ctx.connection.schema,
            all_files,
        )

        dest = Path(out_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json_str)
        return str(dest)

    # ------------------------------------------------------------------
    # list_specs
    # ------------------------------------------------------------------

    def list_specs(
        self,
        input_files: Tuple[str, ...],
        config: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """List specs from files or deployed objects from Snowflake.

        When listing from Snowflake, this surfaces three object kinds in a
        single result:

        - ``FeatureView`` rows from ``SHOW ONLINE FEATURE TABLES``, enriched
          with the full spec JSON returned by
          ``DESCRIBE ONLINE FEATURE TABLE <name> TYPE = SPECIFICATION``.
        - ``Entity`` rows from ``SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'``.
        - ``Datasource`` rows derived by unioning ``spec.sources[]`` across
          every recovered FV spec (datasources are virtual — they have no
          dedicated SHOW command).
        """
        if input_files:
            batch = decl_api.load_specs(list(input_files), config)
            specs = getattr(batch, "specs", [])
            return {"source": "files", "specs": [str(s) for s in specs]}

        # Snowflake-bound branch: prime the session before any state query.
        # SessionSetupError is intentionally raised outside the try/except
        # below so the command aborts cleanly.
        self._ensure_session_setup()

        ctx = get_cli_context()
        try:
            queries = decl_api.list_state_queries(
                ctx.connection.database, ctx.connection.schema
            )
            oft_rows = _rows_to_dicts(
                self.execute_query(queries["show_ofts"], cursor_class=DictCursor)
            )

            entity_rows = self._fetch_entity_rows(ctx)

            specification_map = self._fetch_oft_state(oft_rows, queries)

            enriched = decl_api.enrich_list_results(
                oft_rows,
                entity_rows=entity_rows,
                specification_map=specification_map,
            )
            return {**self._target_info(), "source": "snowflake", "specs": enriched}
        except Exception as exc:
            log.warning("list query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(self, name: str) -> dict[str, Any]:
        """Return metadata for a named feature view (resolves to OFT name)."""
        self._ensure_session_setup()

        ctx = get_cli_context()

        # Resolve feature view name → OFT name via SHOW lookup
        sqls = decl_api.state_queries(ctx.connection.database, ctx.connection.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )

        from snowflake.ml.feature_store.decl.state import _parse_oft_name

        oft_name = None
        for row in raw_show:
            candidate = row.get("name", "")
            base_name, _ = _parse_oft_name(candidate)
            if base_name.upper() == name.upper():
                oft_name = candidate
                break
            # Also allow passing the full OFT name directly
            if candidate.upper() == name.upper():
                oft_name = candidate
                break

        if not oft_name:
            return {
                "status": "error",
                "name": name,
                "error": f"{name}: not found in deployed feature views",
            }

        # Find the SHOW row for metadata
        show_row = None
        for row in raw_show:
            if row.get("name", "") == oft_name:
                show_row = row
                break

        try:
            sql = decl_api.describe_query(
                oft_name, ctx.connection.database, ctx.connection.schema
            )
            rows = list(self.execute_query(sql, cursor_class=DictCursor))
            desc_rows = _rows_to_dicts(rows)
        except Exception as exc:
            log.warning("describe raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "name": name, "error": str(exc)}

        # Parse feature view name and entities from the OFT name / DESCRIBE
        fv_name, version = _parse_oft_name(oft_name)

        # Extract entity (primary key) columns
        pk_cols = []
        for col in desc_rows:
            is_pk = False
            for pk_key in ("primary key", "PRIMARY KEY", "primary_key"):
                val = col.get(pk_key, "")
                if val and str(val).upper() in ("Y", "YES", "TRUE", "1"):
                    is_pk = True
                    break
            if is_pk:
                pk_cols.append(col.get("name", col.get("NAME", "")))

        # Build example curl commands if service is running
        examples: list[str] = []
        try:
            status = self.get_status()
            ingest_url = decl_api.get_service_endpoint(status, "ingest")
            query_url = decl_api.get_service_endpoint(status, "query")

            # Try to find local spec for accurate column info
            spec = self._find_spec(fv_name)
            source_name = fv_name  # default fallback
            if spec:
                sources = spec.get("sources", [])
                if sources and isinstance(sources, list) and sources[0].get("name"):
                    source_name = sources[0]["name"]
                    # If source has no columns, look for a datasource YAML
                    if not sources[0].get("columns"):
                        ds_spec = self._find_datasource(source_name)
                        if ds_spec and ds_spec.get("columns"):
                            spec["sources"][0]["columns"] = ds_spec["columns"]

            examples = decl_api.build_describe_examples(
                fv_name.lower(),
                version.lower(),
                source_name,
                desc_rows,
                ingest_url,
                query_url,
                spec=spec,
            )
        except Exception as exc:
            log.debug("Could not fetch service endpoints for examples: %s", exc)

        result: dict[str, Any] = {
            "name": name,
            "feature_view": fv_name.lower(),
            "version": version.lower(),
            "database": ctx.connection.database,
            "schema": ctx.connection.schema,
            "oft_name": oft_name,
            "entities": pk_cols,
            "rows": desc_rows,
        }
        if examples:
            result["examples"] = examples

        # Build rich formatted display
        result["_display"] = decl_api.format_describe_display(
            fv_name=fv_name.lower(),
            version=version.lower(),
            database=ctx.connection.database or "",
            schema=ctx.connection.schema or "",
            oft_name=oft_name,
            entities=pk_cols,
            describe_rows=desc_rows,
            show_row=show_row,
            spec=spec,
            examples=examples,
        )

        return result

    # ------------------------------------------------------------------
    # _find_source_name (helper for describe examples)
    # ------------------------------------------------------------------

    @staticmethod
    def _find_spec(fv_name: str) -> Optional[dict[str, Any]]:
        """Try to find a local YAML spec for a feature view by name.

        Searches YAML files in the current directory and common subdirs for
        a spec whose ``name`` matches *fv_name* (case-insensitive).

        Returns the parsed spec dict, or ``None`` if not found.
        """
        import glob as _glob

        try:
            import yaml
        except ImportError:
            return None

        search_dirs = [".", "feature_views", "specs", "example_store/feature_views"]
        for d in search_dirs:
            for path in _glob.glob(f"{d}/*.yaml") + _glob.glob(f"{d}/*.yml"):
                try:
                    with open(path) as f:
                        spec = yaml.safe_load(f)
                    if not isinstance(spec, dict):
                        continue
                    spec_name = spec.get("name", "")
                    if spec_name.lower() == fv_name.lower():
                        return spec
                except Exception:
                    continue
        return None

    @staticmethod
    def _find_datasource(source_name: str) -> Optional[dict[str, Any]]:
        """Try to find a local datasource YAML by source name.

        Searches YAML files in common datasource directories for a spec
        whose ``name`` matches *source_name* (case-insensitive).

        Returns the parsed datasource dict, or ``None`` if not found.
        """
        import glob as _glob

        try:
            import yaml
        except ImportError:
            return None

        search_dirs = [
            ".",
            "datasources",
            "sources",
            "example_store/datasources",
            "example_store/sources",
        ]
        for d in search_dirs:
            for path in _glob.glob(f"{d}/*.yaml") + _glob.glob(f"{d}/*.yml"):
                try:
                    with open(path) as f:
                        spec = yaml.safe_load(f)
                    if not isinstance(spec, dict):
                        continue
                    spec_name = spec.get("name", "")
                    if spec_name.lower() == source_name.lower():
                        return spec
                except Exception:
                    continue
        return None

    def _build_session(self) -> Any:
        """Construct a Snowpark Session from the CLI's existing connection.

        The Session wraps the snowflake-connector connection already managed
        by ``SqlExecutionMixin``, so no new connection is created.
        """
        from snowflake.snowpark import Session

        return Session.builder.configs({"connection": self._conn}).create()

    def _get_feature_store(self) -> Any:
        """Return a snowml-core ``FeatureStore`` bound to the active connection.

        Mirrors :func:`imperative_executor._get_fs` (used by the apply
        path): constructs ``FeatureStore`` lazily with
        ``creation_mode=FAIL_IF_NOT_EXIST`` so the schema must already be
        initialised as a SnowML feature store.  Session priming is run
        first via :meth:`_ensure_session_setup` so subsequent
        ``stream_ingest`` / ``read_feature_view`` calls inherit the
        correct session settings.

        The apply path inside ``imperative_executor.py`` keeps its own
        ``_get_fs`` because the two callers want different semantics:
        the executor defers construction until the first FV op and
        wraps it in a single-element box, while the CLI's
        ``ingest`` / ``query`` methods need a freshly-bound store on
        each invocation.
        """
        self._ensure_session_setup()
        ctx = get_cli_context()
        session = self._build_session()
        from snowflake.ml.feature_store.feature_store import (  # lazy
            CreationMode,
            FeatureStore,
        )

        return FeatureStore(
            session,
            ctx.connection.database,
            ctx.connection.schema,
            ctx.connection.warehouse or "",
            creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
        )

    def _fetch_oft_state(
        self,
        oft_rows: list[dict[str, Any]],
        state_sqls: dict[str, str],
    ) -> dict[str, dict[str, Any]]:
        """Fetch per-OFT spec JSON via ``DESCRIBE … TYPE = SPECIFICATION``.

        The session must already be primed (see :meth:`_ensure_session_setup`),
        so the SPECIFICATION call is the authoritative source of truth for
        every OFT.  Per the strict-spec contract, both ``execute_query``
        failures and ``decl_api.parse_specification_rows`` failures
        propagate to the caller — there is no column-DESCRIBE fallback.
        """
        specification_map: dict[str, dict[str, Any]] = {}
        spec_template = state_sqls.get("describe_specification_template")
        if not spec_template:
            return specification_map
        for row in oft_rows:
            name = row.get("name", "")
            if not name:
                continue
            spec_sql = spec_template.format(name=name)
            spec_rows = _rows_to_dicts(
                self.execute_query(spec_sql, cursor_class=DictCursor)
            )
            parsed = decl_api.parse_specification_rows(spec_rows)
            if parsed is not None:
                specification_map[name] = parsed
        return specification_map

    def _fetch_entity_rows(self, ctx: Any) -> list[dict[str, Any]]:
        """Fetch entity tag rows via the imperative ``list_entities()`` facade.

        Delegates to :func:`decl_api.fetch_entity_rows`, which lazy-imports
        the imperative ``FeatureStore`` inside ``imperative_executor.py``
        — the only declarative module permitted to do so.  The CLI no
        longer issues a raw ``SHOW TAGS`` query of its own.

        Failures are tolerated (logged and converted to an empty list) so
        that missing-privilege paths still let ``snow feature list``
        complete with FeatureView rows only, mirroring the prior raw-SQL
        behaviour.
        """
        try:
            session = self._build_session()
            return decl_api.fetch_entity_rows(
                session,
                ctx.connection.database,
                ctx.connection.schema,
                ctx.connection.warehouse or "",
            )
        except Exception as exc:
            log.debug("fetch_entity_rows failed (treating as empty): %s", exc)
            return []

    @staticmethod
    def _expand_with_datasources(input_files: list[str]) -> list[str]:
        """Expand input file list to include datasource YAMLs from sibling dirs.

        For each input file, checks for ``datasources/``, ``sources/``, and
        ``entities/`` directories alongside or one level up, and adds any YAML
        files found there. This ensures the loader picks up datasource definitions
        that feature views reference by name.
        """
        import glob as _glob
        from pathlib import Path

        result = list(input_files)
        seen = set(result)

        for f in input_files:
            p = Path(f)
            # Check sibling directories
            for sibling_name in ("datasources", "sources", "entities"):
                for parent in [p.parent, p.parent.parent]:
                    sibling = parent / sibling_name
                    if sibling.is_dir():
                        for extra in _glob.glob(str(sibling / "*.yaml")) + _glob.glob(
                            str(sibling / "*.yml")
                        ):
                            if extra not in seen:
                                result.append(extra)
                                seen.add(extra)
        return result

    # ------------------------------------------------------------------
    # get_status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Query and parse the feature store runtime status."""
        self._ensure_session_setup()
        ctx = get_cli_context()
        sqls = decl_api.service_sql(ctx.connection.database, ctx.connection.schema)
        try:
            rows = list(self.execute_query(sqls["get_status"]))
            raw = list(rows[0])[0] if rows else None
            if not raw:
                return {"status": "error", "error": "No response from system function"}
            result = decl_api.parse_service_status(raw)
            # Add connection context for display
            result["_user"] = ctx.connection.user or ""
            result["_database"] = ctx.connection.database or ""
            result["_schema"] = ctx.connection.schema or ""
            return result
        except Exception as exc:
            log.warning("get_status raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # initialize_service
    # ------------------------------------------------------------------

    def initialize_service(
        self,
        producer_role: Optional[str] = None,
        consumer_role: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send CREATE runtime command. Returns immediately; caller polls."""
        self._ensure_session_setup()
        ctx = get_cli_context()
        p_role = producer_role or ctx.connection.role
        c_role = consumer_role or "PUBLIC"
        sqls = decl_api.service_sql(
            ctx.connection.database, ctx.connection.schema, p_role, c_role
        )
        location = f"{ctx.connection.database}.{ctx.connection.schema}"

        current = self.get_status()
        if current.get("status") == "RUNNING":
            return {
                "status": "RUNNING",
                "message": f"Service already initialized in {location}",
            }

        try:
            self.execute_query(sqls["create"])
        except Exception as exc:
            log.warning("create_runtime raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

        return {"status": "CREATING", "message": f"Create requested for {location}"}

    # ------------------------------------------------------------------
    # destroy_service
    # ------------------------------------------------------------------

    def destroy_service(self) -> dict[str, Any]:
        """Drop all OFTs then drop the feature store runtime."""
        self._ensure_session_setup()
        ctx = get_cli_context()
        sqls = decl_api.service_sql(ctx.connection.database, ctx.connection.schema)

        dropped_ofts: list[str] = []
        errors: list[str] = []
        try:
            rows = list(self.execute_query(sqls["show_ofts"], cursor_class=DictCursor))
            for row in rows:
                name = row.get("name", "")
                if name:
                    try:
                        drop_sql = decl_api.drop_queries(
                            [name], ctx.connection.database, ctx.connection.schema
                        )
                        for sql in drop_sql:
                            self.execute_query(sql)
                        dropped_ofts.append(name)
                    except Exception as exc:
                        log.warning("drop OFT %s: %s", name, exc)
                        errors.append(f"{name}: {exc}")
        except Exception as exc:
            log.warning("SHOW OFTs raised %s: %s", type(exc).__name__, exc)
            errors.append(f"SHOW OFTs: {exc}")

        try:
            self.execute_query(sqls["drop"])
        except Exception as exc:
            log.warning("drop_runtime raised %s: %s", type(exc).__name__, exc)
            errors.append(f"drop_runtime: {exc}")

        return {"status": "destroyed", "dropped_ofts": dropped_ofts, "errors": errors}

    # ------------------------------------------------------------------
    # export_specs
    # ------------------------------------------------------------------

    def export_specs(self, output_dir: str) -> dict[str, Any]:
        """Export deployed feature-store objects as YAML spec files.

        Strict full-fidelity flow: prime the session, list OFTs, then fetch
        each OFT's full spec JSON via ``DESCRIBE … TYPE = SPECIFICATION``
        through :meth:`_fetch_oft_state`.  Any per-OFT failure aborts the
        entire export — there is no column-DESCRIBE fallback.

        Entity rows are fetched via the imperative
        :func:`decl_api.fetch_entity_rows` facade and forwarded to the
        exporter so orphan entity tags (registered via
        ``FeatureStore.register_entity()`` without ever being attached to
        an FV) survive the export → plan round-trip.  Without this
        forwarding, full-directory plans would emit spurious
        ``DROP_ENTITY`` ops for any unreferenced tag.
        """
        self._ensure_session_setup()
        ctx = get_cli_context()
        eq = decl_api.export_queries(ctx.connection.database, ctx.connection.schema)

        show_rows = _rows_to_dicts(
            self.execute_query(eq["show_ofts"], cursor_class=DictCursor)
        )

        # Always fetch entity rows — the exporter needs them whether or
        # not any FVs are deployed (a schema with only entities still
        # requires their YAMLs on disk to plan as NO_CHANGE).
        entity_rows = self._fetch_entity_rows(ctx)

        # Skip the exporter only when the schema is genuinely empty
        # (no FVs and no entity tags).  This preserves the pre-fix
        # short-circuit for fresh-schema callers while still exporting
        # entity-only schemas through the regular path.
        if not show_rows and not entity_rows:
            return {"status": "exported", "directory": "", "files": []}

        specification_map = self._fetch_oft_state(show_rows, eq) if show_rows else {}

        return decl_api.export_specs(
            show_rows,
            {},
            output_dir,
            ctx.connection.database,
            ctx.connection.schema,
            specification_map=specification_map,
            entity_rows=entity_rows,
        )

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------

    def ingest(self, source_name: str, records: list[dict]) -> dict[str, Any]:
        """Stream records into a source via ``FeatureStore.stream_ingest``.

        Delegates the entire wire path (URL resolution, PAT auth, schema
        validation, partial-success reporting) to snowml-core.  The
        manager's only job is to bind the connection-scoped
        ``FeatureStore`` and shape the result envelope so downstream
        CLI rendering matches the rest of the feature plugin
        (``target_database`` / ``target_schema`` / ``target_warehouse``
        triple, plus the new ``accepted_count`` field).

        Args:
            source_name: Registered streaming source name.
            records: Non-empty list of row dicts; each row's keys must
                match the source schema exactly (snowml-core enforces).

        Returns:
            ``{**target_info, "accepted_count": int}`` where
            ``accepted_count`` is the integer returned by
            ``FeatureStore.stream_ingest`` (may be less than
            ``len(records)`` on partial success).

        Raises:
            Exception: any error raised by
                ``FeatureStore.stream_ingest`` (PAT missing, schema
                mismatch, network failure, etc.) is propagated unchanged.
        """
        fs = self._get_feature_store()
        accepted = fs.stream_ingest(source_name, records)
        return {**self._target_info(), "accepted_count": accepted}

    # ------------------------------------------------------------------
    # query
    # ------------------------------------------------------------------

    def query(
        self,
        feature_view_name: str,
        version: str,
        keys: list[dict],
    ) -> dict[str, Any]:
        """Online-lookup features via ``FeatureStore.read_feature_view``.

        ``version`` is required because snowml-core's
        ``FeatureStore.get_feature_view(name, version)`` requires both
        when the feature view is referenced by string — there is no
        "latest version" lookup for a bare name.  The CLI surfaces
        this as a required ``--version`` option (see ``commands.py``).

        The dict-shaped input ``keys`` is translated to snowml-core's
        positional list-of-list shape via :func:`_to_positional_keys`,
        using the FV's declared entity join-key order.  Missing
        join-key columns raise a clear ``ValueError`` *before* any
        wire call.

        Args:
            feature_view_name: Logical feature view name.
            version: FeatureView version (e.g. ``"V1"``).
            keys: List of dicts; each dict must carry every join-key
                column declared by the FV.

        Returns:
            ``{**target_info, "rows": list[dict]}`` where ``rows`` is
            ``df.to_dict("records")`` of the pandas DataFrame returned
            by ``FeatureStore.read_feature_view`` (Postgres online path
            renders directly to pandas).

        Raises:
            ValueError: If an input dict is missing a declared join-key.
            Exception: Any error raised by ``read_feature_view``
                (PAT missing, no online store, network failure, etc.)
                is propagated unchanged.
        """
        fs = self._get_feature_store()
        fv = fs.get_feature_view(feature_view_name, version)
        join_key_order = [str(jk) for ent in fv.entities for jk in ent.join_keys]
        positional_keys = _to_positional_keys(keys, join_key_order)
        df = fs.read_feature_view(
            fv,
            keys=positional_keys,
            store_type="ONLINE",
            as_pandas=True,
        )
        return {**self._target_info(), "rows": df.to_dict("records")}
