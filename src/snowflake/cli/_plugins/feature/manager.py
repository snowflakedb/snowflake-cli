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


class FeatureManager(SqlExecutionMixin):
    """Thin CLI adapter — delegates all business logic to decl_api."""

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
    # generate_example
    # ------------------------------------------------------------------

    def generate_example(self, output_dir: str) -> dict[str, Any]:
        """Write example YAML spec files under *output_dir*."""
        return decl_api.generate_example(output_dir)

    # ------------------------------------------------------------------
    # apply
    # ------------------------------------------------------------------

    def apply(
        self,
        input_files: Sequence[str],
        config: Optional[dict[str, Any]],
        dry_run: bool,
        dev_mode: bool,
        overwrite: bool,
        allow_recreate: bool,
        plan_file: Optional[str] = None,
        no_delete: bool = True,
    ) -> dict[str, Any]:
        """Load → validate → plan → generate SQL → (execute if not dry_run).

        If *plan_file* is provided, skip spec loading and plan generation —
        deserialize the pre-computed plan from the file and execute it.

        If *no_delete* is True, deletion detection is disabled — objects in
        Snowflake not represented in the local spec files will NOT be dropped.
        """
        if plan_file is not None:
            return self._apply_from_plan_file(
                plan_file=plan_file,
                dry_run=dry_run,
                dev_mode=dev_mode,
                overwrite=overwrite,
                allow_recreate=allow_recreate,
            )

        ctx = get_cli_context()

        # 1. Fetch state via decl_api query strings
        sqls = decl_api.state_queries(ctx.connection.database, ctx.connection.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )

        # DESCRIBE each deployed OFT for structural fingerprinting
        eq = decl_api.export_queries(ctx.connection.database, ctx.connection.schema)
        describe_map: dict[str, list[dict[str, Any]]] = {}
        for row in raw_show:
            name = row.get("name", "")
            if name:
                desc_sql = eq["describe_template"].format(name=name)
                describe_map[name] = _rows_to_dicts(
                    self.execute_query(desc_sql, cursor_class=DictCursor)
                )

        applied_state = decl_api.fetch_applied_state(raw_show, raw_tables, describe_map)

        # 2. Load specs — also scan sibling directories for datasource YAMLs
        all_files = self._expand_with_datasources(list(input_files))
        batch = decl_api.load_specs(all_files, config)

        # 3. Validate + plan + execute via imperative API
        from snowflake.ml.feature_store.decl.types import PlanOptions

        options = PlanOptions(
            dev_mode=dev_mode,
            overwrite=overwrite,
            allow_recreate=allow_recreate,
            full_directory_mode=not no_delete,
        )

        if dry_run:
            # Dry run: generate plan + SQL for display only (no execution)
            result = decl_api.generate_apply_sql(
                batch,
                applied_state,
                options,
                database=ctx.connection.database,
                schema=ctx.connection.schema,
                warehouse=ctx.connection.warehouse or "",
            )
            status = (
                "validation_failed"
                if result.status == "validation_failed"
                else "dry_run"
            )
            return {
                **self._target_info(),
                "status": status,
                "ops": result.ops,
                "executed": 0,
                "warnings": result.warnings,
                "errors": result.errors,
            }

        # Wet run: validate → plan → execute via FeatureStore objects
        validation_results = decl_api.validate_specs(batch, applied_state)
        errors = [
            r for r in validation_results if getattr(r, "severity", "") == "ERROR"
        ]
        if errors:
            return {
                **self._target_info(),
                "status": "validation_failed",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [str(e) for e in errors],
            }

        plan = decl_api.generate_plan(batch, applied_state, options)
        session = self._build_session()
        result = decl_api.execute_plan(
            plan,
            session,
            database=ctx.connection.database,
            schema=ctx.connection.schema,
            warehouse=ctx.connection.warehouse or "",
            options=options,
        )

        return {
            **self._target_info(),
            "status": result.status,
            "ops": result.ops,
            "executed": len([o for o in result.ops if o.get("status") == "success"]),
            "warnings": result.warnings,
            "errors": result.errors,
        }

    # ------------------------------------------------------------------
    # _apply_from_plan_file (private helper)
    # ------------------------------------------------------------------

    def _apply_from_plan_file(
        self,
        plan_file: str,
        dry_run: bool,
        dev_mode: bool,
        overwrite: bool,
        allow_recreate: bool,
    ) -> dict[str, Any]:
        """Execute a pre-computed plan loaded from a JSON plan file."""
        from pathlib import Path

        from snowflake.ml.feature_store.decl.types import PlanOptions

        json_str = Path(plan_file).read_text()
        pf = decl_api.deserialize_plan(json_str)
        plan = pf.plan
        database = pf.target_database
        schema = pf.target_schema

        options = PlanOptions(
            dev_mode=dev_mode,
            overwrite=overwrite,
            allow_recreate=allow_recreate,
        )

        if dry_run:
            ops = getattr(plan, "ops", [])
            return {
                "status": "dry_run",
                "ops": [
                    {
                        "operation": op.kind.value,
                        "name": op.name.lower(),
                        "reason": op.reason,
                        "destructive": op.destructive,
                    }
                    for op in ops
                ],
                "executed": 0,
                "warnings": list(getattr(plan, "warnings", [])),
                "errors": [],
                "plan_file": plan_file,
            }

        session = self._build_session()
        result = decl_api.execute_plan(
            plan,
            session,
            database=database,
            schema=schema,
            warehouse="",
            options=options,
        )
        return {
            "status": result.status,
            "ops": result.ops,
            "executed": len([o for o in result.ops if o.get("status") == "success"]),
            "warnings": result.warnings,
            "errors": result.errors,
            "plan_file": plan_file,
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

        ctx = get_cli_context()

        # Fetch applied state
        sqls = decl_api.state_queries(ctx.connection.database, ctx.connection.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        eq = decl_api.export_queries(ctx.connection.database, ctx.connection.schema)
        describe_map: dict[str, list[dict[str, Any]]] = {}
        for row in raw_show:
            name = row.get("name", "")
            if name:
                desc_sql = eq["describe_template"].format(name=name)
                describe_map[name] = _rows_to_dicts(
                    self.execute_query(desc_sql, cursor_class=DictCursor)
                )
        applied_state = decl_api.fetch_applied_state(raw_show, raw_tables, describe_map)

        # Load and expand specs
        all_files = self._expand_with_datasources(list(input_files))
        batch = decl_api.load_specs(all_files, config)

        options = PlanOptions(dev_mode=dev_mode, full_directory_mode=not no_delete)
        plan = decl_api.generate_plan(batch, applied_state, options)

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
        """List specs from files or deployed objects from Snowflake."""
        if input_files:
            batch = decl_api.load_specs(list(input_files), config)
            specs = getattr(batch, "specs", [])
            return {"source": "files", "specs": [str(s) for s in specs]}

        ctx = get_cli_context()
        try:
            sql = decl_api.list_query(ctx.connection.database, ctx.connection.schema)
            rows = _rows_to_dicts(self.execute_query(sql, cursor_class=DictCursor))

            # Enrich with feature_view name, version, and entities
            eq = decl_api.export_queries(ctx.connection.database, ctx.connection.schema)
            describe_map: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                name = row.get("name", "")
                desc_sql = eq["describe_template"].format(name=name)
                describe_map[name] = _rows_to_dicts(
                    self.execute_query(desc_sql, cursor_class=DictCursor)
                )
            enriched = decl_api.enrich_list_results(rows, describe_map)
            return {**self._target_info(), "source": "snowflake", "specs": enriched}
        except Exception as exc:
            log.warning("list query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(self, name: str) -> dict[str, Any]:
        """Return metadata for a named feature view (resolves to OFT name)."""
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
    # convert
    # ------------------------------------------------------------------

    def convert(
        self,
        input_files: Sequence[str],
        file_format: str,
        output_dir: Optional[str],
        recursive: bool,
        config: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Convert spec files from Python DSL to YAML or JSON."""
        batch = decl_api.load_specs(list(input_files), config)
        specs = getattr(batch, "specs", [])
        return {
            "status": "converted",
            "format": file_format,
            "output_dir": output_dir,
            "recursive": recursive,
            "count": len(specs),
        }

    # ------------------------------------------------------------------
    # get_status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Query and parse the feature store runtime status."""
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
        """Export deployed feature-store objects as YAML spec files."""
        ctx = get_cli_context()
        eq = decl_api.export_queries(ctx.connection.database, ctx.connection.schema)

        show_rows = _rows_to_dicts(
            self.execute_query(eq["show_ofts"], cursor_class=DictCursor)
        )
        if not show_rows:
            return {"status": "exported", "directory": "", "files": []}

        describe_map: dict[str, list[dict[str, Any]]] = {}
        for row in show_rows:
            name = row.get("name", "")
            desc_sql = eq["describe_template"].format(name=name)
            describe_map[name] = _rows_to_dicts(
                self.execute_query(desc_sql, cursor_class=DictCursor)
            )

        return decl_api.export_specs(
            show_rows,
            describe_map,
            output_dir,
            ctx.connection.database,
            ctx.connection.schema,
        )

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------

    def ingest(self, source_name: str, records: list[dict]) -> dict[str, Any]:
        """Stream records into a source via the Online Service."""
        pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
        if not pat:
            raise RuntimeError(
                "SNOWFLAKE_PAT environment variable is required for feature ingest."
            )

        status = self.get_status()
        url = decl_api.get_service_endpoint(status, "ingest")
        if not url:
            return {
                "status": "error",
                "error": "Feature store service is not running or has no ingest endpoint.",
            }

        body = decl_api.build_ingest_request(source_name, records)
        return decl_api.post_service_json(url.rstrip("/") + "/api/v1/ingest", pat, body)

    # ------------------------------------------------------------------
    # query
    # ------------------------------------------------------------------

    def query(self, feature_view_name: str, keys: list[dict]) -> dict[str, Any]:
        """Query online features via the Online Service."""
        pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
        if not pat:
            raise RuntimeError(
                "SNOWFLAKE_PAT environment variable is required for feature query."
            )

        status = self.get_status()
        url = decl_api.get_service_endpoint(status, "query")
        if not url:
            return {
                "status": "error",
                "error": "Feature store service is not running or has no query endpoint.",
            }

        body = decl_api.build_query_request(feature_view_name, keys)
        return decl_api.post_service_json(url.rstrip("/") + "/api/v1/query", pat, body)
