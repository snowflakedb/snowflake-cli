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

"""FeatureManager — orchestrates calls between the CLI and the decl library."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional, Sequence, Tuple

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor

try:
    from snowflake.ml.feature_store.decl import api as decl_api
    from snowflake.ml.feature_store.decl.sql_generator import generate_sql
    from snowflake.ml.feature_store.decl.types import PlanOptions

    _HAS_DECL_API = True
except ImportError:
    decl_api = None  # type: ignore[assignment]
    generate_sql = None  # type: ignore[assignment]
    PlanOptions = None  # type: ignore[assignment]
    _HAS_DECL_API = False

log = logging.getLogger(__name__)


def _rows_to_dicts(rows) -> list[dict[str, Any]]:
    """Convert DictCursor rows to plain dicts for JSON serialization."""
    return [dict(r) for r in rows]


class FeatureManager(SqlExecutionMixin):
    """Orchestrates the declarative feature-store workflow."""

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
    ) -> dict[str, Any]:
        """Load → fetch state → validate → plan → (execute if not dry_run)."""
        log.debug("apply: files=%s dry_run=%s", list(input_files), dry_run)

        # --- 1. Load specs ---
        batch = decl_api.load_specs(list(input_files), config)

        # --- 2. Fetch live state ---
        raw_show = list(
            self.execute_query(
                "SHOW ONLINE FEATURE TABLES IN SCHEMA", cursor_class=DictCursor
            )
        )
        raw_tables = list(
            self.execute_query(
                "SHOW TABLES LIKE '%' IN SCHEMA", cursor_class=DictCursor
            )
        )
        applied_state = decl_api.fetch_applied_state(
            raw_show_results=_rows_to_dicts(raw_show),
            raw_table_results=_rows_to_dicts(raw_tables),
        )

        # --- 3. Validate ---
        validation_results = decl_api.validate_specs(batch, applied_state)
        errors = [
            r for r in validation_results if getattr(r, "severity", "") == "ERROR"
        ]
        if errors:
            return {
                "status": "validation_failed",
                "errors": [str(e) for e in errors],
            }

        # --- 4. Generate plan ---
        options = PlanOptions(
            dev_mode=dev_mode, overwrite=overwrite, allow_recreate=allow_recreate
        )
        plan = decl_api.generate_plan(batch, applied_state, options)

        # --- 5. Display plan ---
        ops = getattr(plan, "ops", [])
        log.debug("plan ops: %d", len(ops))

        # --- 6. Execute (if not dry_run) ---
        sql_stmts = generate_sql(plan)
        executed: list[str] = []
        if not dry_run:
            for sql in sql_stmts:
                self.execute_query(sql)
                executed.append(sql)

        return {
            "status": "dry_run" if dry_run else "applied",
            "ops": len(ops),
            "executed": len(executed),
        }

    # ------------------------------------------------------------------
    # list_specs
    # ------------------------------------------------------------------

    def list_specs(
        self,
        input_files: Tuple[str, ...],
        config: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """List specs from files (if provided) or deployed specs from Snowflake."""
        if input_files:
            batch = decl_api.load_specs(list(input_files), config)
            specs = getattr(batch, "specs", [])
            return {"source": "files", "specs": [str(s) for s in specs]}

        # No files — list from Snowflake
        try:
            rows = list(
                self.execute_query(
                    "SHOW ONLINE FEATURE TABLES IN SCHEMA",
                    cursor_class=DictCursor,
                )
            )
            return {"source": "snowflake", "specs": _rows_to_dicts(rows)}
        except Exception as exc:
            log.warning("SHOW query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(
        self,
        name: str,
    ) -> dict[str, Any]:
        """Return metadata for a single named feature-store object."""
        try:
            rows = list(
                self.execute_query(
                    f"SHOW ONLINE FEATURE TABLES LIKE '{name}'",
                    cursor_class=DictCursor,
                )
            )
            return {"name": name, "rows": _rows_to_dicts(rows)}
        except Exception as exc:
            log.warning("describe raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "name": name, "error": str(exc)}

    # ------------------------------------------------------------------
    # drop
    # ------------------------------------------------------------------

    def drop(
        self,
        names: Sequence[str],
    ) -> dict[str, Any]:
        """Drop one or more named feature-store objects."""
        dropped: list[str] = []
        errors: list[str] = []
        for name in names:
            try:
                self.execute_query(f"DROP ONLINE FEATURE TABLE IF EXISTS {name}")
                dropped.append(name)
            except Exception as exc:
                log.warning("drop %s raised %s: %s", name, type(exc).__name__, exc)
                errors.append(f"{name}: {exc}")
        return {"dropped": dropped, "errors": errors}

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
            return decl_api.parse_service_status(raw)
        except Exception as exc:
            log.warning("get_status raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # initialize_service
    # ------------------------------------------------------------------

    def initialize_service(self) -> dict[str, Any]:
        """Check status, create runtime if needed, then poll until RUNNING."""
        ctx = get_cli_context()
        sqls = decl_api.service_sql(ctx.connection.database, ctx.connection.schema)
        location = f"{ctx.connection.database}.{ctx.connection.schema}"

        current = self.get_status()
        if current.get("status") == "RUNNING":
            log.info("Feature store runtime already running in %s", location)
            return {
                "status": "RUNNING",
                "message": f"Service already initialized in {location}",
            }

        try:
            self.execute_query(sqls["create"])
        except Exception as exc:
            log.warning("create_runtime raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

        # Poll until RUNNING (max 10 minutes, every 15 seconds)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            time.sleep(15)
            current = self.get_status()
            if current.get("status") == "RUNNING":
                return {
                    "status": "RUNNING",
                    "message": "Service initialized successfully",
                }

        return {
            "status": "timeout",
            "error": "Timed out waiting for service to reach RUNNING",
        }

    # ------------------------------------------------------------------
    # destroy_service
    # ------------------------------------------------------------------

    def destroy_service(self) -> dict[str, Any]:
        """Drop all OFTs in the schema then drop the feature store runtime."""
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
                        self.execute_query(sqls["drop_oft_template"].format(name=name))
                        dropped_ofts.append(name)
                    except Exception as exc:
                        log.warning(
                            "drop OFT %s raised %s: %s", name, type(exc).__name__, exc
                        )
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
        """Query SHOW ONLINE FEATURE TABLES, DESCRIBE each, delegate to decl_api."""
        ctx = get_cli_context()
        show_rows = _rows_to_dicts(
            self.execute_query(
                "SHOW ONLINE FEATURE TABLES IN SCHEMA", cursor_class=DictCursor
            )
        )
        if not show_rows:
            return {"status": "exported", "directory": "", "files": []}

        describe_map: dict[str, list[dict[str, Any]]] = {}
        for row in show_rows:
            name = row.get("name", "")
            fqn = f'"{ctx.connection.database}"."{ctx.connection.schema}"."{name}"'
            describe_map[name] = _rows_to_dicts(
                self.execute_query(
                    f"DESCRIBE ONLINE FEATURE TABLE {fqn}",
                    cursor_class=DictCursor,
                )
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
        """Stream records into a source via the Online Service ingest endpoint."""
        pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
        if not pat:
            raise RuntimeError(
                "SNOWFLAKE_PAT environment variable is required for feature ingest. "
                "Set it to a Snowflake Programmatic Access Token."
            )

        status = self.get_status()
        url = decl_api.get_service_endpoint(status, "ingest")
        if not url:
            return {
                "status": "error",
                "error": (
                    "Feature store service is not running or has no ingest endpoint. "
                    "Run 'snow feature status' to check service status."
                ),
            }

        body = decl_api.build_ingest_request(source_name, records)
        return decl_api.post_service_json(url.rstrip("/") + "/api/v1/ingest", pat, body)

    # ------------------------------------------------------------------
    # query
    # ------------------------------------------------------------------

    def query(self, feature_view_name: str, keys: list[dict]) -> dict[str, Any]:
        """Query online features via the Online Service query endpoint."""
        pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
        if not pat:
            raise RuntimeError(
                "SNOWFLAKE_PAT environment variable is required for feature query. "
                "Set it to a Snowflake Programmatic Access Token."
            )

        status = self.get_status()
        url = decl_api.get_service_endpoint(status, "query")
        if not url:
            return {
                "status": "error",
                "error": (
                    "Feature store service is not running or has no query endpoint. "
                    "Run 'snow feature status' to check service status."
                ),
            }

        body = decl_api.build_query_request(feature_view_name, keys)
        return decl_api.post_service_json(url.rstrip("/") + "/api/v1/query", pat, body)
