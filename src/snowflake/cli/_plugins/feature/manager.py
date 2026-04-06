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
import time
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
        """Load → validate → plan → generate SQL → (execute if not dry_run)."""
        ctx = get_cli_context()

        # 1. Fetch state via decl_api query strings
        sqls = decl_api.state_queries(ctx.connection.database, ctx.connection.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        applied_state = decl_api.fetch_applied_state(raw_show, raw_tables)

        # 2. Load specs
        batch = decl_api.load_specs(list(input_files), config)

        # 3. Validate + plan + generate SQL — all in decl
        from snowflake.ml.feature_store.decl.types import PlanOptions

        options = PlanOptions(
            dev_mode=dev_mode,
            overwrite=overwrite,
            allow_recreate=allow_recreate,
        )
        result = decl_api.generate_apply_sql(
            batch,
            applied_state,
            options,
            database=ctx.connection.database,
            schema=ctx.connection.schema,
            warehouse=ctx.connection.warehouse or "",
        )

        # 4. Execute SQL (CLI's only job)
        executed = 0
        if result.status != "validation_failed" and not dry_run:
            for sql in result.sql_statements:
                self.execute_query(sql)
                executed += 1

        status = "validation_failed" if result.status == "validation_failed" else (
            "dry_run" if dry_run else "applied"
        )
        return {
            "status": status,
            "ops": result.ops,
            "executed": executed,
            "warnings": result.warnings,
            "errors": result.errors,
        }

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
            rows = list(self.execute_query(sql, cursor_class=DictCursor))
            return {"source": "snowflake", "specs": _rows_to_dicts(rows)}
        except Exception as exc:
            log.warning("list query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(self, name: str) -> dict[str, Any]:
        """Return metadata for a single named feature-store object."""
        ctx = get_cli_context()
        try:
            sql = decl_api.describe_query(
                name, ctx.connection.database, ctx.connection.schema
            )
            rows = list(self.execute_query(sql, cursor_class=DictCursor))
            return {"name": name, "rows": _rows_to_dicts(rows)}
        except Exception as exc:
            log.warning("describe raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "name": name, "error": str(exc)}

    # ------------------------------------------------------------------
    # drop
    # ------------------------------------------------------------------

    def drop(self, names: Sequence[str]) -> dict[str, Any]:
        """Drop one or more named feature-store objects."""
        ctx = get_cli_context()
        sqls = decl_api.drop_queries(
            list(names), ctx.connection.database, ctx.connection.schema
        )
        dropped: list[str] = []
        errors: list[str] = []
        for name, sql in zip(names, sqls):
            try:
                self.execute_query(sql)
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

    def initialize_service(
        self,
        producer_role: Optional[str] = None,
        consumer_role: Optional[str] = None,
    ) -> dict[str, Any]:
        """Check status, create runtime if needed, poll until RUNNING."""
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

        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            time.sleep(15)
            current = self.get_status()
            if current.get("status") == "RUNNING":
                return {"status": "RUNNING", "message": "Service initialized successfully"}

        return {"status": "timeout", "error": "Timed out waiting for RUNNING"}

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
