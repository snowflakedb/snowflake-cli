# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FeatureManager — orchestrates calls between the CLI and the decl library."""

from __future__ import annotations

import glob as _glob
import logging
from typing import Any, Optional, Sequence, Tuple

from snowflake.cli.api.sql_execution import SqlExecutionMixin

try:
    from snowflake.ml.feature_store.decl import api as decl_api
except ImportError:
    decl_api = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

_NOT_IMPL_MSG = (
    "decl library not yet available (Phase 1 in progress). "
    "Returning placeholder result."
)


def _expand_globs(patterns: Sequence[str]) -> list[str]:
    """Expand any glob patterns in *patterns* into a flat file list."""
    files: list[str] = []
    for pattern in patterns:
        expanded = _glob.glob(pattern, recursive=True)
        files.extend(expanded if expanded else [pattern])
    return files


class FeatureManager(SqlExecutionMixin):
    """Orchestrates the declarative feature-store workflow.

    All calls to the ``decl`` shared library are wrapped in try/except so
    that the CLI remains structurally testable before Phase 1 is complete.
    """

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
        files = _expand_globs(input_files)
        log.debug("apply: files=%s dry_run=%s", files, dry_run)

        # --- 1. Load specs ---
        try:
            batch = decl_api.load_specs(files, config)  # type: ignore[union-attr]
        except (NotImplementedError, Exception) as exc:
            log.warning("load_specs raised %s: %s", type(exc).__name__, exc)
            return {"status": "pending", "message": _NOT_IMPL_MSG, "error": str(exc)}

        # --- 2. Fetch live state ---
        try:
            raw_show = list(self.execute_query("SHOW ONLINE FEATURE TABLES IN SCHEMA"))
            raw_tables = list(self.execute_query("SHOW TABLES LIKE '%' IN SCHEMA"))
            applied_state = decl_api.fetch_applied_state(  # type: ignore[union-attr]
                raw_show_results=[dict(r) for r in raw_show],
                raw_table_results=[dict(r) for r in raw_tables],
            )
        except (NotImplementedError, Exception) as exc:
            log.warning("fetch_applied_state raised %s: %s", type(exc).__name__, exc)
            return {"status": "pending", "message": _NOT_IMPL_MSG, "error": str(exc)}

        # --- 3. Validate ---
        try:
            validation_results = decl_api.validate_specs(batch, applied_state)  # type: ignore[union-attr]
            errors = [r for r in validation_results if getattr(r, "is_error", False)]
            if errors:
                return {
                    "status": "validation_failed",
                    "errors": [str(e) for e in errors],
                }
        except (NotImplementedError, Exception) as exc:
            log.warning("validate_specs raised %s: %s", type(exc).__name__, exc)
            return {"status": "pending", "message": _NOT_IMPL_MSG, "error": str(exc)}

        # --- 4. Generate plan ---
        try:
            from snowflake.ml.feature_store.decl.types import PlanOptions

            options = PlanOptions(
                dev_mode=dev_mode, overwrite=overwrite, allow_recreate=allow_recreate
            )
            plan = decl_api.generate_plan(batch, applied_state, options)  # type: ignore[union-attr]
        except (NotImplementedError, Exception) as exc:
            log.warning("generate_plan raised %s: %s", type(exc).__name__, exc)
            return {"status": "pending", "message": _NOT_IMPL_MSG, "error": str(exc)}

        # --- 5. Display plan ---
        ops = getattr(plan, "ops", [])
        log.debug("plan ops: %d", len(ops))

        # --- 6. Execute (if not dry_run) ---
        executed: list[str] = []
        if not dry_run:
            for op in ops:
                sql = getattr(op, "sql", None)
                if sql:
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
            files = _expand_globs(input_files)
            try:
                batch = decl_api.load_specs(files, config)  # type: ignore[union-attr]
                specs = getattr(batch, "specs", [])
                return {"source": "files", "specs": [str(s) for s in specs]}
            except (NotImplementedError, Exception) as exc:
                log.warning("load_specs raised %s: %s", type(exc).__name__, exc)
                return {
                    "status": "pending",
                    "message": _NOT_IMPL_MSG,
                    "error": str(exc),
                }

        # No files — list from Snowflake
        try:
            rows = list(self.execute_query("SHOW ONLINE FEATURE TABLES IN SCHEMA"))
            return {"source": "snowflake", "specs": [dict(r) for r in rows]}
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
            rows = list(self.execute_query(f"SHOW ONLINE FEATURE TABLES LIKE '{name}'"))
            return {"name": name, "rows": [dict(r) for r in rows]}
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
        files = _expand_globs(input_files)
        try:
            batch = decl_api.load_specs(files, config)  # type: ignore[union-attr]
            specs = getattr(batch, "specs", [])
            return {
                "status": "converted",
                "format": file_format,
                "output_dir": output_dir,
                "recursive": recursive,
                "count": len(specs),
            }
        except (NotImplementedError, Exception) as exc:
            log.warning("convert raised %s: %s", type(exc).__name__, exc)
            return {"status": "pending", "message": _NOT_IMPL_MSG, "error": str(exc)}
