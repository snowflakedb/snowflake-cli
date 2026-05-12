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

"""FeatureManager — thin CLI adapter delegating all logic to decl_api.

Phase 3+4 manifest-driven shape (see
``plans/manifest_layout/phase3_4_cli_and_manager.md`` and
``plans/MANIFEST_YML_LAYOUT_DECISIONS.md``):

* Every Snowflake-bound entry-point takes ``from_dir`` (project root
  start) and ``target_name`` (manifest target name; ``None`` resolves
  to ``default_target``).  The manifest is the source of truth for
  ``database`` / ``schema`` / ``role`` / ``account_identifier``.
* ``warehouse`` is **never** read from the manifest — it always comes
  from the active connection (D2).
* Plan-file lifecycle (L1–L7) lives under
  ``<project_root>/out/plan/`` (D8).  See the apply-lifecycle
  section of ``DESIGN.md`` for the L1–L7 invariants.
* ``init`` writes the manifest auto-derived from the active
  connection (D6) and fails fast when ``manifest.yml`` is already
  present (init-exist locked decision).
* The boundary rule from ``docs/DEVELOPMENT_STANDARDS.md`` still
  holds: this module contains no SQL strings — every state query is
  built by ``decl_api`` and executed via ``self.execute_query``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

from snowflake.cli._plugins.connection.util import get_account_identifier
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import AccountIdentifier
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor

# Manifest helpers are dataclasses + a YAML loader — no SQL, no
# session, no Snowflake imports.  We import them directly (rather
# than through ``decl_api``) because the test suite mocks
# ``decl_api`` to assert the manager's SQL/business-logic seam, not
# the manifest-loading seam.  The DCM plugin follows the same
# pattern (``DCMManifest.load(...)`` is called directly from
# commands.py).
from snowflake.ml.feature_store.decl.manifest import (
    FSManifest,
    FSTarget,
    ManifestNotFoundError,
)
from snowflake.ml.feature_store.decl.project_paths import FSProjectPaths

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
    matching the FeatureView's declared join-key sequence.

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


def _parse_variables(variables: Optional[Sequence[str]]) -> dict[str, Any]:
    """Parse ``--variable key=value`` repeats into a dict.

    Args:
        variables: Sequence of ``key=value`` strings (or ``None``).

    Returns:
        Dict mapping each ``key`` to its (string) ``value``.  Empty
        dict when *variables* is ``None`` / empty.
    """
    parsed = parse_key_value_variables(list(variables or []))
    return {v.key: v.value for v in parsed}


_DEFAULT_MANIFEST_TARGET = "DEFAULT"
_DEFAULT_MANIFEST_TEMPLATE = """\
manifest_version: 1
type: feature_store
default_target: {target}
targets:
  {target}:
    account_identifier: {account_identifier}
    database: {database}
    schema: {schema}
{role_line}\
"""


def _render_default_manifest(
    *,
    account_identifier: str,
    database: str,
    schema: str,
    role: str,
    target: str = _DEFAULT_MANIFEST_TARGET,
) -> str:
    """Render the default ``manifest.yml`` body from connection fields.

    The result intentionally omits ``warehouse`` (D2) — the active
    connection is the sole source of truth for warehouse selection.
    """
    role_line = f"    role: {role}\n" if role else ""
    return _DEFAULT_MANIFEST_TEMPLATE.format(
        target=target,
        account_identifier=account_identifier,
        database=database,
        schema=schema,
        role_line=role_line,
    )


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

    # ------------------------------------------------------------------
    # _resolve_project — manifest discovery + account match (D4)
    # ------------------------------------------------------------------

    def _resolve_project(
        self,
        from_dir: Path,
        target_name: Optional[str],
    ) -> Tuple[FSProjectPaths, FSManifest, FSTarget]:
        """Walk up from *from_dir* to the project root, load the manifest,
        resolve the requested target, and assert account match (D4).

        Args:
            from_dir: Starting directory (or file) for manifest
                discovery.  ``FSProjectPaths.discover`` walks up from
                here until it finds ``manifest.yml`` (or hits the
                filesystem root).
            target_name: Optional explicit target name.  When ``None``
                the manifest's ``default_target`` is used (auto-derived
                for single-target manifests, per Phase 1A).

        Returns:
            ``(FSProjectPaths, FSManifest, FSTarget)`` triple — every
            downstream method threads the manifest target's
            ``database`` / ``schema`` / ``role`` and the connection's
            ``warehouse`` from there.

        Raises:
            CliError: When ``manifest.yml`` is not found, the requested
                target is not declared, the manifest is malformed, OR
                the active connection's account identifier does not
                match the resolved target's ``account_identifier``
                (D4 / L6-extension).
        """
        try:
            paths = FSProjectPaths.discover(from_dir)
        except ManifestNotFoundError as exc:
            raise CliError(
                f"Could not locate manifest.yml starting from "
                f"{Path(from_dir).resolve()!s}: {exc}"
            ) from exc

        try:
            manifest = FSManifest.load(paths.project_root)
        except (
            ManifestNotFoundError,
            Exception,
        ) as exc:
            if isinstance(exc, CliError):
                raise
            raise CliError(str(exc)) from exc

        try:
            target = manifest.get_effective_target(target_name)
        except Exception as exc:
            raise CliError(str(exc)) from exc

        # L6-extension / D4: refuse to operate when the active
        # connection's account ≠ the manifest target's
        # ``account_identifier``.  This check lives in the manager
        # (not the manifest loader) because account identity is a
        # connection-level concern.
        try:
            current_account = get_account_identifier(get_cli_context().connection)
        except Exception as exc:
            log.debug("Could not determine account identifier: %s", exc)
            current_account = None

        if current_account is not None and target.account_identifier:
            expected = AccountIdentifier.from_string(target.account_identifier)
            if current_account != expected:
                raise CliError(
                    f"Account mismatch: the manifest target "
                    f"'{target.name}' specifies account_identifier "
                    f"'{target.account_identifier}', but the active "
                    f"connection reports account '{current_account!s}'. "
                    f"Switch connection or update the manifest."
                )

        return paths, manifest, target

    def _target_info(self, target: FSTarget) -> dict[str, str]:
        """Return ``{target_database, target_schema, target_warehouse,
        target_name}`` for result envelopes.

        Per D2: ``database`` / ``schema`` come from the manifest
        target; ``warehouse`` always comes from the active connection
        (the manifest has no ``warehouse`` field).
        """
        ctx = get_cli_context()
        return {
            "target_database": target.database,
            "target_schema": target.schema,
            "target_warehouse": ctx.connection.warehouse or "",
            "target_name": target.name,
        }

    # ------------------------------------------------------------------
    # init — D6 auto-derive + init-exist fail-fast
    # ------------------------------------------------------------------

    def init(
        self,
        from_dir: Path,
        no_scaffold: bool = False,
    ) -> dict[str, Any]:
        """Initialize a feature-store project under *from_dir*.

        When *no_scaffold* is False (default) this method:

        1. Refuses to overwrite an existing ``manifest.yml`` (init-exist
           locked decision; no ``--force`` escape).
        2. Writes a default ``manifest.yml`` whose single ``DEFAULT``
           target is auto-derived from the active connection
           (``account_identifier`` / ``database`` / ``schema`` /
           ``role``).  ``warehouse`` is never written (D2).
        3. Creates ``sources/{entities,datasources,feature_views}/``
           and ``out/plan/.gitkeep`` so the plan-discovery directory is
           git-trackable but not pre-populated.
        4. Calls ``FeatureStore(..., creation_mode=CREATE_IF_NOT_EXIST)``
           to do the Snowflake-side schema / tag / metadata setup.

        With ``--no-scaffold`` every step above is skipped (manifest
        write, dir scaffolding, AND Snowflake-side init) — this is the
        escape hatch for operators driving ``init`` purely as a noop
        in test harnesses.

        Args:
            from_dir: Directory to initialise.  Becomes the project
                root.  Must exist.
            no_scaffold: When True, skip every side effect.

        Returns:
            ``{status, project_root, manifest_path, target}`` for a
            successful init; ``{status: "skipped"}`` for ``no_scaffold``.

        Raises:
            CliError: When ``manifest.yml`` is already present at
                ``<from_dir>/manifest.yml``.
        """
        ctx = get_cli_context()
        project_root = Path(from_dir).resolve()

        if no_scaffold:
            return {
                "status": "skipped",
                "project_root": str(project_root),
            }

        manifest_path = project_root / "manifest.yml"
        if manifest_path.exists():
            raise CliError(
                f"manifest.yml already exists at {manifest_path}. Refusing "
                f"to overwrite (init is fail-fast by design — there is no "
                f"--force escape)."
            )

        project_root.mkdir(parents=True, exist_ok=True)

        # Build the session BEFORE writing the manifest so we can ask
        # Snowflake for the canonical ``<ORG>-<ACCOUNT>`` identifier
        # (matching the format ``get_account_identifier`` returns at
        # apply time).  ``connection.account`` is the host-prefix form
        # operators put in ``connections.toml`` (e.g.
        # ``feature_store_vnext2``); writing that into ``manifest.yml``
        # would fail every L6 account-mismatch check.
        # Snowflake-side init: lazy-import to keep the heavy
        # snowml-core deps off the import path of plain ``init``.
        from snowflake.ml.feature_store.feature_store import (
            CreationMode,
            FeatureStore,
        )

        session = self._build_session()

        # Best-effort canonical form; fall back to ``connection.account``
        # if the round-trip fails so the scaffold still lands and the
        # operator can edit the manifest manually.
        try:
            account_identifier = str(get_account_identifier(ctx.connection))
        except Exception as exc:  # pragma: no cover — defensive
            log.debug(
                "Could not query canonical account identifier; "
                "falling back to connection.account: %s",
                exc,
            )
            account_identifier = str(ctx.connection.account or "")

        manifest_text = _render_default_manifest(
            account_identifier=account_identifier,
            database=str(ctx.connection.database or ""),
            schema=str(ctx.connection.schema or ""),
            role=str(ctx.connection.role or ""),
        )
        manifest_path.write_text(manifest_text)

        sources_root = project_root / "sources"
        for sub in ("entities", "datasources", "feature_views"):
            (sources_root / sub).mkdir(parents=True, exist_ok=True)

        plans_dir = project_root / "out" / "plan"
        plans_dir.mkdir(parents=True, exist_ok=True)
        (plans_dir / ".gitkeep").write_text("")
        FeatureStore(
            session,
            ctx.connection.database,
            ctx.connection.schema,
            ctx.connection.warehouse or "",
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )

        return {
            "status": "initialized",
            "project_root": str(project_root),
            "manifest_path": str(manifest_path),
            "target": _DEFAULT_MANIFEST_TARGET,
        }

    # ------------------------------------------------------------------
    # apply — L1–L7 plan-file lifecycle, relocated to out/plan/
    # ------------------------------------------------------------------

    def apply(
        self,
        from_dir: Path,
        target_name: Optional[str],
        plan_file: Optional[str],
        dev_mode: bool,
        allow_recreate: bool,
    ) -> dict[str, Any]:
        """Apply the discovered (or explicit) plan file.

        Apply is a *pure plan-file consumer*: either ``plan_file`` is
        given explicitly (the L7 escape hatch), or the manager
        auto-discovers the latest unapplied plan under
        ``<project_root>/out/plan/`` (L1–L4).  There is no
        "re-plan from source" branch.

        Operators preview changes via ``snow feature plan`` (which
        runs ``manager.plan`` + ``manager.write_plan``).

        Args:
            from_dir: Project-root start (manifest discovery walks up
                from here).
            target_name: Optional manifest target name.  ``None``
                resolves to the manifest's ``default_target``.
            plan_file: Optional explicit plan file (L7).  When given,
                auto-discovery is skipped but L6 (account +
                ``target_name`` match) still runs.
            dev_mode: Apply in dev-mode relaxed validation.
            allow_recreate: Permit recreate (drop-and-create) operations.

        Returns:
            Result dict with ``status`` / ``ops`` / ``executed`` /
            ``warnings`` / ``errors`` / ``plan_file`` plus
            connection-target metadata.
        """
        try:
            paths, _, target = self._resolve_project(from_dir, target_name)
        except CliError:
            # Explicit account mismatch → return as a structured
            # ``target_mismatch`` status so ``snow feature apply`` does
            # not exit the process abruptly (operators script around
            # the status string per ``docs/ARCHITECTURE.md`` Apply
            # Lifecycle table).
            ctx = get_cli_context()
            try:
                current_account = get_account_identifier(ctx.connection)
            except Exception:
                current_account = None
            return {
                "target_database": "",
                "target_schema": "",
                "target_warehouse": ctx.connection.warehouse or "",
                "target_name": target_name or "",
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"Account mismatch resolving manifest target "
                    f"{target_name or '(default)'!s}; "
                    f"connection reports {current_account!s}."
                ],
            }

        self._ensure_session_setup()

        if plan_file is not None:
            return self._apply_from_plan_file(
                plan_file=plan_file,
                target=target,
                requested_target_name=target_name,
                dev_mode=dev_mode,
                allow_recreate=allow_recreate,
            )

        # No explicit plan_file: discover the latest unapplied plan
        # under ``<project_root>/out/plan/`` (L1–L3).  If none exists,
        # return a structured ``no_plan`` result.
        discovered = self._discover_unapplied_plan(paths.plans_dir)
        if discovered is None:
            return {
                **self._target_info(target),
                "status": "no_plan",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"No unapplied plan file found under "
                    f"'{paths.plans_dir}' (out/plan/). Run "
                    f"`snow feature plan --from <dir>` first to "
                    f"generate a plan, then re-run apply."
                ],
            }
        return self._apply_from_plan_file(
            plan_file=discovered,
            target=target,
            requested_target_name=target_name,
            dev_mode=dev_mode,
            allow_recreate=allow_recreate,
        )

    # ------------------------------------------------------------------
    # _discover_unapplied_plan (private helper, L1–L3 invariants)
    # ------------------------------------------------------------------

    def _discover_unapplied_plan(self, plans_dir: Path) -> Optional[str]:
        """Return the path of the newest unapplied plan, or ``None``.

        Walks ``plans_dir`` (= ``<project_root>/out/plan/`` per D8) for
        files matching ``feature_plan_*.json`` whose suffix is
        ``.json`` (i.e. not ``.applied`` / ``.discarded``).  Sorts
        lexicographically — the ``YYYYMMDDTHHMMSS`` UTC timestamp
        embedded in the filename is monotonic at one-second resolution.

        Side effect (L3 — Discard-Older): when more than one unapplied
        plan exists, every plan except the newest is renamed to
        ``<name>.discarded`` *before* the function returns.  This keeps
        the plans directory in a normalised state — at most one
        unapplied plan when execution begins.

        Args:
            plans_dir: ``<project_root>/out/plan/``.

        Returns:
            Path string of the newest unapplied plan, or ``None`` if
            no candidates exist.
        """
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
        target: FSTarget,
        requested_target_name: Optional[str],
        dev_mode: bool,
        allow_recreate: bool,
    ) -> dict[str, Any]:
        """Execute a pre-computed plan loaded from a JSON plan file.

        Implements L4–L6 of the apply-lifecycle contract:

        - **L4 (Mark-Applied):** on successful execution, rename the
          plan file to ``<name>.applied``.
        - **L5 (Mark-Failed-Stays-Unapplied):** on execution failure,
          leave the plan file at its original name.
        - **L6 (Target-Match):** widened in Phase 3+4 (D4-ext): refuses
          a plan when ``plan.target_name`` ≠ the requested
          ``--target`` (the account-mismatch branch is checked one
          layer up by ``_resolve_project``).

        Per D2, the connection's warehouse is forwarded to
        ``execute_plan`` (plan files are warehouse-agnostic).

        Args:
            plan_file: Path to the plan JSON file to apply.
            target: Resolved manifest target (db / schema source).
            requested_target_name: The ``--target`` argument the
                operator supplied (``None`` if defaulted).  Used for
                the L6 ``target_name`` mismatch check.
            dev_mode: Apply dev-mode relaxed validation.
            allow_recreate: Permit recreate (drop-and-create) operations.

        Returns:
            Result dict with ``status`` / ``ops`` / ``executed`` /
            ``warnings`` / ``errors`` / ``plan_file``.  ``plan_file``
            is updated to the ``.applied`` path on success.
        """
        from snowflake.ml.feature_store.decl.types import PlanOptions

        plan_path = Path(plan_file)
        json_str = plan_path.read_text()
        pf = decl_api.deserialize_plan(json_str)
        plan = pf.plan
        plan_target_db = pf.target_database
        plan_target_schema = pf.target_schema
        plan_target_name = getattr(pf, "target_name", "") or ""

        ctx = get_cli_context()

        # L6 (Target-Match) — D4-ext: plan envelope ``target_name``
        # must match the requested ``--target`` (case-insensitive).
        # An empty plan ``target_name`` means "legacy / pre-D4-ext"
        # and is accepted unconditionally (apply --plan stays a usable
        # escape hatch for older plan files).
        if plan_target_name:
            requested = (requested_target_name or target.name or "").upper()
            if requested and requested != plan_target_name.upper():
                return {
                    **self._target_info(target),
                    "status": "target_mismatch",
                    "ops": [],
                    "executed": 0,
                    "warnings": [],
                    "errors": [
                        f"Plan was generated for target "
                        f"'{plan_target_name}' but apply was invoked "
                        f"with --target '{requested}'. Re-run plan or "
                        f"use --target {plan_target_name}."
                    ],
                    "plan_file": plan_file,
                }

        # L6 (Target-Match) — legacy shape: db/schema must also match
        # so the active connection's working schema collides with the
        # plan's ``compile_to_spec(...)`` results.
        if plan_target_db and plan_target_db != target.database:
            return {
                **self._target_info(target),
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"Plan was generated for database "
                    f"'{plan_target_db}' but the resolved manifest "
                    f"target points at '{target.database}'."
                ],
                "plan_file": plan_file,
            }
        if plan_target_schema and plan_target_schema != target.schema:
            return {
                **self._target_info(target),
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [
                    f"Plan was generated for schema "
                    f"'{plan_target_schema}' but the resolved "
                    f"manifest target points at '{target.schema}'."
                ],
                "plan_file": plan_file,
            }

        options = PlanOptions(
            dev_mode=dev_mode,
            allow_recreate=allow_recreate,
        )

        session = self._build_session()
        # D2 / Bug C: warehouse comes from the *active connection*,
        # not the plan file (plan files are warehouse-agnostic).
        result = decl_api.execute_plan(
            plan,
            session,
            database=target.database,
            schema=target.schema,
            warehouse=ctx.connection.warehouse or "",
            options=options,
        )

        # L4 (Mark-Applied): rename plan file to .applied on success.
        # L5 (Mark-Failed-Stays-Unapplied): keep the original name on
        # any non-"applied" status (or any exception, which propagates
        # naturally above this line).
        result_plan_file = plan_file
        if result.status == "applied":
            applied_path = plan_path.parent / (plan_path.name + ".applied")
            plan_path.rename(applied_path)
            result_plan_file = str(applied_path)

        return {
            **self._target_info(target),
            "status": result.status,
            "ops": result.ops,
            "executed": len([o for o in result.ops if o.get("status") == "success"]),
            "warnings": result.warnings,
            "errors": result.errors,
            "plan_file": result_plan_file,
        }

    # ------------------------------------------------------------------
    # plan
    # ------------------------------------------------------------------

    def plan(
        self,
        from_dir: Path,
        target_name: Optional[str],
        variables: Optional[Sequence[str]],
        dev_mode: bool,
        allow_recreate: bool,
        no_delete: bool = True,
    ) -> dict[str, Any]:
        """Render plan ops for ``snow feature plan`` (read-only).

        Validates specs against applied state, generates a plan, and
        returns a structured result for the terminal UI.  Does NOT
        generate SQL strings and does NOT execute anything against
        Snowflake.

        Steps:

        1. Resolve the manifest project + target.
        2. Prime the session.
        3. Fetch applied state via ``DESCRIBE … TYPE = SPECIFICATION``.
        4. Load the project via ``decl_api.load_project`` (manifest-aware
           sources walk).
        5. ``decl_api.resolve_datasource_columns`` to inject FV source
           column schemas.
        6. ``decl_api.validate_specs`` — short-circuit on ERROR.
        7. ``decl_api.generate_plan`` to compute the op stream.

        Args:
            from_dir: Project-root start.
            target_name: Optional manifest target name.
            variables: ``--variable key=value`` repeats.
            dev_mode: Apply dev-mode relaxed validation.
            allow_recreate: Permit recreate (drop-and-create) ops.
            no_delete: When True, deletion detection is disabled.

        Returns:
            ``{status, ops, executed, warnings, errors, target_*}``.
            ``status`` is ``"validation_failed"`` if validate_specs
            surfaced any ERROR severities, otherwise ``"ready"``.
        """
        from snowflake.ml.feature_store.decl.types import PlanOptions

        paths, _, target = self._resolve_project(from_dir, target_name)
        runtime_vars = _parse_variables(variables)

        self._ensure_session_setup()

        ctx = get_cli_context()
        sqls = decl_api.state_queries(target.database, target.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        specification_map = self._fetch_oft_state(raw_show, sqls)
        entity_rows = self._fetch_entity_rows(target)
        applied_state = decl_api.fetch_applied_state(
            raw_show,
            raw_tables,
            specification_map=specification_map,
            entity_rows=entity_rows,
            default_database=target.database,
            default_schema=target.schema,
        )

        batch = decl_api.load_project(
            paths.project_root,
            target=target,
            runtime_vars=runtime_vars or None,
        )
        decl_api.resolve_datasource_columns(batch)

        validation_results = decl_api.validate_specs(
            batch,
            applied_state,
            target_database=target.database,
            target_schema=target.schema,
            dev_mode=dev_mode,
        )
        errors = [r for r in validation_results if r.severity == "ERROR"]
        warnings = [r for r in validation_results if r.severity == "WARNING"]
        if errors:
            return {
                **self._target_info(target),
                "status": "validation_failed",
                "ops": [],
                "executed": 0,
                "warnings": [str(w) for w in warnings],
                "errors": [str(e) for e in errors],
            }

        options = PlanOptions(
            dev_mode=dev_mode,
            allow_recreate=allow_recreate,
            full_directory_mode=not no_delete,
        )
        plan = decl_api.generate_plan(
            batch,
            applied_state,
            options,
            database=target.database,
            schema=target.schema,
        )

        # Suppress unused-warning so the linter doesn't complain about
        # ``ctx`` (kept for symmetry with other entry-points and to
        # surface a connection in case downstream wiring is added).
        del ctx

        return {
            **self._target_info(target),
            "status": "ready",
            "ops": [
                {
                    "operation": op.kind.value,
                    # Preserve the spec's original-case name so the
                    # rendered plan UI, the on-disk JSON written by
                    # ``write_plan``, and the apply-time per-op
                    # rendering all share one canonical identifier.
                    "name": op.name,
                    "reason": op.reason,
                    "destructive": op.destructive,
                }
                for op in getattr(plan, "ops", [])
            ],
            "executed": 0,
            "warnings": [str(w) for w in warnings]
            + list(getattr(plan, "warnings", [])),
            "errors": [],
        }

    # ------------------------------------------------------------------
    # write_plan
    # ------------------------------------------------------------------

    def write_plan(
        self,
        from_dir: Path,
        target_name: Optional[str],
        variables: Optional[Sequence[str]],
        dev_mode: bool,
        out_path: Optional[str],
        no_delete: bool = True,
    ) -> str:
        """Generate a plan and write it as JSON.

        When *out_path* is ``None`` the plan lands under
        ``<project_root>/out/plan/feature_plan_<UTC ts>.json`` (D8
        relocated).  The serialised envelope carries ``target_name``
        so apply can later reject mismatched plans (D4-ext).

        Args:
            from_dir: Project-root start.
            target_name: Optional manifest target name.
            variables: ``--variable key=value`` repeats.
            dev_mode: Apply dev-mode relaxed validation.
            out_path: Destination path.  Parent directories are
                created automatically.  When ``None`` the default
                location under ``out/plan/`` is used.
            no_delete: When True, disable deletion detection.

        Returns:
            The absolute path to the written plan file.
        """
        from datetime import datetime as _dt

        from snowflake.ml.feature_store.decl.types import PlanOptions

        paths, _, target = self._resolve_project(from_dir, target_name)
        runtime_vars = _parse_variables(variables)

        self._ensure_session_setup()

        sqls = decl_api.state_queries(target.database, target.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        specification_map = self._fetch_oft_state(raw_show, sqls)
        entity_rows = self._fetch_entity_rows(target)
        applied_state = decl_api.fetch_applied_state(
            raw_show,
            raw_tables,
            specification_map=specification_map,
            entity_rows=entity_rows,
            default_database=target.database,
            default_schema=target.schema,
        )

        batch = decl_api.load_project(
            paths.project_root,
            target=target,
            runtime_vars=runtime_vars or None,
        )
        decl_api.resolve_datasource_columns(batch)

        options = PlanOptions(dev_mode=dev_mode, full_directory_mode=not no_delete)
        plan = decl_api.generate_plan(
            batch,
            applied_state,
            options,
            database=target.database,
            schema=target.schema,
        )

        source_files = sorted(
            str(p)
            for p in paths.sources_dir.rglob("*")
            if p.is_file() and p.suffix in (".yaml", ".yml", ".py", ".json")
        )
        json_str = decl_api.serialize_plan(
            plan,
            target.database,
            target.schema,
            source_files,
            target_name=target.name,
        )

        if out_path is None:
            ts = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
            paths.plans_dir.mkdir(parents=True, exist_ok=True)
            dest = paths.plans_dir / f"feature_plan_{ts}.json"
        else:
            dest = Path(out_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

        dest.write_text(json_str)
        return str(dest)

    # ------------------------------------------------------------------
    # list_specs
    # ------------------------------------------------------------------

    def list_specs(
        self,
        from_dir: Path,
        target_name: Optional[str],
    ) -> dict[str, Any]:
        """List deployed feature-store objects from Snowflake.

        Surfaces three object kinds in a single result:

        - ``FeatureView`` rows from ``SHOW ONLINE FEATURE TABLES``,
          enriched with the full spec JSON returned by ``DESCRIBE
          ONLINE FEATURE TABLE <name> TYPE = SPECIFICATION``.
        - ``Entity`` rows from
          ``SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'``.
        - ``Datasource`` rows derived from FV ``spec.sources[]``.
        """
        _, _, target = self._resolve_project(from_dir, target_name)
        self._ensure_session_setup()

        try:
            queries = decl_api.list_state_queries(target.database, target.schema)
            oft_rows = _rows_to_dicts(
                self.execute_query(queries["show_ofts"], cursor_class=DictCursor)
            )

            entity_rows = self._fetch_entity_rows(target)

            specification_map = self._fetch_oft_state(oft_rows, queries)

            enriched = decl_api.enrich_list_results(
                oft_rows,
                entity_rows=entity_rows,
                specification_map=specification_map,
            )
            return {
                **self._target_info(target),
                "source": "snowflake",
                "specs": enriched,
            }
        except Exception as exc:
            log.warning("list query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(
        self,
        from_dir: Path,
        target_name: Optional[str],
        name: str,
    ) -> dict[str, Any]:
        """Return metadata for a named feature view (resolves to OFT name)."""
        _, _, target = self._resolve_project(from_dir, target_name)
        self._ensure_session_setup()

        sqls = decl_api.state_queries(target.database, target.schema)
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
            if candidate.upper() == name.upper():
                oft_name = candidate
                break

        if not oft_name:
            return {
                "status": "error",
                "name": name,
                "error": f"{name}: not found in deployed feature views",
            }

        show_row = None
        for row in raw_show:
            if row.get("name", "") == oft_name:
                show_row = row
                break

        try:
            sql = decl_api.describe_query(oft_name, target.database, target.schema)
            rows = list(self.execute_query(sql, cursor_class=DictCursor))
            desc_rows = _rows_to_dicts(rows)
        except Exception as exc:
            log.warning("describe raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "name": name, "error": str(exc)}

        fv_name, version = _parse_oft_name(oft_name)

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

        examples: list[str] = []
        spec: Optional[dict[str, Any]] = None
        try:
            status = self.get_status()
            ingest_url = decl_api.get_service_endpoint(status, "ingest")
            query_url = decl_api.get_service_endpoint(status, "query")

            spec = self._find_spec(fv_name)
            source_name = fv_name
            if spec:
                sources = spec.get("sources", [])
                if sources and isinstance(sources, list) and sources[0].get("name"):
                    source_name = sources[0]["name"]
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
            "database": target.database,
            "schema": target.schema,
            "oft_name": oft_name,
            "entities": pk_cols,
            "rows": desc_rows,
        }
        if examples:
            result["examples"] = examples

        result["_display"] = decl_api.format_describe_display(
            fv_name=fv_name.lower(),
            version=version.lower(),
            database=target.database,
            schema=target.schema,
            oft_name=oft_name,
            entities=pk_cols,
            describe_rows=desc_rows,
            show_row=show_row,
            spec=spec,
            examples=examples,
        )

        return result

    @staticmethod
    def _find_spec(fv_name: str) -> Optional[dict[str, Any]]:
        """Try to find a local YAML spec for a feature view by name."""
        import glob as _glob

        try:
            import yaml
        except ImportError:
            return None

        search_dirs = [
            ".",
            "feature_views",
            "specs",
            "sources/feature_views",
            "example_store/feature_views",
        ]
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
        """Try to find a local datasource YAML by source name."""
        import glob as _glob

        try:
            import yaml
        except ImportError:
            return None

        search_dirs = [
            ".",
            "datasources",
            "sources",
            "sources/datasources",
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
        """Construct a Snowpark Session from the CLI's existing connection."""
        from snowflake.snowpark import Session

        return Session.builder.configs({"connection": self._conn}).create()

    def _get_feature_store(self, target: FSTarget) -> Any:
        """Return a snowml-core ``FeatureStore`` bound to the active connection.

        Constructs ``FeatureStore`` lazily with
        ``creation_mode=FAIL_IF_NOT_EXIST`` so the schema must already
        be initialised as a SnowML feature store.
        """
        self._ensure_session_setup()
        ctx = get_cli_context()
        session = self._build_session()
        from snowflake.ml.feature_store.feature_store import (
            CreationMode,
            FeatureStore,
        )

        return FeatureStore(
            session,
            target.database,
            target.schema,
            ctx.connection.warehouse or "",
            creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
        )

    def _fetch_oft_state(
        self,
        oft_rows: list[dict[str, Any]],
        state_sqls: dict[str, str],
    ) -> dict[str, dict[str, Any]]:
        """Fetch per-OFT spec JSON via ``DESCRIBE … TYPE = SPECIFICATION``."""
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

    def _fetch_entity_rows(self, target: FSTarget) -> list[dict[str, Any]]:
        """Fetch entity tag rows via the imperative ``list_entities()`` facade."""
        ctx = get_cli_context()
        try:
            session = self._build_session()
            return decl_api.fetch_entity_rows(
                session,
                target.database,
                target.schema,
                ctx.connection.warehouse or "",
            )
        except Exception as exc:
            log.debug("fetch_entity_rows failed (treating as empty): %s", exc)
            return []

    # ------------------------------------------------------------------
    # get_status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Query and parse the feature store runtime status.

        ``get_status`` is intentionally connection-only — it reads the
        active connection's database/schema rather than a manifest
        target.  Operators query the runtime status before manifest
        scaffolding (``snow feature online-service`` runs without a
        ``--from`` flag), so requiring a manifest here would be
        circular.
        """
        self._ensure_session_setup()
        ctx = get_cli_context()
        sqls = decl_api.service_sql(ctx.connection.database, ctx.connection.schema)
        try:
            rows = list(self.execute_query(sqls["get_status"]))
            raw = list(rows[0])[0] if rows else None
            if not raw:
                return {"status": "error", "error": "No response from system function"}
            result = decl_api.parse_service_status(raw)
            result["_user"] = ctx.connection.user or ""
            result["_database"] = ctx.connection.database or ""
            result["_schema"] = ctx.connection.schema or ""
            return result
        except Exception as exc:
            log.warning("get_status raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # initialize_service / destroy_service
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
            log.warning("show OFTs raised %s: %s", type(exc).__name__, exc)
            errors.append(f"show OFTs: {exc}")

        try:
            self.execute_query(sqls["drop"])
        except Exception as exc:
            log.warning("drop_runtime raised %s: %s", type(exc).__name__, exc)
            errors.append(f"drop_runtime: {exc}")

        return {"status": "destroyed", "dropped_ofts": dropped_ofts, "errors": errors}

    # ------------------------------------------------------------------
    # export_specs
    # ------------------------------------------------------------------

    def export_specs(
        self,
        from_dir: Path,
        target_name: Optional[str],
        output_dir: str,
    ) -> dict[str, Any]:
        """Export deployed feature-store objects as YAML spec files.

        Strict full-fidelity flow: prime the session, list OFTs, then
        fetch each OFT's full spec JSON via ``DESCRIBE … TYPE =
        SPECIFICATION`` through :meth:`_fetch_oft_state`.  Any per-OFT
        failure aborts the entire export — there is no column-DESCRIBE
        fallback.
        """
        _, _, target = self._resolve_project(from_dir, target_name)
        self._ensure_session_setup()

        eq = decl_api.export_queries(target.database, target.schema)

        show_rows = _rows_to_dicts(
            self.execute_query(eq["show_ofts"], cursor_class=DictCursor)
        )

        entity_rows = self._fetch_entity_rows(target)

        if not show_rows and not entity_rows:
            return {"status": "exported", "directory": "", "files": []}

        specification_map = self._fetch_oft_state(show_rows, eq) if show_rows else {}

        return decl_api.export_specs(
            show_rows,
            {},
            output_dir,
            target.database,
            target.schema,
            specification_map=specification_map,
            entity_rows=entity_rows,
        )

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------

    def ingest(
        self,
        from_dir: Path,
        target_name: Optional[str],
        source_name: str,
        records: List[dict],
    ) -> dict[str, Any]:
        """Stream records into a source via ``FeatureStore.stream_ingest``.

        Delegates the wire path (URL resolution, PAT auth,
        partial-success reporting) to snowml-core, but runs a
        client-side per-record schema preflight first.

        Args:
            from_dir: Project-root start.
            target_name: Optional manifest target name.
            source_name: Registered streaming source name.
            records: Non-empty list of row dicts; each row's keys must
                match the source schema exactly.

        Returns:
            ``{**target_info, "accepted_count": int}``.

        Raises:
            ValueError: When the preflight detects per-record key
                divergence from the registered schema.
        """
        _, _, target = self._resolve_project(from_dir, target_name)
        fs = self._get_feature_store(target)

        src = fs.get_stream_source(source_name)
        expected = {col.name for col in src.schema.fields}
        record_errors: list[str] = []
        for i, row in enumerate(records):
            keys = set(row.keys())
            missing = sorted(expected - keys)
            extra = sorted(keys - expected)
            if missing or extra:
                record_errors.append(
                    f"record {i}: missing={missing!r}, extra={extra!r}"
                )
        if record_errors:
            raise ValueError(
                f"Records do not match {source_name!r} schema "
                f"(expected={sorted(expected)!r}); " + "; ".join(record_errors)
            )

        accepted = fs.stream_ingest(source_name, records)
        return {**self._target_info(target), "accepted_count": accepted}

    # ------------------------------------------------------------------
    # query
    # ------------------------------------------------------------------

    def query(
        self,
        from_dir: Path,
        target_name: Optional[str],
        feature_view_name: str,
        version: str,
        keys: List[dict],
    ) -> dict[str, Any]:
        """Online-lookup features via ``FeatureStore.read_feature_view``.

        ``version`` is required because snowml-core's
        ``FeatureStore.get_feature_view(name, version)`` requires both
        when the feature view is referenced by string.

        Args:
            from_dir: Project-root start.
            target_name: Optional manifest target name.
            feature_view_name: Logical feature view name.
            version: FeatureView version (e.g. ``"V1"``).
            keys: List of dicts; each dict must carry every join-key
                column declared by the FV.

        Returns:
            ``{**target_info, "rows": list[dict]}``.

        Raises:
            ValueError: If an input dict is missing a declared join-key.
        """
        _, _, target = self._resolve_project(from_dir, target_name)
        fs = self._get_feature_store(target)
        fv = fs.get_feature_view(feature_view_name, version)
        join_key_order = [str(jk) for ent in fv.entities for jk in ent.join_keys]
        positional_keys = _to_positional_keys(keys, join_key_order)
        df = fs.read_feature_view(
            fv,
            keys=positional_keys,
            store_type="ONLINE",
            as_pandas=True,
        )
        return {**self._target_info(target), "rows": df.to_dict("records")}
