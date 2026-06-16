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
    # init — idempotent bootstrap that subsumes the old `export` command
    # ------------------------------------------------------------------

    def init(
        self,
        project_root: Path,
        target_name: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict[str, Any]:
        """Idempotent project bootstrap that pulls deployed artifacts.

        ``init`` always runs (in this fixed order) under *project_root*:

        1. Resolve target.  If ``manifest.yml`` is already present:
           load it and pick *target_name* (or the manifest's
           ``default_target``).  If absent: build a brand-new target
           using *target_name* (defaults to ``DEFAULT``) plus
           *database* / *schema* (defaults to the active connection's
           db/schema).
        2. Write ``manifest.yml`` only when it does NOT exist; the file
           is never overwritten.
        3. Scaffold ``sources/{entities,datasources,feature_views}/``
           and ``out/plan/.gitkeep`` (``mkdir -p`` semantics).
        4. Call ``FeatureStore(..., creation_mode=CREATE_IF_NOT_EXIST)``
           against the resolved target's db/schema to seed the
           Snowflake-side runtime.
        5. Run the export pipeline against the target's db/schema and
           write YAMLs into ``<project_root>/sources/{entities,
           datasources,feature_views}/`` (the manifest project tree).

        Re-running ``init`` is a noop on step 2 only — every other
        step re-runs idempotently so the on-disk view stays in sync
        with the deployed runtime.

        Args:
            project_root: Project-root directory.  The new Typer
                command always resolves this to ``Path.cwd()``.
            target_name: Optional manifest target name.  On a brand-new
                manifest this names the only target (default
                ``DEFAULT``).  On a re-init this picks which existing
                manifest target to export from (default = manifest's
                ``default_target``).
            database: Optional database override.  On a brand-new
                manifest the value is baked into the new target.  On a
                re-init the value MUST equal the resolved manifest
                target's stored ``database`` — a non-matching value
                raises :class:`CliError` (the manifest is the source
                of truth on re-init; mismatches are an authoring
                error, not a silent override).
            schema: Optional schema override.  Symmetric to *database*
                — baked into a fresh manifest, mismatch-rejected on a
                re-init.

        Returns:
            ``{status, project_root, manifest_path, target,
            manifest_written, export}`` envelope.  ``manifest_written``
            is ``False`` on a re-init.  ``export`` carries the
            envelope returned by ``decl_api.export_specs(...)``.

        Raises:
            CliError: When the manifest exists but cannot be parsed;
                when the resolved target points at a different
                Snowflake account than the active connection; or when
                a re-init receives ``--database`` / ``--schema``
                overrides that conflict with the resolved manifest
                target's stored values.
        """
        ctx = get_cli_context()
        project_root = Path(project_root).resolve()
        project_root.mkdir(parents=True, exist_ok=True)

        manifest_path = project_root / "manifest.yml"
        manifest_existed = manifest_path.exists()

        # Lazy-import snowml-core to keep the heavy deps off the
        # import path of users who never call init.
        from snowflake.ml.feature_store.feature_store import (
            CreationMode,
            FeatureStore,
        )

        if manifest_existed:
            # Re-init: the manifest is the source of truth.  Resolve
            # the requested target (or default).  ``--database`` /
            # ``--schema`` overrides are honoured only when they match
            # the resolved target's stored values; non-matching values
            # are an authoring error and we surface them as a
            # ``CliError`` directing the operator at the manifest
            # (the previous behaviour silently dropped the override,
            # which is the bug this branch fixes).
            _, _, target = self._resolve_project(project_root, target_name)
            if database is not None and database != target.database:
                raise CliError(
                    f"--database '{database}' conflicts with manifest "
                    f"target '{target.name}' database "
                    f"'{target.database}'. Edit manifest.yml or pick "
                    f"a different --target."
                )
            if schema is not None and schema != target.schema:
                raise CliError(
                    f"--schema '{schema}' conflicts with manifest "
                    f"target '{target.name}' schema "
                    f"'{target.schema}'. Edit manifest.yml or pick a "
                    f"different --target."
                )
            target_db = target.database
            target_sch = target.schema
            resolved_target_name = target.name
        else:
            # Fresh init: build a brand-new manifest from the active
            # connection, with optional --target / --database / --schema
            # overrides.
            resolved_target_name = target_name or _DEFAULT_MANIFEST_TARGET
            target_db = (
                database if database is not None else str(ctx.connection.database or "")
            )
            target_sch = (
                schema if schema is not None else str(ctx.connection.schema or "")
            )

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
                database=target_db,
                schema=target_sch,
                role=str(ctx.connection.role or ""),
                target=resolved_target_name,
            )
            manifest_path.write_text(manifest_text)

        # Steps 3 + 4 + 5 always re-run (idempotent).
        sources_root = project_root / "sources"
        for sub in ("entities", "datasources", "feature_views", "feature_groups"):
            (sources_root / sub).mkdir(parents=True, exist_ok=True)

        # ``feature_groups/`` is brand-new in v1 of FeatureGroup support and
        # tends to be empty on a fresh project (no FGs deployed yet).
        # Drop a ``.gitkeep`` so the directory survives ``git add`` and
        # ``snow feature plan`` always finds the loader subdir on disk
        # (the loader treats a missing directory as a noop, but a
        # tracked empty directory is closer to the existing
        # ``out/plan/.gitkeep`` pattern and avoids an "is this scaffolded?"
        # surprise on a peer's first pull).
        fg_gitkeep = sources_root / "feature_groups" / ".gitkeep"
        if not fg_gitkeep.exists():
            fg_gitkeep.write_text("")

        plans_dir = project_root / "out" / "plan"
        plans_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = plans_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.write_text("")

        session = self._build_session()
        FeatureStore(
            session,
            target_db,
            target_sch,
            ctx.connection.warehouse or "",
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )

        export_envelope = self._export_into_sources(
            project_root=project_root,
            database=target_db,
            schema=target_sch,
        )

        return {
            "status": "initialized",
            "project_root": str(project_root),
            "manifest_path": str(manifest_path),
            "target": resolved_target_name,
            "manifest_written": not manifest_existed,
            "export": export_envelope,
        }

    def _export_into_sources(
        self,
        *,
        project_root: Path,
        database: str,
        schema: str,
    ) -> dict[str, Any]:
        """Run the deployed-state export pipeline into ``<project_root>/sources/``.

        Routes init's export pass through the exact same applied-state
        surface the planner consumes
        (:func:`decl_api.fetch_applied_state` fed by ``state_queries``
        + ``_fetch_dt_text_map`` + ``_fetch_feature_view_rows`` +
        ``_fetch_feature_group_rows``).  The recovered
        :class:`AppliedState` is forwarded to
        :func:`decl_api.export_specs` via the ``applied_state=``
        kwarg, so:

        * BatchFV ``spec.sources[0].table`` (which the raw ``DESCRIBE
          … TYPE = SPECIFICATION`` JSON drops because snowml-core
          encodes the source binding in the offline Dynamic Table's
          ``SELECT … FROM …`` body, not the spec payload) is
          recovered from DT text by
          :func:`state._inject_batch_fv_source_from_dt_text` BEFORE
          the YAML is written — closing the cascade where a
          ``snow feature init`` round-trip emitted
          ``RECREATE_FV USER_CLICKS_FG_DECL`` on the next plan and
          then crashed with ``no resolvable source`` on apply.
        * Advanced BFV authoring knobs (``cluster_by`` /
          ``refresh_mode`` / ``initialize`` / ``storage_config`` /
          ``aggregation_secondary_keys``) recovered by
          :func:`state._inject_advanced_bfv_fields_from_dt_text` also
          land in the YAML.
        * Offline-only ``BatchFeatureView`` deployments (visible only
          via the imperative ``list_feature_views()`` facade, not
          ``SHOW ONLINE FEATURE TABLES``) are surfaced through
          :func:`state._build_offline_fv_object` and synthesised into
          show-rows inside ``export_specs`` so they reach the
          YAML-emission loop.

        Plan/init parity is the load-bearing invariant: every code path
        that needs to reason about deployed state goes through
        ``fetch_applied_state`` (R3 from
        ``docs/DEVELOPMENT_STANDARDS.md``: "the deployed runtime, the
        exporter, the loader, and the planner agree on every hash").

        Args:
            project_root: Project-root directory.  YAMLs are written
                under ``<project_root>/sources/{entities,datasources,
                feature_views,feature_groups}/``.
            database: Snowflake database to export from.
            schema: Snowflake schema to export from.

        Returns:
            The envelope returned by :func:`decl_api.export_specs`.
        """
        # Re-use the manifest target's db/schema to fetch entity tags
        # and the imperative-side FV / FG rows; the imperative facades
        # drop them onto the active connection's warehouse.  Build a
        # synthetic FSTarget so we don't have to discover the manifest
        # a second time.
        synthetic_target = FSTarget(
            name="__init_export__",
            account_identifier="",
            database=database,
            schema=schema,
        )
        # Build the local datasources-by-table lookup BEFORE the
        # export pass so the BFV source-binding recovery on the way
        # OUT (re-export over an existing project tree) preserves
        # the operator's authored logical ``BatchSource.name`` rather
        # than rewriting it to the recovered physical table identifier.
        # On a brand-new ``init`` (no manifest.yml on disk yet) the
        # helper returns ``{}``; the contract stays uniform across
        # plan / write_plan / init.  See ``docs/CHANGES.md`` →
        # "BFV source-name recovery via local datasources lookup".
        datasources_by_table = self._build_local_datasources_by_table(
            project_root,
            target=synthetic_target,
        )
        (
            show_rows,
            applied_state,
            entity_rows,
            feature_group_rows,
            specification_map,
        ) = self._fetch_applied_state_bundle(
            synthetic_target,
            datasources_by_table=datasources_by_table,
        )

        # Always delegate to ``decl_api.export_specs`` — it owns the
        # "empty input → noop envelope" semantics so the manager
        # doesn't second-guess the library boundary.
        #
        # ``specification_map`` is the per-OFT raw DESCRIBE payload
        # (same map ``fetch_applied_state`` consumed).  It is the
        # authoritative source for *non-FV-kind* OFTs such as the
        # ``FeatureView`` rows that back ``FeatureGroup`` registrations
        # — the ``applied_state`` overlay only handles the BFV / SFV /
        # RealtimeFV kinds, so without forwarding the raw spec map
        # those FG-backing OFT entries land in ``show_rows`` with no
        # spec and the exporter's strict-cutover check raises.  See
        # docs/CHANGES.md "Init export unified with applied-state
        # path" for the cascade analysis.
        return decl_api.export_specs(
            show_rows,
            {},
            str(project_root),
            database,
            schema,
            specification_map=specification_map,
            entity_rows=entity_rows,
            feature_group_rows=feature_group_rows,
            applied_state=applied_state,
            layout="sources",
        )

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

        # Init-first guard: fail fast against an uninitialised schema
        # with the actionable "run `snow feature init`" error before
        # touching any plan file or executing any DDL.
        self._assert_initialized(target)

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
            overwrite=allow_recreate,
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

        # Init-first guard: fail fast against an uninitialised schema
        # with the actionable "run `snow feature init`" error before
        # issuing any state SQL or running the loader.
        self._assert_initialized(target)

        ctx = get_cli_context()
        # Load the local project FIRST so the decl-side BatchFV
        # source-binding recovery can prefer the operator's authored
        # logical ``BatchSource.name`` over the recovered physical
        # table identifier.  Without this thread, every re-plan after
        # ``snow feature init`` trips ``MISSING_SOURCE`` because the
        # exported FV YAMLs reference table-as-name sources that the
        # local BatchSource YAML doesn't declare.  See
        # ``docs/CHANGES.md`` → "BFV source-name recovery via local
        # datasources lookup".
        batch = decl_api.load_project(
            paths.project_root,
            target=target,
            runtime_vars=runtime_vars or None,
        )
        datasources_by_table = decl_api.build_datasources_by_table(batch.specs)
        _, applied_state, _, _, _ = self._fetch_applied_state_bundle(
            target,
            datasources_by_table=datasources_by_table,
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

        # Load the local project FIRST so the BFV source-binding
        # recovery prefers the operator's authored logical names —
        # mirrors the same wiring in ``plan`` and ``init``.  Loading
        # the batch once and reusing it for both the lookup AND the
        # plan/validate pipeline keeps the IO cost flat.
        batch = decl_api.load_project(
            paths.project_root,
            target=target,
            runtime_vars=runtime_vars or None,
        )
        datasources_by_table = decl_api.build_datasources_by_table(batch.specs)
        _, applied_state, _, _, _ = self._fetch_applied_state_bundle(
            target,
            datasources_by_table=datasources_by_table,
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
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        _, _, target = self._resolve_project(from_dir, target_name)
        # Init-first guard: fail fast against an uninitialised schema
        # so `snow feature list` does not silently return an empty
        # specs list (which previously masked the "you forgot to run
        # `snow feature init`" diagnostic).
        self._assert_initialized(target)

        try:
            queries = decl_api.list_state_queries(target.database, target.schema)
            oft_rows = _rows_to_dicts(
                self.execute_query(queries["show_ofts"], cursor_class=DictCursor)
            )

            entity_rows = self._fetch_entity_rows(target)
            feature_group_rows = self._fetch_feature_group_rows(target)

            specification_map = self._fetch_oft_state(oft_rows, queries)

            enriched = decl_api.enrich_list_results(
                oft_rows,
                entity_rows=entity_rows,
                specification_map=specification_map,
                feature_group_rows=feature_group_rows,
            )
            return {
                **self._target_info(target),
                "source": "snowflake",
                "specs": enriched,
            }
        except FeatureStoreNotInitializedError:
            # Surface init-required as the operator-facing
            # "run snow feature init" error rather than burying it
            # in a status="error" envelope; the CLI wrapper renders
            # it as a top-level ``ClickException``.
            raise
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
        # Init-first guard: surface the actionable "run `snow feature
        # init`" error against an uninitialised schema instead of the
        # generic "not found in deployed feature views" envelope that
        # `describe` would otherwise return (because the SHOW OFTs
        # call against an uninit schema returns an empty list).
        self._assert_initialized(target)

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

        Constructs the ``FeatureStore`` via
        :func:`decl_api.assert_feature_store_initialized`, so the
        target schema must already carry the SnowML bootstrap tags
        (``SNOWML_FEATURE_STORE_OBJECT`` /
        ``SNOWML_FEATURE_VIEW_METADATA``).  When the tags are missing,
        the helper rewraps the snowml-core ``NOT_FOUND`` as
        :class:`decl_api.FeatureStoreNotInitializedError`, which the
        command-layer wrapper in ``commands.py`` converts into a
        ``ClickException`` directing the operator at
        ``snow feature init``.
        """
        ctx = get_cli_context()
        session = self._build_session()
        return decl_api.assert_feature_store_initialized(
            session,
            target.database,
            target.schema,
            ctx.connection.warehouse or "",
        )

    def _assert_initialized(self, target: FSTarget) -> None:
        """Init-first guard — every ``snow feature`` command except
        ``init`` calls this before issuing any read/write SQL.

        Constructs (and immediately discards) a
        ``FeatureStore(FAIL_IF_NOT_EXIST)`` via
        :func:`decl_api.assert_feature_store_initialized`.  On an
        uninitialised schema the helper raises
        :class:`decl_api.FeatureStoreNotInitializedError`, which the
        command-layer wrapper converts to a top-level
        ``ClickException`` carrying the "run ``snow feature init``"
        message.  On a healthy schema the call is a single
        ``SHOW TAGS`` round-trip (per snowml-core's
        ``_check_internal_objects_exist_or_throw``).

        Args:
            target: Resolved manifest target.  Database / schema /
                warehouse threading mirrors ``_get_feature_store``.

        Raises:
            decl_api.FeatureStoreNotInitializedError: When the target
                schema lacks the bootstrap feature-store tags.
        """
        ctx = get_cli_context()
        session = self._build_session()
        decl_api.assert_feature_store_initialized(
            session,
            target.database,
            target.schema,
            ctx.connection.warehouse or "",
        )

    def _build_local_datasources_by_table(
        self,
        project_root: Optional[Path],
        *,
        target: Optional[FSTarget] = None,
        runtime_vars: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Load the local project (when present) and build the
        ``{physical_table → logical BatchSource.name}`` lookup.

        Single entry point used by ``plan`` / ``write_plan`` / ``init``
        so the three callers cannot drift on the build-and-thread
        contract pinned by
        ``test_*_threads_datasources_by_table_into_fetch_applied_state``.

        Always returns a dict (possibly empty) so callers can pass the
        result straight to :meth:`_fetch_applied_state_bundle` without
        special-casing ``None``.  An empty dict carries the same
        meaning as ``None`` at the decl boundary (no local datasources
        available — cold-start contract).

        Args:
            project_root: Project-root directory.  When ``None`` (no
                project on disk yet, e.g. brand-new ``init``) the
                helper short-circuits to an empty map.
            target: Optional resolved manifest target — when provided,
                its ``database`` / ``schema`` are injected into the
                loaded specs the way the planner's load path does.
            runtime_vars: Optional ``{key: value}`` runtime variable
                bag (Jinja templating).  Mirrors the same dict
                ``plan`` / ``write_plan`` already pass to
                :func:`decl_api.load_project`.

        Returns:
            The lookup dict.  Empty when no project is loadable.
        """
        if project_root is None:
            return {}
        if not (project_root / "manifest.yml").exists():
            return {}
        try:
            batch = decl_api.load_project(
                project_root,
                target=target,
                runtime_vars=runtime_vars or None,
            )
        except Exception as exc:  # noqa: BLE001 — defensive: malformed local tree
            log.debug(
                "Local project load failed; falling back to empty "
                "datasources_by_table lookup: %s",
                exc,
            )
            return {}
        return decl_api.build_datasources_by_table(batch.specs)

    def _fetch_applied_state_bundle(
        self,
        target: FSTarget,
        *,
        datasources_by_table: Optional[dict[str, Any]] = None,
    ) -> tuple[
        list[dict[str, Any]],
        Any,
        list[dict[str, Any]],
        list[dict[str, Any]],
        dict[str, dict[str, Any]],
    ]:
        """Fetch the full applied-state bundle for *target*.

        Single source of truth for the SQL + imperative-facade
        sequence the planner, ``write_plan`` and ``init``'s export
        pass all consume.  Centralising it here keeps the three
        callers from drifting on which inputs feed
        :func:`decl_api.fetch_applied_state` — drift here was the
        original Bug A root cause (``init`` used the
        ``export_queries`` SQL set which has no
        ``show_dynamic_tables``, so BatchFV source-binding recovery
        never ran during the init-export pass).

        The bundle issues exactly the SQL ``plan`` previously issued
        inline:

        1. ``SHOW ONLINE FEATURE TABLES`` — online FV enumeration.
        2. ``SHOW TABLES`` — legacy structural-fingerprint fallback.
        3. ``SHOW DYNAMIC TABLES`` — DT text for BatchFV source +
           advanced-field recovery.
        4. ``DESCRIBE … TYPE = SPECIFICATION`` per OFT — full spec
           payload.
        5. ``FeatureStore.list_entities()`` — entity tags.
        6. ``FeatureStore.list_feature_views()`` — offline-only BFVs.
        7. ``FeatureStore.list_feature_groups()`` — FG rows.

        Args:
            target: Resolved manifest target the SQL is scoped to.
            datasources_by_table: Optional ``{physical_table →
                logical BatchSource.name}`` lookup, built by
                :func:`decl_api.build_datasources_by_table` from the
                local project's loaded specs.  Threaded into
                :func:`decl_api.fetch_applied_state` so the decl-side
                BatchFV source-binding recovery prefers the operator's
                authored logical source names over the recovered
                physical table identifiers.  ``None`` falls back to
                the legacy table-as-name behaviour (cold-start
                contract for fresh ``snow feature init``).  See
                ``docs/CHANGES.md`` → "BFV source-name recovery via
                local datasources lookup".

        Returns:
            Tuple ``(show_rows, applied_state, entity_rows,
            feature_group_rows, specification_map)``.  ``show_rows``
            is surfaced for callers that need the raw ``SHOW ONLINE
            FEATURE TABLES`` row set (e.g. the exporter wants both
            the raw rows and the recovered applied-state overlay).
            ``applied_state`` is the canonical :class:`AppliedState`
            snapshot.  ``specification_map`` is the per-OFT
            ``DESCRIBE … TYPE = SPECIFICATION`` payload that
            ``fetch_applied_state`` consumed — exposed for callers
            that need the *full* raw spec map (the exporter forwards
            it to :func:`decl_api.export_specs` so non-FV-kind OFTs
            such as the FeatureGroup-backing ones still resolve to
            an authoritative spec; the ``applied_state`` overlay only
            covers ``BatchFeatureView`` / ``StreamingFeatureView`` /
            ``RealtimeFeatureView`` kinds).
        """
        sqls = decl_api.state_queries(target.database, target.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )
        raw_tables = _rows_to_dicts(
            self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
        )
        specification_map = self._fetch_oft_state(raw_show, sqls)
        dt_text_map = self._fetch_dt_text_map(sqls)
        entity_rows = self._fetch_entity_rows(target)
        feature_view_rows = self._fetch_feature_view_rows(target)
        feature_group_rows = self._fetch_feature_group_rows(target)
        # Wave 3B / contract §8b: thread runtime-authoritative
        # ``FeatureStore.list_stream_sources()`` rows into
        # ``decl_api.fetch_applied_state`` so the planner's source-diff
        # branch sees ``Datasource(kind="StreamingSource")`` entries for
        # already-registered stream sources and emits NO_CHANGE rather
        # than the spurious CREATE_SOURCE noise that produced the
        # ``UserWarning: StreamSource <name> already exists. Skip
        # registration.`` symptom on apply.  The bundle contract's
        # return-tuple shape is preserved (rows are not surfaced).
        stream_source_rows = self._fetch_stream_source_rows(target)
        applied_state = decl_api.fetch_applied_state(
            raw_show,
            raw_tables,
            specification_map=specification_map,
            dt_text_map=dt_text_map,
            entity_rows=entity_rows,
            feature_view_rows=feature_view_rows,
            feature_group_rows=feature_group_rows,
            stream_source_rows=stream_source_rows,
            datasources_by_table=datasources_by_table,
            default_database=target.database,
            default_schema=target.schema,
        )
        return (
            raw_show,
            applied_state,
            entity_rows,
            feature_group_rows,
            specification_map,
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

    def _fetch_dt_text_map(
        self,
        state_sqls: dict[str, str],
    ) -> dict[str, str]:
        """Fetch ``SHOW DYNAMIC TABLES`` rows and project to ``{name: text}``.

        The deployed ``DESCRIBE ONLINE FEATURE TABLE … TYPE = SPECIFICATION``
        JSON for a ``BatchFeatureView`` returns an empty ``spec.sources``
        list — Snowflake doesn't round-trip the offline source-table
        binding through that surface.  The Dynamic Table backing each
        BatchFV does carry that binding in its ``CREATE DYNAMIC TABLE …
        AS SELECT … FROM <fqn>`` body, which ``SHOW DYNAMIC TABLES`` exposes
        as the ``text`` column.  The result is keyed by the unquoted DT
        name (matching ``oft_row['name']``) and threaded into
        :func:`decl_api.fetch_applied_state` so the planner sees a
        non-lossy BatchFV ``sources[0].table`` and can produce
        ``UPDATE_FV`` / ``RECREATE_FV`` correctly (see W-G(A) in the
        ``apply_lifecycle_resilience`` plan and docs/BATCH_FV_BUG_BASH.md
        §6–§8).

        Args:
            state_sqls: Output of :func:`decl_api.state_queries` for the
                resolved target.

        Returns:
            ``{dt_name: ddl_text}`` for every row returned by the
            ``show_dynamic_tables`` SQL with a non-empty ``name`` and
            ``text``.  Empty when the SQL is absent (older snowml-decl
            builds) or the query yields no rows.
        """
        sql = state_sqls.get("show_dynamic_tables")
        if not sql:
            return {}
        try:
            rows = _rows_to_dicts(self.execute_query(sql, cursor_class=DictCursor))
        except Exception as exc:  # noqa: BLE001 — defensive: missing privs / older accounts
            log.debug("Dynamic-table listing failed (treating as empty): %s", exc)
            return {}
        result: dict[str, str] = {}
        for row in rows:
            name = row.get("name") or ""
            text = row.get("text") or ""
            if isinstance(name, str) and isinstance(text, str) and name and text:
                result[name] = text
        return result

    def _fetch_entity_rows(self, target: FSTarget) -> list[dict[str, Any]]:
        """Fetch entity tag rows via the imperative ``list_entities()`` facade.

        Raises:
            decl_api.FeatureStoreNotInitializedError: When the target
                schema lacks the bootstrap feature-store tags.  The
                command-level wrapper in ``commands.py`` converts this
                into a ``ClickException`` whose message directs the
                operator at ``snow feature init``.
        """
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        ctx = get_cli_context()
        try:
            session = self._build_session()
            return decl_api.fetch_entity_rows(
                session,
                target.database,
                target.schema,
                ctx.connection.warehouse or "",
            )
        except FeatureStoreNotInitializedError:
            # Init-required is a first-class error — let it propagate
            # so the CLI handler renders the actionable message
            # instead of silently returning an empty list (which would
            # mask the bug and surface as a confusing "no entities"
            # downstream).
            raise
        except Exception as exc:
            log.debug("fetch_entity_rows failed (treating as empty): %s", exc)
            return []

    def _fetch_feature_view_rows(self, target: FSTarget) -> list[dict[str, Any]]:
        """Fetch feature-view rows via the imperative ``list_feature_views()``.

        Mirror of :func:`_fetch_entity_rows` for FeatureView
        enumeration.  The imperative ``FeatureStore.list_feature_views``
        path surfaces offline-only ``BatchFeatureView``s that
        ``SHOW ONLINE FEATURE TABLES`` cannot enumerate; without it
        the planner re-emits a spurious ``CREATE_FV`` after a
        successful offline-only apply.  See
        ``plans/offline_bfv_state_fix_b9da0006.plan.md``.

        Returns:
            A list of FV row dicts in the Phase-1 contract shape.
            Soft-fails with ``[]`` on any non-init-required error so
            ``snow feature plan`` does not regress on accounts that
            lack ``list_feature_views`` privileges.

        Raises:
            decl_api.FeatureStoreNotInitializedError: When the target
                schema lacks the bootstrap feature-store tags.  The
                command-level wrapper in ``commands.py`` converts this
                into a ``ClickException`` whose message directs the
                operator at ``snow feature init``.
        """
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        ctx = get_cli_context()
        try:
            session = self._build_session()
            return decl_api.fetch_feature_view_rows(
                session,
                target.database,
                target.schema,
                ctx.connection.warehouse or "",
            )
        except FeatureStoreNotInitializedError:
            raise
        except Exception as exc:
            log.debug("fetch_feature_view_rows failed (treating as empty): %s", exc)
            return []

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

        Returns:
            A list of stream-source row dicts in the shape produced
            by :func:`decl_api.fetch_stream_source_rows` (``name`` /
            ``schema`` / ``desc`` / ``owner``).  Soft-fails with
            ``[]`` on any non-init-required error so ``snow feature
            plan`` does not regress on accounts that lack
            ``list_stream_sources`` privileges.

        Raises:
            decl_api.FeatureStoreNotInitializedError: When the target
                schema lacks the bootstrap feature-store tags.  The
                command-level wrapper in ``commands.py`` converts this
                into a ``ClickException`` whose message directs the
                operator at ``snow feature init``.
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
        except Exception as exc:
            log.debug("fetch_stream_source_rows failed (treating as empty): %s", exc)
            return []

    def _fetch_feature_group_rows(self, target: FSTarget) -> list[dict[str, Any]]:
        """Fetch feature-group rows via the imperative ``list_feature_groups()``.

        Mirror of :func:`_fetch_feature_view_rows` for FeatureGroup
        enumeration.  ``FeatureGroup`` is a fully imperative-side
        construct: there is no ``SHOW FEATURE GROUPS`` SQL, so
        ``list_feature_groups()`` is the only path that surfaces
        deployed FGs into the applied-state snapshot.  Without this
        wiring the planner would re-emit a spurious ``CREATE_FG`` on
        every plan after a successful apply.

        Returns:
            A list of FG row dicts in the shape produced by
            :func:`decl_api.fetch_feature_group_rows`.  Soft-fails with
            ``[]`` on any non-init-required error so ``snow feature
            plan`` does not regress on accounts that lack
            ``list_feature_groups`` privileges.

        Raises:
            decl_api.FeatureStoreNotInitializedError: When the target
                schema lacks the bootstrap feature-store tags.  The
                command-level wrapper in ``commands.py`` converts this
                into a ``ClickException`` whose message directs the
                operator at ``snow feature init``.
        """
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        ctx = get_cli_context()
        try:
            session = self._build_session()
            return decl_api.fetch_feature_group_rows(
                session,
                target.database,
                target.schema,
                ctx.connection.warehouse or "",
            )
        except FeatureStoreNotInitializedError:
            raise
        except Exception as exc:
            log.debug("fetch_feature_group_rows failed (treating as empty): %s", exc)
            return []

    # ------------------------------------------------------------------
    # _resolve_service_target — manifest-or-connection for online-service
    # ------------------------------------------------------------------

    def _resolve_service_target(
        self,
        from_dir: Optional[Path],
        target_name: Optional[str],
    ) -> Tuple[str, str, Optional[str]]:
        """Resolve the ``(database, schema, role)`` for an
        ``online-service`` sub-command.

        Tries manifest discovery first via :meth:`_resolve_project`.
        Falls back to the active connection only when no manifest is
        reachable AND no explicit ``target_name`` was requested — this
        preserves the documented "operators run ``online-service``
        before/after manifest scaffolding" workflow without silently
        misrouting an explicit ``--target`` against a manifest-less
        directory.

        Args:
            from_dir: Project-root start (or ``None`` for ``cwd``).
            target_name: Optional manifest target name.  Passing this
                against a directory without ``manifest.yml`` is a hard
                error (the resolver re-raises ``CliError``).

        Returns:
            ``(database, schema, role)``.  ``role`` is the manifest
            target's ``role`` when set, falling back to
            ``ctx.connection.role``.

        Raises:
            CliError: Propagated from :meth:`_resolve_project` when
                manifest discovery fails (missing manifest, unknown
                target, account mismatch).  Only suppressed when both
                ``target_name`` is ``None`` AND the failure is the
                "manifest not found" kind — every other failure mode
                continues to propagate so the operator sees the real
                cause.
        """
        ctx = get_cli_context()
        start = Path(from_dir) if from_dir is not None else Path.cwd()
        try:
            _, _, target = self._resolve_project(start, target_name)
        except CliError:
            if target_name is not None:
                raise
            return (
                ctx.connection.database or "",
                ctx.connection.schema or "",
                ctx.connection.role,
            )
        return (
            target.database,
            target.schema,
            target.role or ctx.connection.role,
        )

    # ------------------------------------------------------------------
    # get_status
    # ------------------------------------------------------------------

    def get_status(
        self,
        from_dir: Optional[Path] = None,
        target_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Query and parse the feature store online-service status.

        The online service is bound to a specific ``<DB>.<SCHEMA>``
        location, so different manifest targets can run independent
        services in different states.  When *from_dir* points at a
        directory under (or equal to) a manifest project root, the
        named target (or ``default_target``) supplies the location;
        otherwise the active connection's database / schema is used.

        Args:
            from_dir: Project-root start.  ``None`` resolves to
                ``Path.cwd()``.
            target_name: Optional manifest target.  ``None`` resolves
                to the manifest's ``default_target`` when a manifest
                is reachable; against a manifest-less directory it
                triggers the connection fallback.  A non-``None``
                value against a manifest-less directory is a hard
                error surfaced through the ``{status: error}``
                envelope.

        Returns:
            Parsed status dict (or an ``{status: error}`` envelope on
            failure).
        """
        try:
            database, schema, _ = self._resolve_service_target(from_dir, target_name)
        except CliError as exc:
            return {"status": "error", "error": str(exc)}

        ctx = get_cli_context()
        sqls = decl_api.service_sql(database, schema)
        try:
            rows = list(self.execute_query(sqls["get_status"]))
            raw = list(rows[0])[0] if rows else None
            if not raw:
                return {"status": "error", "error": "No response from system function"}
            result = decl_api.parse_service_status(raw)
            result["_user"] = ctx.connection.user or ""
            result["_database"] = database
            result["_schema"] = schema
            return result
        except Exception as exc:
            log.warning("get_status raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # initialize_service / destroy_service
    # ------------------------------------------------------------------

    def initialize_service(
        self,
        from_dir: Optional[Path] = None,
        target_name: Optional[str] = None,
        producer_role: Optional[str] = None,
        consumer_role: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send CREATE runtime command. Returns immediately; caller polls.

        Args:
            from_dir: Project-root start (or ``None`` for ``cwd``).
            target_name: Optional manifest target.  ``None`` resolves
                to the manifest's ``default_target`` when reachable.
            producer_role: Optional explicit producer role override.
                Precedence: explicit > manifest ``target.role`` >
                ``ctx.connection.role``.
            consumer_role: Optional explicit consumer role override.
                Defaults to ``PUBLIC``.

        Returns:
            ``{status, message}`` envelope.  Status is ``RUNNING``
            (early-exit when the service is already up),
            ``CREATING`` (after a successful CREATE submission), or
            ``error`` (failure).
        """
        database, schema, resolved_role = self._resolve_service_target(
            from_dir, target_name
        )
        p_role = producer_role or resolved_role
        c_role = consumer_role or "PUBLIC"
        sqls = decl_api.service_sql(database, schema, p_role, c_role)
        location = f"{database}.{schema}"

        current = self.get_status(from_dir=from_dir, target_name=target_name)
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

    def destroy_service(
        self,
        from_dir: Optional[Path] = None,
        target_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Drop all OFTs then drop the feature store runtime.

        Args:
            from_dir: Project-root start (or ``None`` for ``cwd``).
            target_name: Optional manifest target.  ``None`` resolves
                to the manifest's ``default_target`` when reachable.

        Returns:
            ``{status: destroyed, dropped_ofts, errors}`` envelope.
        """
        database, schema, _ = self._resolve_service_target(from_dir, target_name)
        sqls = decl_api.service_sql(database, schema)

        dropped_ofts: list[str] = []
        errors: list[str] = []
        try:
            rows = list(self.execute_query(sqls["show_ofts"], cursor_class=DictCursor))
            for row in rows:
                name = row.get("name", "")
                if name:
                    try:
                        drop_sql = decl_api.drop_queries([name], database, schema)
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
