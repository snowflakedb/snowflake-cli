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

Snowflake-bound methods take ``from_dir`` and ``target_name``; the manifest
is the source of truth for ``database`` / ``schema`` / ``role`` /
``account_identifier``, but ``warehouse`` always comes from the active
connection. This module contains no SQL strings. See DESIGN.md for the
apply-lifecycle invariants under ``<project_root>/out/plan/``.
"""

from __future__ import annotations

import copy
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple

from snowflake.cli._plugins.connection.util import get_account_identifier
from snowflake.cli._plugins.feature.exceptions import (
    AccountMismatchError,
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.feature.models import (
    FSManifest,
    FSProjectPaths,
    FSTarget,
    as_path,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import AccountIdentifier
from snowflake.cli.api.project.util import identifier_to_str
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor

# ``decl_api`` comes from the optional ``snowflake-ml-python[feature_store]``
# library. Wrapped in try/except so the plugin still imports (and registers)
# when the library is absent; ``_require_snowflake_ml`` (in ``commands.py``)
# then surfaces one actionable error per command. Manifest parsing lives in
# this plugin and follows the DCM ``DCMManifest.load`` path (does not need the
# library).
#
# This binding is the **single source of truth** for library availability:
# ``decl_api`` is what every command path actually dereferences, so the
# missing-library check must key off *this* import — not a separate probe of
# ``decl.errors`` — or a partial install (``decl.api`` heavier than
# ``decl.errors``: it pulls the executor / types / enums) could pass preflight
# and then raise ``AttributeError: 'NoneType' object has no attribute
# 'load_project'`` instead of the actionable message. ``snowml_import_error``
# exposes the captured error so ``commands.py`` guards off one import.
try:
    from snowflake.ml.feature_store.decl import api as decl_api

    _SNOWML_IMPORT_ERROR: Optional[ImportError] = None
except ImportError as _exc:
    decl_api = None  # type: ignore[assignment]
    _SNOWML_IMPORT_ERROR = _exc


def snowml_import_error() -> Optional[ImportError]:
    """Return the ``decl_api`` import failure, or ``None`` when available.

    Single source of truth for the ``snowflake-ml-python[feature_store]``
    guard: it is ``None`` iff :data:`decl_api` imported successfully, so the
    CLI preflight and the manager can never disagree about availability.
    """
    return _SNOWML_IMPORT_ERROR


log = logging.getLogger(__name__)


# Per-row progress callback threaded from the CLI into the ``decl_api``
# fetch facades. Invoked as ``on_progress(completed, total, label)``: once
# as ``(0, total, "")`` when a listing returns, then once per processed row.
ProgressCallback = Callable[[int, int, str], None]


class _NullStateFetchProgress:
    """No-op progress handle used when output is structured / silent."""

    def begin_phase(self, phase_label: str, *, pre_counted: bool = False) -> None:
        return None

    def add_known(self, n: int) -> None:
        return None

    def callback(self, phase_label: str) -> Optional[ProgressCallback]:
        return None


class _RichStateFetchProgress:
    """Drive ONE cumulative, monotonic Rich task across the fetch phases.

    ``completed`` never resets between phases and the denominator is
    front-loaded so the bar never reads "done" while a slow listing is still
    in flight. Each phase reserves ``+1`` on the total until its count is
    known; a ``pre_counted`` phase (already added via ``add_known``) takes
    no reserve and is not re-added.
    """

    def __init__(self, progress: Any, task_id: Any) -> None:
        self._progress = progress
        self._task_id = task_id
        # Sum of the totals of phases that have fully completed.
        self._completed_base = 0
        # Sum of every phase total known so far (pre-seeds + returned counts).
        self._known_total = 0
        # The in-flight phase's total, or ``None`` until its listing returns.
        self._current_total: Optional[int] = None
        # ``1`` while an in-flight phase's count is still unknown, else ``0``;
        # keeps the denominator above ``completed`` during the collect.
        self._reserve = 0
        # Whether the in-flight phase's count was already added via
        # ``add_known`` (so its ``(0, n, "")`` must not add it again).
        self._pre_counted = False

    def _render_total(self) -> Optional[int]:
        total = self._known_total + self._reserve
        return total or None

    def add_known(self, n: int) -> None:
        # Pre-seed a future phase's count that is already known (the OFT
        # DESCRIBE count from ``SHOW ONLINE FEATURE TABLES``), so the
        # denominator is realistic before that phase runs.
        self._known_total += n
        self._progress.update(
            self._task_id,
            completed=self._completed_base,
            total=self._render_total(),
        )

    def begin_phase(self, phase_label: str, *, pre_counted: bool = False) -> None:
        if self._current_total is not None:
            # The previous phase is done; roll its count into the base so the
            # cumulative ``completed`` stays monotonic across phases.
            self._completed_base += self._current_total
            self._current_total = None
        self._pre_counted = pre_counted
        # A pre-counted phase is already in ``known_total`` (no reserve); a
        # normal phase reserves +1 until its ``(0, n, "")`` reveals the count.
        self._reserve = 0 if pre_counted else 1
        self._progress.update(
            self._task_id,
            completed=self._completed_base,
            total=self._render_total(),
            description=phase_label,
        )

    def callback(self, phase_label: str) -> ProgressCallback:
        def _on_progress(completed: int, total: int, label: str) -> None:
            if completed == 0:
                # This phase's total just became known (possibly 0).  Release
                # the in-flight reserve; add the count unless it was already
                # pre-seeded via ``add_known``.
                self._current_total = total
                self._reserve = 0
                if not self._pre_counted:
                    self._known_total += total
                self._progress.update(
                    self._task_id,
                    completed=self._completed_base,
                    total=self._render_total(),
                    description=phase_label,
                )
            else:
                # Keep the stable phase label as the description — the
                # per-item ``label`` (object name) is intentionally not
                # rendered: a long name changes width tick to tick and would
                # shift the bar's right edge around.
                self._progress.update(
                    self._task_id,
                    completed=self._completed_base + completed,
                    total=self._render_total(),
                    description=phase_label,
                )

        return _on_progress


@contextmanager
def _state_fetch_progress() -> Iterator[Any]:
    """Yield a progress handle for the plan/list state-fetch sequence.

    A no-op handle when ``silent`` (structured / ``--silent`` output);
    otherwise a transient Rich bar on ``sys.stderr``.
    """
    if get_cli_context().silent:
        yield _NullStateFetchProgress()
        return

    from rich.console import Console
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
    )

    console = Console(file=sys.stderr)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
        transient=True,
    ) as progress:
        task_id = progress.add_task("Loading feature store state", total=None)
        yield _RichStateFetchProgress(progress, task_id)


def _rows_to_dicts(rows) -> list[dict[str, Any]]:
    """Convert DictCursor rows to plain dicts for JSON serialization."""
    return [dict(r) for r in rows]


def _to_positional_keys(
    keys: list[dict[str, Any]],
    join_key_order: list[str],
) -> list[list[Any]]:
    """Translate dict-shaped keys to the positional list-of-lists shape.

    ``FeatureStore.read_feature_view`` wants value-lists ordered to match
    the FeatureView's join-key sequence. Raises ``ValueError`` when a row
    is missing a join key.
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
    """Parse ``--variable key=value`` repeats into a dict (empty when ``None``)."""
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

    Omits ``warehouse`` — the active connection is its sole source of truth.
    Unquoted identifier fields (database, schema, role) are folded to
    uppercase so the on-disk YAML matches ``FSTarget.from_dict``.
    """
    database = database.upper() if database else ""
    schema = schema.upper() if schema else ""
    role = role.upper() if role else ""
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
    # _resolve_project — manifest discovery + account match
    # ------------------------------------------------------------------

    def _resolve_project(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
    ) -> Tuple[FSProjectPaths, FSManifest, FSTarget]:
        """Resolve the project from *from_dir*, resolve the target, assert account match.

        Discovery matches DCM: *from_dir* (``--from`` / cwd) must itself
        contain ``manifest.yml``; ancestors are not searched, so a nested cwd
        never silently binds to a parent project's manifest. Manifest load
        then mirrors DCM: ``FSManifest.load(SecurePath(...))`` then
        ``get_effective_target``. Returns the
        ``(FSProjectPaths, FSManifest, FSTarget)`` triple that downstream
        methods thread. ``target_name=None`` uses the manifest's
        ``default_target``. Raises ``CliError`` when the manifest is missing
        / malformed or the target is undeclared, and the dedicated
        ``AccountMismatchError`` (a ``CliError`` subclass) when the
        connection's account does not match the target's ``account_identifier``
        so callers can distinguish the account guard from real manifest errors.
        """
        start = as_path(from_dir)
        try:
            paths = FSProjectPaths.discover(start)
            manifest = FSManifest.load(SecurePath(paths.project_root))
            target = manifest.get_effective_target(target_name)
        except ManifestNotFoundError as exc:
            raise CliError(
                f"Could not locate manifest.yml starting from "
                f"{start.resolve()!s}: {exc}"
            ) from exc
        except (InvalidManifestError, ManifestConfigurationError) as exc:
            raise CliError(str(exc)) from exc

        # Account-match guard: refuse to operate when the connection's
        # account differs from the target's ``account_identifier`` (this is a
        # connection-level concern, so it lives here not in the loader).
        try:
            current_account = get_account_identifier(get_cli_context().connection)
        except Exception as exc:
            log.debug("Could not determine account identifier: %s", exc)
            current_account = None

        if current_account is not None and target.account_identifier:
            try:
                expected = AccountIdentifier.from_string(target.account_identifier)
            except (TypeError, ValueError, AttributeError) as exc:
                raise CliError(
                    f"The manifest target "
                    f"'{sanitize_for_terminal(str(target.name))}' has an "
                    f"invalid account_identifier "
                    f"'{sanitize_for_terminal(str(target.account_identifier))}': "
                    f"{exc}"
                ) from exc
            if current_account != expected:
                raise AccountMismatchError(
                    f"Account mismatch: the manifest target "
                    f"'{sanitize_for_terminal(str(target.name))}' specifies "
                    f"account_identifier "
                    f"'{sanitize_for_terminal(str(target.account_identifier))}', "
                    f"but the active connection reports account "
                    f"'{sanitize_for_terminal(str(current_account))}'. "
                    f"Switch connection or update the manifest."
                )

        return paths, manifest, target

    def _target_info(self, target: FSTarget) -> dict[str, str]:
        """Return the ``target_*`` fields for result envelopes.

        ``database`` / ``schema`` come from the manifest target; ``warehouse``
        always comes from the active connection.
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
        project_root: Path | SecurePath,
        target_name: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        python: bool = True,
    ) -> dict[str, Any]:
        """Idempotent project bootstrap that pulls deployed artifacts.

        Resolves the target, scaffolds ``sources/`` + ``out/plan/``, seeds the
        runtime (``CREATE_IF_NOT_EXIST``), then writes ``manifest.yml`` when it
        does not already exist (never overwritten), and finally exports
        deployed objects as ``.py`` (or YAML when ``python=False``). The
        manifest is written only after the runtime bootstrap succeeds, so a
        failed init leaves no partial project state. Re-running is a no-op only
        on the manifest write; every other step re-runs so the on-disk view
        stays in sync with the deployed runtime.

        On a fresh init the resolved ``database`` / ``schema`` /
        ``account_identifier`` must be non-empty (from ``--database`` /
        ``--schema`` or the connection defaults) or ``init`` raises
        ``CliError`` before touching disk — a blank target would otherwise
        strand the project.

        On a re-init, ``database`` / ``schema`` overrides must equal the
        resolved manifest target's stored values or raise ``CliError`` (the
        manifest is the source of truth; mismatches are an authoring error).
        Returns an envelope with ``manifest_written`` and the ``export`` /
        ``warnings`` lifted from ``decl_api.export_specs(...)``.
        """
        ctx = get_cli_context()
        project_root = as_path(project_root).resolve()
        project_root.mkdir(parents=True, exist_ok=True)

        manifest_path = project_root / "manifest.yml"
        manifest_existed = manifest_path.exists()
        # Rendered on a fresh init and persisted only *after* the runtime
        # bootstrap succeeds (see below), so a failed init leaves no partial
        # project state.
        pending_manifest_text: Optional[str] = None

        # Lazy-import to keep the heavy deps off the module import path.
        from snowflake.ml.feature_store.feature_store import (
            CreationMode,
            FeatureStore,
        )

        if manifest_existed:
            # Re-init: the manifest is the source of truth. Overrides are
            # honoured only when they match the resolved target; otherwise
            # ``CliError`` directs the operator at the manifest.
            _, _, target = self._resolve_project(project_root, target_name)
            if database is not None and database.upper() != target.database.upper():
                raise CliError(
                    f"--database '{sanitize_for_terminal(str(database))}' "
                    f"conflicts with manifest target "
                    f"'{sanitize_for_terminal(str(target.name))}' database "
                    f"'{sanitize_for_terminal(str(target.database))}'. Edit "
                    f"manifest.yml or pick a different --target."
                )
            if schema is not None and schema.upper() != target.schema.upper():
                raise CliError(
                    f"--schema '{sanitize_for_terminal(str(schema))}' "
                    f"conflicts with manifest target "
                    f"'{sanitize_for_terminal(str(target.name))}' schema "
                    f"'{sanitize_for_terminal(str(target.schema))}'. Edit "
                    f"manifest.yml or pick a different --target."
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

            # Fail closed before touching disk: a blank ``database`` /
            # ``schema`` / ``account_identifier`` would persist an invalid
            # target (e.g. ``database: ""``). Because ``manifest_existed`` is
            # then True on the next run, the fresh-init branch never re-runs,
            # so ``--database X`` raises a conflict and the project is
            # unrecoverable without hand-editing the file. Treat stripped-empty
            # values (missing connection defaults, whitespace-only overrides)
            # as missing.
            missing = [
                field
                for field, value in (
                    ("database", target_db),
                    ("schema", target_sch),
                    ("account_identifier", account_identifier),
                )
                if not value.strip()
            ]
            if missing:
                raise CliError(
                    "Cannot initialize a feature-store project: no value for "
                    f"{', '.join(missing)}. Pass --database / --schema or set "
                    "the connection's default database, schema, and account, "
                    "then re-run 'snow feature init'."
                )

            target_db = target_db.upper()
            target_sch = target_sch.upper()

            pending_manifest_text = _render_default_manifest(
                account_identifier=account_identifier,
                database=target_db,
                schema=target_sch,
                role=str(ctx.connection.role or ""),
                target=resolved_target_name,
            )

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
            SecurePath(fg_gitkeep).write_text("")

        plans_dir = project_root / "out" / "plan"
        plans_dir.mkdir(parents=True, exist_ok=True)
        gitkeep = plans_dir / ".gitkeep"
        if not gitkeep.exists():
            SecurePath(gitkeep).write_text("")

        session = self._build_session()
        FeatureStore(
            session,
            target_db,
            target_sch,
            ctx.connection.warehouse or "",
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )

        # Persist the manifest only after the runtime bootstrap succeeds so a
        # failed init leaves no partial state. Re-init never rewrites the
        # existing manifest (``pending_manifest_text`` stays None).
        if pending_manifest_text is not None:
            SecurePath(manifest_path).write_text(pending_manifest_text)

        export_envelope = self._export_into_sources(
            project_root=project_root,
            database=target_db,
            schema=target_sch,
            python=python,
        )

        return {
            "status": "initialized",
            "project_root": str(project_root),
            "manifest_path": str(manifest_path),
            "target": resolved_target_name,
            "manifest_written": not manifest_existed,
            "export": export_envelope,
            "warnings": list(export_envelope.get("warnings", [])),
        }

    def _export_into_sources(
        self,
        *,
        project_root: Path | SecurePath,
        database: str,
        schema: str,
        python: bool = True,
        name_filter: Optional[str] = None,
    ) -> dict[str, Any]:
        """Run the deployed-state export pipeline into ``<project_root>/sources/``.

        Routes init's export through the same ``fetch_applied_state`` surface
        the planner consumes — the load-bearing plan/init parity invariant so
        the runtime, exporter, loader, and planner agree on every hash (which
        is what recovers BFV source bindings and offline-only FVs that the raw
        ``DESCRIBE`` spec drops). Emits ``.py`` when ``python`` else YAML.
        """
        project_root = as_path(project_root)
        # Synthetic target so we don't re-discover the manifest just to fetch
        # entity tags and the imperative FV / FG rows.
        synthetic_target = FSTarget(
            name="__init_export__",
            account_identifier="",
            database=database,
            schema=schema,
        )
        # Build the local datasources-by-table lookup first so BFV
        # source-binding recovery preserves the operator's authored logical
        # ``BatchSource.name`` instead of the recovered physical table.
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

        # ``specification_map`` (the raw per-OFT DESCRIBE payload) must be
        # forwarded because the ``applied_state`` overlay only covers the
        # BFV / SFV / RealtimeFV kinds; without it the FG-backing OFT entries
        # reach the exporter with no spec and its strict-cutover check raises.
        _export_fn = (
            decl_api.export_specs_as_python if python else decl_api.export_specs
        )
        return _export_fn(
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
            name_filter=name_filter,
        )

    # ------------------------------------------------------------------
    # sync — pull deployed objects into sources/ (no bootstrap)
    # ------------------------------------------------------------------

    def sync(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        name_filter: Optional[str],
        python: bool,
    ) -> dict[str, Any]:
        """Pull deployed feature-store objects into the local sources tree.

        ``sync`` is ``init``'s export phase without the Snowflake-side
        bootstrap; it requires an existing ``manifest.yml`` and an
        initialised feature store. ``name_filter`` (exact, case-insensitive)
        limits the export to one object; ``python=False`` emits YAML. Raises
        ``CliError`` / ``FeatureStoreNotInitializedError`` like the other
        manifest-bound methods.
        """
        paths, _, target = self._resolve_project(from_dir, target_name)
        target_db = target.database
        target_sch = target.schema
        ctx = get_cli_context()
        session = self._build_session()
        decl_api.assert_feature_store_initialized(
            session,
            target_db,
            target_sch,
            ctx.connection.warehouse or "",
        )
        export_envelope = self._export_into_sources(
            project_root=paths.project_root,
            database=target_db,
            schema=target_sch,
            python=python,
            name_filter=name_filter,
        )
        if name_filter and not export_envelope.get("files"):
            raise CliError(
                f"No deployed object named "
                f"'{sanitize_for_terminal(str(name_filter))}' found in"
                f" {sanitize_for_terminal(str(target_db))}."
                f"{sanitize_for_terminal(str(target_sch))}."
                " Run `snow feature list` to see available objects."
            )
        return {
            "status": "synced",
            "files": export_envelope.get("files", []),
            "directory": export_envelope.get("directory", ""),
            "target_database": target_db,
            "target_schema": target_sch,
            "warnings": list(export_envelope.get("warnings", [])),
        }

    # ------------------------------------------------------------------
    # apply — L1–L7 plan-file lifecycle, relocated to out/plan/
    # ------------------------------------------------------------------

    def apply(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        plan_file: Optional[str],
        destructive: bool,
    ) -> dict[str, Any]:
        """Apply the discovered (or explicit) plan file.

        A pure plan-file consumer: uses an explicit ``plan_file`` or
        auto-discovers the latest unapplied plan under
        ``<project_root>/out/plan/``. There is no "re-plan from source" branch.
        """
        try:
            paths, _, target = self._resolve_project(from_dir, target_name)
        except AccountMismatchError as exc:
            # Only a *true* account mismatch becomes a structured
            # ``target_mismatch`` status (operators script on L6); real
            # manifest errors (missing file, malformed YAML, unknown target)
            # keep propagating with their own message via ``_resolve_project``.
            ctx = get_cli_context()
            return {
                "target_database": "",
                "target_schema": "",
                "target_warehouse": ctx.connection.warehouse or "",
                "target_name": target_name or "",
                "status": "target_mismatch",
                "ops": [],
                "executed": 0,
                "warnings": [],
                "errors": [str(exc)],
            }

        # Init-first guard: fail fast against an uninitialised schema before
        # touching any plan file or DDL.
        self._assert_initialized(target)

        if plan_file is not None:
            return self._apply_from_plan_file(
                plan_file=plan_file,
                target=target,
                requested_target_name=target_name,
                destructive=destructive,
            )

        # No explicit plan_file: discover the latest unapplied plan, or
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
            destructive=destructive,
        )

    # ------------------------------------------------------------------
    # _discover_unapplied_plan (private helper, L1–L3 invariants)
    # ------------------------------------------------------------------

    def _discover_unapplied_plan(self, plans_dir: Path) -> Optional[str]:
        """Return the path of the newest unapplied plan, or ``None``.

        Considers ``feature_plan_*.json`` (not ``.applied`` / ``.discarded``),
        sorted lexicographically on the embedded UTC timestamp. Side effect:
        every plan but the newest is renamed to ``<name>.discarded`` so at
        most one unapplied plan remains when execution begins.
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
        destructive: bool,
    ) -> dict[str, Any]:
        """Execute a pre-computed plan loaded from a JSON plan file.

        Renames the plan to ``.applied`` on success (stays unapplied on
        failure), and refuses a plan whose ``target_name`` does not match the
        requested ``--target``. The connection's warehouse is forwarded to
        ``execute_plan`` (plan files are warehouse-agnostic).
        """
        from snowflake.ml.feature_store.decl.types import PlanOptions

        plan_path = Path(plan_file)
        json_str = SecurePath(plan_file).read_text(
            file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB
        )
        pf = decl_api.deserialize_plan(json_str)
        plan = pf.plan
        plan_target_db = pf.target_database
        plan_target_schema = pf.target_schema
        plan_target_name = getattr(pf, "target_name", "") or ""

        ctx = get_cli_context()

        # Target-match (case-insensitive); an empty plan ``target_name`` is
        # legacy and accepted unconditionally so ``apply --plan`` still works
        # for older plan files.
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
        if plan_target_db and plan_target_db.upper() != target.database.upper():
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
        if plan_target_schema and plan_target_schema.upper() != target.schema.upper():
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
            allow_recreate=destructive,
            overwrite=destructive,
        )

        session = self._build_session()
        # Warehouse comes from the *active connection*,
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

    def _compute_plan(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        variables: Optional[Sequence[str]],
        destructive: bool,
        no_delete: bool = True,
    ) -> tuple[dict[str, Any], Any]:
        """Run the load + fetch + validate + generate pipeline exactly once.

        Shared core of :meth:`plan` and :meth:`write_plan` so a single
        ``snow feature plan`` invocation pays the per-object ``DESCRIBE`` cost
        (and renders the state-fetch progress bar) once, and the ops table the
        operator sees is the *same* ``Plan`` object serialized to
        ``out/plan/``.  Returns ``(envelope, plan)`` where ``envelope`` is the
        JSON-serializable UI dict and ``plan`` is the generated ``Plan`` — or
        ``None`` when the envelope status is ``"validation_failed"`` (validate
        or planner errors), so callers must never serialize an error plan.
        """
        from snowflake.ml.feature_store.decl.types import PlanOptions

        paths, _, target = self._resolve_project(from_dir, target_name)
        runtime_vars = _parse_variables(variables)

        # Init-first guard: fail fast against an uninitialised schema before
        # any state SQL or the loader.
        self._assert_initialized(target)

        ctx = get_cli_context()
        # Load the local project FIRST so BFV source-binding recovery prefers
        # the operator's authored logical ``BatchSource.name``; otherwise a
        # re-plan after ``init`` trips ``MISSING_SOURCE`` on the exported
        # table-as-name sources.
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
        )
        errors = [r for r in validation_results if r.severity == "ERROR"]
        warnings = [r for r in validation_results if r.severity == "WARNING"]
        if errors:
            return (
                {
                    **self._target_info(target),
                    "status": "validation_failed",
                    "ops": [],
                    "executed": 0,
                    "warnings": warnings,
                    "errors": errors,
                },
                None,
            )

        options = PlanOptions(
            allow_recreate=destructive,
            full_directory_mode=not no_delete,
        )
        plan = decl_api.generate_plan(
            batch,
            applied_state,
            options,
            database=target.database,
            schema=target.schema,
        )

        del ctx  # kept above for symmetry with other entry-points

        # Planner-side blocking errors surface like validate_specs ERRORs (no
        # ops, no plan file). ``getattr`` keeps envelopes written before
        # ``Plan.errors`` existed working.
        plan_errors = getattr(plan, "errors", None)
        if not isinstance(plan_errors, list):
            plan_errors = []
        if plan_errors:
            return (
                {
                    **self._target_info(target),
                    "status": "validation_failed",
                    "ops": [],
                    "executed": 0,
                    "warnings": warnings + list(getattr(plan, "warnings", [])),
                    "errors": plan_errors,
                },
                None,
            )

        envelope = {
            **self._target_info(target),
            "status": "ready",
            # ``format_op_display_row`` leads each row with ``type`` (matching
            # ``snow feature list``) and preserves the original-case ``name``
            # so plan UI, plan JSON, and apply share one identifier.
            "ops": [
                decl_api.format_op_display_row(op) for op in getattr(plan, "ops", [])
            ],
            "executed": 0,
            "warnings": warnings + list(getattr(plan, "warnings", [])),
            "errors": [],
        }
        return envelope, plan

    def plan(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        variables: Optional[Sequence[str]],
        destructive: bool,
        no_delete: bool = True,
    ) -> tuple[dict[str, Any], Any]:
        """Render plan ops for ``snow feature plan`` (read-only).

        Validates specs against applied state and generates a plan for the
        terminal UI; issues no SQL and executes nothing. ``status`` is
        ``"validation_failed"`` when validation or the planner surface any
        ERROR, otherwise ``"ready"``.

        Returns ``(envelope, plan)``.  The caller writes ``out/plan/`` from the
        returned ``plan`` via :meth:`write_plan_object` so the displayed ops and
        the persisted JSON come from one generate (no double ``DESCRIBE``, no
        TOCTOU gap between the shown table and the applied file).  ``plan`` is
        ``None`` on ``validation_failed``.
        """
        return self._compute_plan(
            from_dir,
            target_name,
            variables,
            destructive,
            no_delete,
        )

    # ------------------------------------------------------------------
    # write_plan
    # ------------------------------------------------------------------

    def write_plan_object(
        self,
        plan: Any,
        *,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        out_path: Optional[str],
    ) -> str:
        """Serialize an already-generated ``plan`` to JSON; returns the path.

        Serialize-only: no ``load_project``, no ``_fetch_applied_state_bundle``,
        no ``generate_plan``.  The ``plan`` is the object :meth:`plan` returned,
        so the persisted ``out/plan/`` file is exactly the plan whose ops the
        operator saw.  Defaults to
        ``<project_root>/out/plan/feature_plan_<UTC ts>.json``; the envelope
        carries ``target_name`` so apply can reject mismatches.
        """
        paths, _, target = self._resolve_project(from_dir, target_name)

        # Never persist a plan that carries blocking planner errors (e.g.
        # FG_MEMBER_STILL_REFERENCED).  ``getattr`` + isinstance keeps older
        # plan envelopes (no ``errors`` field) working.
        plan_errors = getattr(plan, "errors", None)
        if not isinstance(plan_errors, list):
            plan_errors = []
        if plan_errors:
            raise CliError(
                "Refusing to write plan: " + "; ".join(str(e) for e in plan_errors)
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
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            paths.plans_dir.mkdir(parents=True, exist_ok=True)
            dest = paths.plans_dir / f"feature_plan_{ts}.json"
        else:
            dest = Path(out_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

        SecurePath(dest).write_text(json_str)
        return str(dest)

    def write_plan(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        variables: Optional[Sequence[str]],
        out_path: Optional[str],
        no_delete: bool = True,
    ) -> str:
        """Generate a plan and write it as JSON; returns the written path.

        Convenience for callers that do not already hold a ``Plan`` (tests,
        scripts): generate once via :meth:`_compute_plan`, then serialize via
        :meth:`write_plan_object`.  Unlike ``snow feature plan`` (which calls
        :meth:`plan` + :meth:`write_plan_object` so it pays a single fetch),
        this runs its own generate.  Raises ``CliError`` on
        ``validation_failed`` so an error plan is never persisted.
        """
        envelope, plan = self._compute_plan(
            from_dir,
            target_name,
            variables,
            destructive=False,
            no_delete=no_delete,
        )
        if plan is None:
            findings = envelope.get("errors") or []
            raise CliError(
                "Refusing to write plan: " + "; ".join(str(e) for e in findings)
            )
        return self.write_plan_object(
            plan,
            from_dir=from_dir,
            target_name=target_name,
            out_path=out_path,
        )

    # ------------------------------------------------------------------
    # list_specs
    # ------------------------------------------------------------------

    def list_specs(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
    ) -> dict[str, Any]:
        """List deployed feature-store objects from Snowflake.

        Surfaces FeatureView (including offline-only BFVs), Entity, and
        Datasource rows in a single enriched result.
        """
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        _, _, target = self._resolve_project(from_dir, target_name)
        # Init-first guard so ``list`` surfaces the "run init" diagnostic
        # instead of silently returning an empty specs list.
        self._assert_initialized(target)

        try:
            queries = decl_api.list_state_queries(target.database, target.schema)
            # Progress bar on stderr, TABLE output only (see
            # ``_state_fetch_progress``); each fetch seam gets a phase callback.
            with _state_fetch_progress() as progress:
                oft_rows = _rows_to_dicts(
                    self.execute_query(queries["show_ofts"], cursor_class=DictCursor)
                )
                # Front-load the denominator: the OFT DESCRIBE phase runs last
                # but its count (one DESCRIBE per SHOW row) is already known, so
                # the bar reads mid-flight during the slow collect.
                progress.add_known(len(oft_rows))

                progress.begin_phase("Loading entities")
                entity_rows = self._fetch_entity_rows(
                    target, on_progress=progress.callback("Loading entities")
                )
                progress.begin_phase("Loading feature groups")
                feature_group_rows = self._fetch_feature_group_rows(
                    target,
                    on_progress=progress.callback("Loading feature groups"),
                )
                # ``list_feature_views()`` is the authoritative FV set; it also
                # surfaces OFT-less (``online: false``) BFVs that
                # ``SHOW ONLINE FEATURE TABLES`` never returns.
                progress.begin_phase("Loading feature views")
                feature_view_rows = self._fetch_feature_view_rows(
                    target,
                    on_progress=progress.callback("Loading feature views"),
                )

                # Pre-counted: its count was added via ``add_known`` above, so
                # its ``(0, n, "")`` must not add it a second time.
                progress.begin_phase("Loading online feature tables", pre_counted=True)
                specification_map = self._fetch_oft_state(
                    oft_rows,
                    queries,
                    on_progress=progress.callback("Loading online feature tables"),
                )

            enriched = decl_api.enrich_list_results(
                oft_rows,
                entity_rows=entity_rows,
                specification_map=specification_map,
                feature_group_rows=feature_group_rows,
                feature_view_rows=feature_view_rows,
            )
            return {
                **self._target_info(target),
                "source": "snowflake",
                "specs": enriched,
            }
        except FeatureStoreNotInitializedError:
            # Let init-required propagate as the operator-facing error rather
            # than burying it in a status="error" envelope.
            raise
        except Exception as exc:
            log.warning("list query raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        name: str,
        version: Optional[str] = None,
    ) -> dict[str, Any]:
        """Return metadata for a named feature view (resolves to OFT name)."""
        paths, _, target = self._resolve_project(from_dir, target_name)
        # Init-first guard: surface "run init" instead of the generic
        # "not found" envelope an empty SHOW OFTs would otherwise produce.
        self._assert_initialized(target)

        sqls = decl_api.state_queries(target.database, target.schema)
        raw_show = _rows_to_dicts(
            self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
        )

        # Resolution + version disambiguation live in decl (which owns the
        # `<base>$<version>$ONLINE` naming convention); the CLI stays thin.
        oft_name, resolve_error = decl_api.resolve_oft_name(raw_show, name, version)
        if not oft_name:
            return {
                "status": "error",
                "name": name,
                "error": resolve_error
                or f"{name}: not found in deployed feature views",
            }

        from snowflake.ml.feature_store.decl.state import _parse_oft_name

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

        fv_name, resolved_version = _parse_oft_name(oft_name)

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
            status = self.get_status(from_dir=from_dir, target_name=target_name)
            ingest_url = decl_api.get_service_endpoint(status, "ingest")
            query_url = decl_api.get_service_endpoint(status, "query")

            found_spec = self._find_spec(paths, fv_name, resolved_version)
            # Enrich on a copy: never mutate the dict ``_find_spec`` returned.
            # It re-parses YAML per call today, but a future memoized
            # ``_find_spec`` would otherwise leak the injected ``columns`` into
            # later describes as if the operator had authored them.
            spec = copy.deepcopy(found_spec) if found_spec else None
            source_name = fv_name
            if spec:
                sources = spec.get("sources", [])
                if sources and isinstance(sources, list) and sources[0].get("name"):
                    source_name = sources[0]["name"]
                    if not sources[0].get("columns"):
                        ds_spec = self._find_datasource(paths, source_name)
                        if ds_spec and ds_spec.get("columns"):
                            spec["sources"][0]["columns"] = copy.deepcopy(
                                ds_spec["columns"]
                            )

            examples = decl_api.build_describe_examples(
                fv_name.lower(),
                resolved_version.lower(),
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
            "version": resolved_version.lower(),
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
            version=resolved_version.lower(),
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
    def _load_yaml_specs(directory: Path) -> Iterator[dict[str, Any]]:
        """Yield each parsed YAML mapping under *directory* (best-effort).

        Reads only the canonical authored-spec directory resolved from
        ``FSProjectPaths``; a coincidental ``example_store/`` (or any other
        cwd-relative tree) is never consulted. Unreadable / non-mapping files
        are skipped so describe examples degrade gracefully rather than fail.
        """
        try:
            import yaml
        except ImportError:
            return
        if not directory.is_dir():
            return
        for path in sorted(directory.glob("*.yaml")) + sorted(directory.glob("*.yml")):
            try:
                with SecurePath(str(path)).open(
                    "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
                ) as f:
                    spec = yaml.safe_load(f)
            except Exception as exc:
                # Best-effort enrichment: a broken/unreadable spec must not
                # fail ``describe``, but it should not be silently invisible
                # either. Log the offending path so a genuine parse error is
                # discoverable.
                log.warning("Skipping unreadable spec %s: %s", path, exc)
                continue
            if isinstance(spec, dict):
                yield spec

    @classmethod
    def _find_spec(
        cls,
        paths: FSProjectPaths,
        fv_name: str,
        version: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Find an authored FeatureView YAML in the project's ``sources``.

        Matches by ``name`` (case-insensitive). When *version* is known (from
        the resolved OFT name) a spec whose ``version`` also matches wins, so
        the examples reflect the exact deployed version; otherwise the first
        name match is returned.
        """
        name_match: Optional[dict[str, Any]] = None
        for spec in cls._load_yaml_specs(paths.feature_views_dir):
            if str(spec.get("name", "")).lower() != fv_name.lower():
                continue
            if version and str(spec.get("version", "")).lower() == version.lower():
                return spec
            if name_match is None:
                name_match = spec
        # No exact version match: fall back to the first name match so examples
        # still render (better a same-name spec than none).
        return name_match

    @classmethod
    def _find_datasource(
        cls,
        paths: FSProjectPaths,
        source_name: str,
    ) -> Optional[dict[str, Any]]:
        """Find an authored datasource YAML by ``name`` in the project's ``sources``."""
        for spec in cls._load_yaml_specs(paths.datasources_dir):
            if str(spec.get("name", "")).lower() == source_name.lower():
                return spec
        return None

    def _build_session(self) -> Any:
        """Return the one CLI-owned Snowpark Session for this command.

        Delegates to :attr:`SqlExecutionMixin.snowpark_session`, which lazily
        builds a single Session wrapping ``self._conn`` and caches it, so the
        many state-fetch facades and the init-first guard in one ``snow
        feature`` invocation share one Session instead of leaking a fresh
        Session per call. Not closed here: the Session wraps the CLI's active
        connection (``self._conn``), which the process owns for the command
        lifetime; closing it would break subsequent ``execute_query`` calls.
        """
        return self.snowpark_session

    def _get_feature_store(self, target: FSTarget) -> Any:
        """Return a ``FeatureStore`` bound to the active connection.

        Raises ``FeatureStoreNotInitializedError`` when the target schema
        lacks the bootstrap tags (the command layer turns it into a "run
        ``snow feature init``" ``CliError``).
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
        """Init-first guard called by every command except ``init``.

        A single ``SHOW TAGS`` round-trip that raises
        ``FeatureStoreNotInitializedError`` on an uninitialised schema (the
        command layer converts it to the "run ``snow feature init``" error).
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
        """Build the ``{physical_table → logical BatchSource.name}`` lookup.

        Single entry point shared by ``plan`` / ``write_plan`` / ``init`` so
        they cannot drift. Always returns a dict (empty when no project is on
        disk or loadable), which carries the cold-start meaning at the decl
        boundary; threaded into :meth:`_fetch_applied_state_bundle` so BFV
        source-binding recovery prefers authored logical names.
        """
        if project_root is None:
            return {}
        project_root = as_path(project_root)
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

        Single source of truth for the SQL + imperative-facade sequence the
        planner, ``write_plan``, and ``init``'s export pass all consume, so
        they cannot drift on which inputs feed ``fetch_applied_state`` (that
        drift previously broke BatchFV source-binding recovery on init).

        Returns ``(show_rows, applied_state, entity_rows, feature_group_rows,
        specification_map)``. ``specification_map`` (the raw per-OFT DESCRIBE
        payload) is exposed because the ``applied_state`` overlay only covers
        the BFV / SFV / RealtimeFV kinds, so the exporter still needs it for
        non-FV-kind OFTs such as the FeatureGroup-backing ones.
        """
        sqls = decl_api.state_queries(target.database, target.schema)
        with _state_fetch_progress() as progress:
            raw_show = _rows_to_dicts(
                self.execute_query(sqls["show_ofts"], cursor_class=DictCursor)
            )
            raw_tables = _rows_to_dicts(
                self.execute_query(sqls["show_tables"], cursor_class=DictCursor)
            )
            # Front-load the denominator with the known OFT count (DESCRIBE
            # phase runs last) so the collect reads mid-flight instead of "done".
            progress.add_known(len(raw_show))
            # Fetched before the visible phases to keep phase ordering clean.
            dt_text_map = self._fetch_dt_text_map(sqls)
            progress.begin_phase("Loading entities")
            entity_rows = self._fetch_entity_rows(
                target, on_progress=progress.callback("Loading entities")
            )
            progress.begin_phase("Loading feature groups")
            feature_group_rows = self._fetch_feature_group_rows(
                target, on_progress=progress.callback("Loading feature groups")
            )
            progress.begin_phase("Loading feature views")
            feature_view_rows = self._fetch_feature_view_rows(
                target, on_progress=progress.callback("Loading feature views")
            )
            progress.begin_phase("Loading stream sources")
            stream_source_rows = self._fetch_stream_source_rows(
                target, on_progress=progress.callback("Loading stream sources")
            )
            # OFT DESCRIBE runs LAST as a pre-counted phase (count already
            # seeded via ``add_known`` above): the big, smooth per-item motion.
            progress.begin_phase("Loading online feature tables", pre_counted=True)
            specification_map = self._fetch_oft_state(
                raw_show,
                sqls,
                on_progress=progress.callback("Loading online feature tables"),
            )
        # Wave 3B / contract §8b: ``stream_source_rows`` (fetched above,
        # inside the progress block) is threaded runtime-authoritative into
        # ``decl_api.fetch_applied_state`` so the planner's source-diff branch
        # sees ``Datasource(kind="StreamingSource")`` entries for
        # already-registered stream sources and emits NO_CHANGE rather than
        # the spurious CREATE_SOURCE noise that produced the ``UserWarning:
        # StreamSource <name> already exists. Skip registration.`` symptom on
        # apply.  The bundle contract's return-tuple shape is preserved.
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
        on_progress: Optional[ProgressCallback] = None,
    ) -> dict[str, dict[str, Any]]:
        """Fetch per-OFT spec JSON via ``DESCRIBE … TYPE = SPECIFICATION``.

        This is the slowest leg of ``plan`` / ``list`` state fetch: one
        ``DESCRIBE`` round-trip per online feature table.  ``on_progress``
        (when supplied) is invoked as ``(0, N, "")`` once up front — the
        row count from ``SHOW ONLINE FEATURE TABLES`` is the total — then
        ``(i, N, name)`` after each OFT is processed (whether its DESCRIBE
        succeeded, was skipped, or was nameless) so a determinate bar
        always fills.  The library never prints; the CLI owns the bar.
        """
        specification_map: dict[str, dict[str, Any]] = {}
        spec_template = state_sqls.get("describe_specification_template")
        if not spec_template:
            return specification_map
        total = len(oft_rows)
        if on_progress is not None:
            on_progress(0, total, "")
        for idx, row in enumerate(oft_rows, start=1):
            name = row.get("name", "")
            if name:
                # The decl template's ``{name}`` slot is already wrapped in
                # double quotes (``…"{name}"…``), so fill it with the bare
                # object name and escape any embedded ``"`` (doubled) rather
                # than routing through ``to_identifier`` /
                # ``to_quoted_identifier`` — those emit their own quotes and
                # would double-wrap the already-quoted slot.
                # ``identifier_to_str`` is a no-op unless the SHOW row already
                # returned a quoted identifier, which it then unquotes so the
                # ``"`` doubling below re-quotes exactly once. ``str.replace``
                # (not ``.format``) avoids a ``KeyError`` on names containing
                # ``{`` / ``}``.
                inner = identifier_to_str(str(name)).replace('"', '""')
                spec_sql = spec_template.replace("{name}", inner)
                spec_rows = None
                try:
                    spec_rows = _rows_to_dicts(
                        self.execute_query(spec_sql, cursor_class=DictCursor)
                    )
                except Exception as exc:  # noqa: BLE001
                    log.debug("spec-query failed for %s (skipping): %s", name, exc)
                if spec_rows is not None:
                    parsed = decl_api.parse_specification_rows(spec_rows)
                    if parsed is not None:
                        specification_map[name] = parsed
            if on_progress is not None:
                on_progress(idx, total, str(name))
        return specification_map

    def _fetch_dt_text_map(
        self,
        state_sqls: dict[str, str],
    ) -> dict[str, str]:
        """Fetch ``SHOW DYNAMIC TABLES`` rows and project to ``{name: text}``.

        The BatchFV ``DESCRIBE ... TYPE = SPECIFICATION`` JSON drops the
        offline source-table binding; the DT ``text`` column carries it, so
        this map recovers a non-lossy ``sources[0].table`` for the planner.
        Empty when the SQL is absent (older builds) or yields no rows.
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

    def _call_inventory_facade(
        self,
        facade: Callable[..., list[dict[str, Any]]],
        target: FSTarget,
        what: str,
        on_progress: Optional[ProgressCallback] = None,
    ) -> list[dict[str, Any]]:
        """Call an imperative applied-state inventory facade, failing loudly.

        The four applied-state inventory reads — ``list_entities`` /
        ``list_feature_views`` / ``list_feature_groups`` /
        ``list_stream_sources`` — are **authoritative**: an empty result means
        "nothing deployed", which the planner turns into ``CREATE_*`` ops that
        ``apply`` then executes without a re-plan. A read that fails past the
        library's retry must therefore surface as a ``CliError`` ("unknown"
        must never be reported as "none") rather than degrade to ``[]`` and
        produce a confidently-wrong plan that recreates objects that already
        exist. ``FeatureStoreNotInitializedError`` propagates unchanged so the
        command layer can rewrap it into the actionable ``snow feature init``
        message.

        *what* is the human-readable object class for the error message
        (e.g. ``"registered entities"`` / ``"feature views"``).
        """
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        ctx = get_cli_context()
        try:
            session = self._build_session()
            return facade(
                session,
                target.database,
                target.schema,
                ctx.connection.warehouse or "",
                on_progress=on_progress,
            )
        except FeatureStoreNotInitializedError:
            raise
        except Exception as exc:
            # Do NOT degrade to []: an empty applied-state inventory is read
            # by the planner as "nothing deployed" and emits ``CREATE_*`` for
            # objects that already exist. The library already retried the
            # transient warehouse-suspend race, so a failure here is real.
            log.warning(
                "%s failed for %s.%s: %s",
                getattr(facade, "__name__", "inventory read"),
                target.database,
                target.schema,
                exc,
            )
            raise CliError(
                f"Failed to read {what} for "
                f"{sanitize_for_terminal(str(target.database))}."
                f"{sanitize_for_terminal(str(target.schema))}: "
                f"{sanitize_for_terminal(str(exc))}. "
                "This is often a transient warehouse auto-resume/auto-suspend race "
                "(the lookup runs a warehouse-bound RESULT_SCAN query); re-run the command. "
                "If it persists, verify the warehouse is available and the schema is a feature store."
            ) from exc

    def _fetch_entity_rows(
        self,
        target: FSTarget,
        on_progress: Optional[ProgressCallback] = None,
    ) -> list[dict[str, Any]]:
        """Fetch entity tag rows via the imperative ``list_entities()`` facade.

        Does NOT soft-fail to ``[]``: an empty entity set makes
        ``validate_specs`` emit spurious ``MISSING_ENTITY`` for every
        referencing FV, so a read that fails past the library's retry surfaces
        as a ``CliError``. See :meth:`_call_inventory_facade` for the shared
        fail-loud contract.
        """
        return self._call_inventory_facade(
            decl_api.fetch_entity_rows,
            target,
            "registered entities",
            on_progress=on_progress,
        )

    def _fetch_feature_view_rows(
        self,
        target: FSTarget,
        on_progress: Optional[ProgressCallback] = None,
    ) -> list[dict[str, Any]]:
        """Fetch feature-view rows via the imperative ``list_feature_views()``.

        Authoritative FV discovery: surfaces offline-only
        ``BatchFeatureView``s that ``SHOW ONLINE FEATURE TABLES`` cannot
        enumerate. Does NOT soft-fail to ``[]`` — an empty result would re-emit
        a spurious ``CREATE_FV`` for every deployed FV. See
        :meth:`_call_inventory_facade` for the shared fail-loud contract.
        """
        return self._call_inventory_facade(
            decl_api.fetch_feature_view_rows,
            target,
            "feature views",
            on_progress=on_progress,
        )

    def _fetch_stream_source_rows(
        self,
        target: FSTarget,
        on_progress: Optional[ProgressCallback] = None,
    ) -> list[dict[str, Any]]:
        """Fetch registered stream-source rows for *target*.

        Gives the planner a runtime-authoritative view of
        ``Datasource(kind="StreamingSource")`` registrations. Does NOT
        soft-fail to ``[]`` — an empty result would re-emit a spurious
        ``CREATE_SOURCE`` for every registered stream source. See
        :meth:`_call_inventory_facade` for the shared fail-loud contract.
        """
        return self._call_inventory_facade(
            decl_api.fetch_stream_source_rows,
            target,
            "stream sources",
            on_progress=on_progress,
        )

    def _fetch_feature_group_rows(
        self,
        target: FSTarget,
        on_progress: Optional[ProgressCallback] = None,
    ) -> list[dict[str, Any]]:
        """Fetch feature-group rows via the imperative ``list_feature_groups()``.

        ``FeatureGroup`` has no ``SHOW`` SQL, so this is the only path that
        surfaces deployed FGs into applied state. Does NOT soft-fail to ``[]``
        — an empty result would re-emit a spurious ``CREATE_FG`` for every
        deployed FG. See :meth:`_call_inventory_facade` for the shared
        fail-loud contract.
        """
        return self._call_inventory_facade(
            decl_api.fetch_feature_group_rows,
            target,
            "feature groups",
            on_progress=on_progress,
        )

    # ------------------------------------------------------------------
    # _resolve_service_target — manifest-or-connection for online-service
    # ------------------------------------------------------------------

    def _resolve_service_target(
        self,
        from_dir: Optional[Path | SecurePath],
        target_name: Optional[str],
        *,
        allow_connection_fallback: bool = False,
    ) -> Tuple[str, str, Optional[str]]:
        """Resolve ``(database, schema, role)`` for an ``online-service`` sub-command.

        Tries manifest discovery first. The connection fallback is opt-in and
        fail-closed: it applies only when ``allow_connection_fallback`` is
        True AND no explicit ``target_name`` was given AND no manifest is
        reachable, so a runtime can be stood up before a project exists. Every
        other caller is strict — a manifest-less directory re-raises
        ``CliError``. This keeps the destructive ``online-service drop`` path
        from ever resolving to whatever the connection happens to point at. An
        explicit
        ``--target`` against a manifest-less directory always re-raises
        ``CliError``. ``role`` is the target's when set, else the connection's.
        """
        ctx = get_cli_context()
        start = as_path(from_dir) if from_dir is not None else Path.cwd()
        try:
            _, _, target = self._resolve_project(start, target_name)
        except AccountMismatchError:
            # A reachable manifest with a mismatched account is a real error
            # even without an explicit ``--target``: never silently manage a
            # runtime against the connection's account instead.
            raise
        except CliError:
            if target_name is not None or not allow_connection_fallback:
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
        from_dir: Optional[Path | SecurePath] = None,
        target_name: Optional[str] = None,
        *,
        allow_connection_fallback: bool = False,
    ) -> dict[str, Any]:
        """Query and parse the feature store online-service status.

        Location comes from the manifest target. The connection fallback is
        opt-in via ``allow_connection_fallback`` (used by the
        ``online-service create`` flow, which polls status before a project
        exists); a bare status read
        is strict and returns the ``{status: error}`` envelope carrying the
        manifest message when no manifest is reachable. Returns a parsed
        status dict, or an ``{status: error}`` envelope on failure.
        """
        try:
            database, schema, _ = self._resolve_service_target(
                from_dir,
                target_name,
                allow_connection_fallback=allow_connection_fallback,
            )
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
            # Own the rich TABLE banner here (adapter parity with ``describe``):
            # the library formatter is invoked from the manager, not from
            # ``commands.py``, and the rendered text rides on ``_display``. The
            # user / database / schema / verbose formatter inputs are resolved
            # here and never leaked as payload keys.
            result["_display"] = decl_api.format_status_display(
                result,
                user=ctx.connection.user or "",
                database=database,
                schema=schema,
                verbose=bool(getattr(ctx, "verbose", False)),
            )
            return result
        except Exception as exc:
            log.warning("get_status raised %s: %s", type(exc).__name__, exc)
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # initialize_service / destroy_service
    # ------------------------------------------------------------------

    def initialize_service(
        self,
        from_dir: Optional[Path | SecurePath] = None,
        target_name: Optional[str] = None,
        producer_role: Optional[str] = None,
        consumer_role: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send CREATE runtime command. Returns immediately; caller polls.

        Producer-role precedence is explicit > manifest ``target.role`` >
        connection role; consumer defaults to ``PUBLIC``. Returns a
        ``{status, message}`` envelope (``RUNNING`` / ``CREATING`` / ``error``).
        """
        database, schema, resolved_role = self._resolve_service_target(
            from_dir, target_name, allow_connection_fallback=True
        )
        p_role = producer_role or resolved_role
        c_role = consumer_role or "PUBLIC"
        sqls = decl_api.service_sql(database, schema, p_role, c_role)
        location = f"{database}.{schema}"

        current = self.get_status(
            from_dir=from_dir,
            target_name=target_name,
            allow_connection_fallback=True,
        )
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
        from_dir: Optional[Path | SecurePath] = None,
        target_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Drop all OFTs then drop the feature store runtime.

        Returns a ``{status, dropped_ofts, errors}`` envelope whose ``status``
        reflects the outcome: ``destroyed`` when nothing errored,
        ``partial_failure`` when at least one OFT was dropped but something
        errored (a failed OFT drop, a failed runtime drop, or a failed SHOW),
        and ``failed`` when errors occurred and nothing was dropped. This lets
        ``online-service drop`` exit non-zero on a failed teardown instead of
        reporting ``destroyed`` unconditionally.
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

        if not errors:
            status = "destroyed"
        elif dropped_ofts:
            status = "partial_failure"
        else:
            status = "failed"
        return {"status": status, "dropped_ofts": dropped_ofts, "errors": errors}

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------

    def ingest(
        self,
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        source_name: str,
        records: List[dict],
    ) -> dict[str, Any]:
        """Stream records into a source via ``FeatureStore.stream_ingest``.

        Delegates the wire path (URL resolution, PAT auth,
        partial-success reporting) to the feature-store library, but runs a
        client-side per-record schema preflight first, raising ``ValueError``
        when a record's keys diverge from the registered source schema.
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
        from_dir: Path | SecurePath,
        target_name: Optional[str],
        feature_view_name: str,
        version: str,
        keys: List[dict],
    ) -> dict[str, Any]:
        """Online-lookup features via ``FeatureStore.read_feature_view``.

        ``version`` is required (``get_feature_view`` needs name + version for
        a string reference); each key dict must carry every declared join key
        or ``_to_positional_keys`` raises ``ValueError``.
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
