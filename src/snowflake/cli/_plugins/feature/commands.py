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

"""Command surface for 'snow feature' (registration scaffold).

This module declares the ``snow feature`` command group and its command
surface (names, arguments, options, and help text) so the plugin can be
registered and reviewed on its own. The command bodies are intentionally
unimplemented; the implementation lands in a follow-up feature-store PR.
"""

from __future__ import annotations

from typing import List, Optional

import typer
from snowflake.cli.api.commands.flags import (
    ForceOption,
    InteractiveOption,
    LocalDirectoryType,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.secure_path import SecurePath


def _from_option_callback(value: Optional[SecurePath]) -> SecurePath:
    """Default ``--from`` to the current working directory (same as DCM)."""
    return value if value is not None else SecurePath.cwd()


from_option = typer.Option(
    None,
    "--from",
    help="Local directory containing the feature-store project (must "
    "contain manifest.yml). Omit to use the current directory.",
    show_default=False,
    click_type=LocalDirectoryType(),
    callback=_from_option_callback,
)


target_option = typer.Option(
    None,
    "--target",
    help="Target profile from manifest.yml to use. Uses default_target "
    "when not specified.",
    show_default=False,
)


variables_option = typer.Option(
    None,
    "--variable",
    "-D",
    help="Variables for the project's templating context, e.g. "
    '`-D "<key>=<value>"`. May be repeated.',
    show_default=False,
)


app = SnowTyperFactory(
    name="feature",
    help="Manages declarative feature-store objects in Snowflake.",
    preview=True,
    is_hidden=FeatureFlag.ENABLE_FEATURE_STORE.is_disabled,
)


# ---------------------------------------------------------------------------
# init — single bootstrap command (subsumes the deleted `export` command)
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def init(
    target: Optional[str] = typer.Option(
        None,
        "--target",
        help=(
            "Manifest target name.  On a brand-new manifest this names "
            "the only target (default 'DEFAULT').  On a re-init, picks "
            "which existing manifest target to export from "
            "(default = manifest's default_target)."
        ),
        show_default=False,
    ),
    python_form: bool = typer.Option(
        False,
        "--python",
        help="Export deployed objects as .py files (Pydantic constructors).  This is the default form; pass --yaml to export YAML instead.",
    ),
    yaml_form: bool = typer.Option(
        False,
        "--yaml",
        help="Export deployed objects as YAML files instead of the default .py Pydantic constructors.",
    ),
    **options,
) -> CommandResult:
    """Bootstrap a feature-store project and pull deployed artifacts.

    Always runs in the current directory. Idempotent: re-running preserves
    the existing ``manifest.yml`` and refreshes the on-disk artifacts.
    Objects export as ``.py`` (Pydantic constructors) by default, or YAML
    with ``--yaml``. On a re-init, ``--database`` / ``--schema`` values that
    differ from the resolved manifest target are rejected (edit the manifest).
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def sync(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    name: Optional[str] = typer.Option(
        None,
        "--name",
        help=(
            "Exact name of a single object to sync (case-insensitive). "
            "Matches across all object kinds (Entity, BatchSource, "
            "StreamingSource, BatchFeatureView, StreamingFeatureView, "
            "FeatureGroup).  Omit to sync all deployed objects."
        ),
        show_default=False,
    ),
    python_form: bool = typer.Option(
        False,
        "--python",
        help="Export deployed objects as .py files (Pydantic constructors).  This is the default form; pass --yaml to export YAML instead.",
    ),
    yaml_form: bool = typer.Option(
        False,
        "--yaml",
        help="Export deployed objects as YAML files instead of the default .py Pydantic constructors.",
    ),
    **options,
) -> CommandResult:
    """Pull deployed feature-store objects into the local sources tree.

    Requires an existing ``manifest.yml`` and an initialised feature store;
    unlike ``init`` it does not re-bootstrap the runtime. Use ``--name`` to
    sync a single object; ``--yaml`` emits YAML instead of ``.py``.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def apply(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    destructive: bool = typer.Option(
        False,
        "--destructive",
        help="Allow operations that drop, recreate, or overwrite existing objects.",
    ),
    plan: Optional[str] = typer.Option(
        None,
        "--plan",
        help="Path to a pre-computed plan JSON file (from 'snow feature plan'). "
        "When provided, skips the auto-discovery of out/plan/.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Apply the discovered (or explicit) plan against Snowflake.

    A pure plan-file consumer: auto-discovers the latest unapplied plan
    under ``<project_root>/out/plan/`` (or consumes ``--plan <path>``).
    Run ``snow feature plan`` first to produce a plan file.

    Templating overrides are a plan-time concern only: they are resolved
    and baked into the plan JSON, so ``apply`` does not accept them. Re-run
    ``snow feature plan`` with the new override, then ``apply``.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def plan(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    out: Optional[str] = typer.Option(
        None,
        "--out",
        help="Path to write the plan JSON file. Defaults to "
        "`<project_root>/out/plan/feature_plan_<timestamp>.json`.",
        show_default=False,
    ),
    no_delete: bool = typer.Option(
        True,
        "--no-delete/--delete",
        help="Deletion detection is OFF by default: objects present in the "
        "target but absent from the manifest are left untouched. Pass "
        "``--delete`` to enable full-sync deletion (emit DROP ops for orphans).",
    ),
    **options,
) -> CommandResult:
    """Show what would change if the project were applied (read-only).

    Also writes the plan as JSON under ``<project_root>/out/plan/`` so it
    can be applied later with ``snow feature apply``.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command(name="list", requires_connection=True)
def list_cmd(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
) -> CommandResult:
    """List deployed feature-store objects from Snowflake."""
    raise NotImplementedError


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def describe(
    name: str = typer.Argument(
        ...,
        help="Feature store object (e.g. 'user_event_features'). "
        "Also accepts the full OFT name (NAME$VERSION$ONLINE).",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    version: Optional[str] = typer.Option(
        None,
        "--version",
        help="Feature store object version (e.g. 'V1'). Use to disambiguate "
        "objects that share a base name but differ only by version. "
        "Required when multiple versions of the named object are deployed.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Describe a single feature-store object."""
    raise NotImplementedError


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


# ``online-service`` is its own entity with a lifecycle (status / create /
# drop), so it is a nested sub-group rather than one command configured by
# flags: the operations are alternate verbs, not configuration of a single
# command (see ``docs/contributing/adding-commands.md``). ``preview=True``
# matches the parent factory; it is not inherited through ``add_typer``.
online_service_app = SnowTyperFactory(
    name="online-service",
    help="Manages the feature store online service.",
    preview=True,
)
app.add_typer(online_service_app)


@online_service_app.command(name="status", requires_connection=True)
def online_service_status(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
) -> CommandResult:
    """Show the feature store online service runtime status.

    Status is compact by default; pass ``-v`` / ``--verbose`` for the full
    layout. The location comes from the manifest target, which must be
    reachable; an explicit ``--target`` against a manifest-less directory is
    always a hard error.
    """
    raise NotImplementedError


@online_service_app.command(name="create", requires_connection=True)
def online_service_create(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    producer_role: Optional[str] = typer.Option(
        None,
        "--producer-role",
        help="Role for producing features. Defaults to the manifest "
        "target's role (or the connection role if neither is set).",
        show_default=False,
    ),
    consumer_role: Optional[str] = typer.Option(
        None,
        "--consumer-role",
        help="Role for consuming features. Defaults to PUBLIC.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Create and initialize the feature store online service.

    Idempotent: a no-op when the service is already RUNNING. This is the only
    online-service command that falls back to the connection's database /
    schema when no ``manifest.yml`` is reachable, so a runtime can be stood up
    before a project exists; an explicit ``--target`` against a manifest-less
    directory is always a hard error.
    """
    raise NotImplementedError


@online_service_app.command(name="drop", requires_connection=True)
def online_service_drop(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """Destroy the online service and all Online Feature Tables.

    Destructive: requires a reachable manifest (no connection fallback) and,
    unless ``--force`` is passed, a typed confirmation of the resolved
    ``DATABASE.SCHEMA``. An explicit ``--target`` against a manifest-less
    directory is always a hard error.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def ingest(
    source_name: str = typer.Argument(
        ...,
        help="Name of the streaming source to ingest records into.",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    data: str = typer.Option(
        "-",
        "--data",
        help="Path to a JSON file containing a records array, or - to read from stdin.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Ingest records into a streaming feature source via the Online Service."""
    raise NotImplementedError


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def query(
    feature_view_name: str = typer.Argument(
        ...,
        help="Name of the feature view to query.",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    version: str = typer.Option(
        ...,
        "--version",
        help=(
            "Feature view version (e.g. 'V1').  Required because "
            "the online lookup is keyed on (name, version) — "
            "there is no 'latest' fallback for a bare name."
        ),
        show_default=False,
    ),
    keys: str = typer.Option(
        ...,
        "--keys",
        help='JSON array of entity key objects, e.g. \'[{"user_id": "u1"}]\'.',
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Query online features for a feature view via the Online Service."""
    raise NotImplementedError
