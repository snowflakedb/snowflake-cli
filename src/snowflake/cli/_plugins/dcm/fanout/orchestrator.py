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
"""Turning a `--fanout` invocation into a paced run over many targets.

Option handling here rejects rather than merges. When a fan-out pattern is
supplied, ``manifest.yml`` becomes the single source of per-tenant identity and
values, so a positional identifier, a ``--target`` or a ``-D`` value alongside it
is a mistake: silently preferring one of the two would deploy something the
operator did not ask for.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from snowflake.cli._plugins.dcm.fanout import provision, report
from snowflake.cli._plugins.dcm.fanout.pacing import resolve_pacing
from snowflake.cli._plugins.dcm.fanout.runner import (
    InvocationTemplate,
    connection_arguments,
)
from snowflake.cli._plugins.dcm.fanout.scheduler import FanoutScheduler
from snowflake.cli._plugins.dcm.fanout.targets import (
    DEFAULT_RETRY_ATTEMPTS,
    FanoutTarget,
    filter_targets,
    select_targets,
)
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.models import DCMManifest, DCMTarget
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CommandResult, EmptyResult, ObjectResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutor

log = logging.getLogger(__name__)


@dataclass
class FanoutOptions:
    """The fan-out options as they arrive from the command line."""

    pattern: str
    pace: Optional[float] = None
    max_inflight: Optional[int] = None
    calibrate: Optional[bool] = None
    retries: Optional[int] = None
    only: Optional[str] = None
    list_targets: bool = False


def reject_conflicting_options(
    identifier: Optional[FQN],
    target: Optional[str],
    variables: Optional[List[str]],
) -> None:
    """Refuse the options the manifest replaces in a fan-out run.

    Each of these would compete with the manifest for the same decision, and a
    customer who passes both has made a mistake worth surfacing rather than
    resolving by precedence.
    """
    conflicts = []
    if identifier is not None:
        conflicts.append(
            "a project identifier argument (each target's project_name comes from manifest.yml)"
        )
    if target is not None:
        conflicts.append(
            "--target (--fanout selects the targets it runs by name pattern)"
        )
    if variables:
        conflicts.append(
            "-D/--variable (per-target values come from 'variable_overrides' in manifest.yml)"
        )
    if conflicts:
        raise CliError(
            "--fanout cannot be combined with " + "; ".join(conflicts) + ". "
            "Remove the conflicting option and let manifest.yml supply the value."
        )


def reject_orphaned_options(
    fanout: Optional[str],
    pace: Optional[float],
    max_inflight: Optional[int],
    calibrate: Optional[bool],
    retries: Optional[int],
    only: Optional[str],
    list_targets: bool,
) -> None:
    """Refuse fan-out options that were supplied without ``--fanout``."""
    if fanout is not None:
        return
    orphaned = [
        name
        for name, supplied in (
            ("--pace", pace is not None),
            ("--max-inflight", max_inflight is not None),
            ("--calibrate/--no-calibrate", calibrate is not None),
            ("--fanout-retries", retries is not None),
            ("--only", only is not None),
            ("--list-targets", list_targets),
        )
        if supplied
    ]
    if orphaned:
        raise CliError(
            f"{', '.join(orphaned)} only applies to a fan-out run. Add --fanout "
            f"<target name pattern>, or remove the option."
        )


def _manifest_targets(
    manifest: DCMManifest, selected: List[FanoutTarget]
) -> List[DCMTarget]:
    """The manifest entries behind the selected names, validated as the
    single-tenant path would validate them."""
    return [manifest.get_target(target.name) for target in selected]


def run_fanout(
    command_name: str,
    options: FanoutOptions,
    from_location: SecurePath,
    alias: Optional[str] = None,
    delta: bool = False,
) -> CommandResult:
    """Select the targets from the manifest, ensure the projects exist, then run."""
    manifest = DCMManifest.load(from_location)
    selected = filter_targets(select_targets(manifest, options.pattern), options.only)
    if not selected:
        raise CliError("No targets left to run after applying --only.")

    manifest_targets = _manifest_targets(manifest, selected)
    manager = DCMProjectManager()

    template = InvocationTemplate(
        command_name=command_name,
        from_location=Path(from_location.path).resolve(),
        alias=alias,
        delta=delta,
        connection_arguments=connection_arguments(get_cli_context().connection_context),
    )

    if options.list_targets:
        planned = provision.plan_provisioning(manifest_targets, manager)
        listing = report.print_target_listing(template, selected, planned)
        if get_cli_context().output_format == OutputFormat.TABLE:
            return EmptyResult()
        return ObjectResult(listing)

    provisioned = provision.ensure_projects(manifest_targets, manager, _current_role())
    if provisioned.created:
        cli_console.message(
            f"Created {len(provisioned.created)} missing DCM project object(s)."
        )

    pacing = resolve_pacing(options.pace, options.max_inflight, options.calibrate)
    retry_attempts = (
        DEFAULT_RETRY_ATTEMPTS if options.retries is None else options.retries
    )

    cli_console.message(
        f"Fanning out {command_name} across {len(selected)} target(s): "
        f"pace {pacing.min_seconds:.2f}s, in-flight ceiling {pacing.max_inflight}, "
        f"{retry_attempts} retry attempt(s)"
        + (", pace adjusted from observed durations" if pacing.calibrate else "")
        + "."
    )

    scheduler = FanoutScheduler(
        template=template,
        targets=selected,
        pacing=pacing,
        retry_attempts=retry_attempts,
        project_names={
            name: str(identifier)
            for name, identifier in provisioned.project_identifiers.items()
        },
        on_progress=report.progress_line,
    )
    result = scheduler.run()

    payload = report.build_run_log(command_name, result, provisioned)
    run_log_path = report.write_run_log(
        report.default_run_log_path(command_name), payload
    )
    report.print_run_summary(result, provisioned, run_log_path)

    if result.failed or result.skipped:
        # A partially failed run must not look like a success to a CI job, but the
        # per-tenant detail has already been printed and written, so the error
        # itself stays short.
        raise CliError(
            f"{len(result.failed)} target(s) failed"
            + (f" and {len(result.skipped)} were skipped" if result.skipped else "")
            + f" out of {len(result.outcomes)}."
        )

    if get_cli_context().output_format == OutputFormat.TABLE:
        return EmptyResult()
    return ObjectResult(payload)


def _current_role() -> Optional[str]:
    try:
        return SqlExecutor().current_role()
    except Exception as error:  # pragma: no cover - depends on the live session
        log.info("Could not determine the current role: %s.", error)
        return None
