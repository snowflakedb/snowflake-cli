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
"""What a fan-out run tells the operator while it runs and when it finishes.

Three outputs, each for a different reader. Progress lines are for someone
watching a run that will take minutes. The summary table is for whoever has to
act on the result. The JSON run log is for CI, which needs an artifact that
outlives the job and a failed-tenant list that can be fed straight back through
``--only``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.table import Table
from snowflake.cli._plugins.dcm.fanout.provision import ProvisionResult
from snowflake.cli._plugins.dcm.fanout.runner import InvocationTemplate
from snowflake.cli._plugins.dcm.fanout.scheduler import (
    STATUS_FAILED,
    FanoutRunResult,
    ProgressSnapshot,
)
from snowflake.cli._plugins.dcm.fanout.targets import FanoutTarget
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

RUN_LOG_VERSION = 1


def progress_line(snapshot: ProgressSnapshot) -> None:
    """One line of periodic progress."""
    cli_console.message(
        f"{snapshot.completed}/{snapshot.total} complete "
        f"({snapshot.succeeded} succeeded, {snapshot.failed} failed), "
        f"{snapshot.inflight} in flight, "
        f"pace {snapshot.pace_seconds:.2f}s, "
        f"elapsed {snapshot.elapsed_seconds / 60:.1f}m"
    )


def print_target_listing(
    template: InvocationTemplate,
    targets: List[FanoutTarget],
    provisioning: ProvisionResult,
) -> Dict[str, Any]:
    """Print what a run would do, without doing any of it.

    Deliberately not called a dry run: in DCM that already means PLAN, so a dry
    run of ``snow dcm plan`` would name two different things at once.
    """
    with cli_console.phase(f"{len(targets)} target(s) would run"):
        for target in targets:
            cli_console.step(
                sanitize_for_terminal(template.display_command_for(target))
            )

    if provisioning.created:
        with cli_console.phase(
            f"{len(provisioning.created)} DCM project object(s) would be created"
        ):
            for identifier in provisioning.created:
                cli_console.step(sanitize_for_terminal(identifier))
    else:
        cli_console.message("No DCM project objects would be created.")

    if provisioning.existing:
        cli_console.message(
            f"{len(provisioning.existing)} DCM project object(s) already exist and "
            f"would be reused."
        )

    return {
        "targets": [
            {
                "target": target.name,
                "project_name": str(
                    provisioning.project_identifiers.get(target.name, "")
                ),
                "command": template.display_command_for(target),
            }
            for target in targets
        ],
        "projects_to_create": list(provisioning.created),
        "projects_existing": list(provisioning.existing),
    }


def _summary_table(result: FanoutRunResult) -> Table:
    table = Table(title=None, show_lines=False)
    table.add_column("target")
    table.add_column("status")
    table.add_column("duration", justify="right")
    table.add_column("attempts", justify="right")
    table.add_column("operations", justify="right")
    for outcome in result.outcomes:
        table.add_row(
            sanitize_for_terminal(outcome.name),
            sanitize_for_terminal(outcome.status),
            f"{outcome.duration_seconds:.1f}s",
            str(outcome.attempts),
            "-" if outcome.operations is None else str(outcome.operations),
        )
    return table


def print_run_summary(
    result: FanoutRunResult,
    provisioning: ProvisionResult,
    run_log_path: Optional[Path],
) -> None:
    """Print the per-tenant table and the run-level facts that follow from it."""
    cli_console.renderable(_summary_table(result), soft_wrap=False)

    succeeded = len(result.succeeded)
    failed = len(result.failed)
    skipped = len(result.skipped)
    cli_console.message(
        f"{succeeded} succeeded, {failed} failed"
        + (f", {skipped} skipped" if skipped else "")
        + f" of {len(result.outcomes)} target(s) in {result.wall_seconds / 60:.1f}m."
    )

    if provisioning.created:
        cli_console.message(
            f"Created {len(provisioning.created)} DCM project object(s); "
            f"reused {len(provisioning.existing)}."
        )

    pace_note = (
        f"Pace {result.pace_seconds:.2f}s, in-flight ceiling {result.max_inflight}"
    )
    if result.calibrated_from_seconds is not None:
        pace_note += (
            f", derived from a {result.calibrated_from_seconds:.1f}s mean "
            f"over {result.pace_samples} invocation(s)"
        )
    cli_console.message(f"{pace_note}. Peak in flight {result.peak_inflight}.")

    if result.is_inflight_limited:
        # Worth stating plainly: a ceiling-bound run will not get faster from a
        # tighter pace, so the remedy is more parallel jobs or a larger runner.
        cli_console.warning(
            "This run was limited by the in-flight ceiling rather than by the pace. "
            "To go faster, shard the targets across more jobs or raise --max-inflight "
            "on a runner with more memory."
        )

    if result.capacity_rejections:
        cap = (
            f" The account reported a limit of {result.observed_cap} concurrent "
            f"templating operations."
            if result.observed_cap is not None
            else ""
        )
        # A rejection is only "retried" when retries were actually available;
        # with --fanout-retries 0 the same rejection is terminal.
        retried = sum(
            outcome.capacity_rejections
            for outcome in result.outcomes
            if outcome.attempts > 1
        )
        detail = f", {retried} of them retried" if retried else " and were not retried"
        cli_console.warning(
            f"{result.capacity_rejections} capacity rejection(s) occurred{detail}.{cap}"
        )

    if result.interrupted:
        cli_console.warning(
            "Interrupted. In-flight targets were allowed to finish; targets that had "
            "not started were skipped."
        )

    if failed or skipped:
        retry_list = ",".join(
            outcome.name for outcome in result.failed + result.skipped
        )
        cli_console.message(
            f"Re-run the unfinished targets with: --only {sanitize_for_terminal(retry_list)}"
        )

    if run_log_path is not None:
        cli_console.message(f"Run log written to {run_log_path}.")


def build_run_log(
    command_name: str,
    result: FanoutRunResult,
    provisioning: ProvisionResult,
) -> Dict[str, Any]:
    """The aggregate result object, shared by the JSON run log and ``--format json``."""
    return {
        "version": RUN_LOG_VERSION,
        "command": command_name,
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "summary": {
            "total": len(result.outcomes),
            "succeeded": len(result.succeeded),
            "failed": len(result.failed),
            "skipped": len(result.skipped),
            "wall_seconds": round(result.wall_seconds, 1),
            "interrupted": result.interrupted,
        },
        "pacing": {
            "pace_seconds": round(result.pace_seconds, 3),
            "max_inflight": result.max_inflight,
            "peak_inflight": result.peak_inflight,
            "calibrated_from_seconds": (
                None
                if result.calibrated_from_seconds is None
                else round(result.calibrated_from_seconds, 1)
            ),
            "pace_samples": result.pace_samples,
            "inflight_limited": result.is_inflight_limited,
        },
        "capacity": {
            "rejections": result.capacity_rejections,
            "observed_cap": result.observed_cap,
        },
        "projects": {
            "created": list(provisioning.created),
            "existing": list(provisioning.existing),
        },
        "targets": [outcome.as_dict() for outcome in result.outcomes],
        "failed_targets": [
            outcome.name for outcome in result.outcomes if outcome.status != "SUCCESS"
        ],
    }


def default_run_log_path(command_name: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path.cwd() / f"dcm-fanout-{command_name}-{stamp}.json"


def write_run_log(path: Path, payload: Dict[str, Any]) -> Optional[Path]:
    """Persist the run log, and keep going if the location is not writable.

    A run that has already done its work should not be reported as a failure
    because an artifact could not be written.
    """
    try:
        SecurePath(path).write_text(json.dumps(payload, indent=2))
    except OSError as error:
        cli_console.warning(f"Could not write the fan-out run log to {path}: {error}")
        return None
    return path


def failed_target_names(result: FanoutRunResult) -> List[str]:
    return [
        outcome.name for outcome in result.outcomes if outcome.status == STATUS_FAILED
    ]
