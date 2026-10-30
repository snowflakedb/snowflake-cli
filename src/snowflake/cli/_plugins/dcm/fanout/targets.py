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
"""Which manifest targets take part in a run.

``manifest.yml`` is the only place a tenant is declared. Each target already
carries its own ``project_name``, ``account_identifier``, ``project_owner`` and
``templating_config``, and an optional ``variable_overrides`` mapping supplies the
values that differ between tenants. A run therefore only has to say which of
those targets to include, which is what the ``--fanout`` pattern does.
"""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from snowflake.cli._plugins.dcm.models import DCMManifest, as_sql_string_literal
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)

# Defaults applied when no flag overrides them. The in-flight ceiling is
# deliberately sized for a CI runner rather than a workstation: one `snow dcm`
# invocation peaks around 163 MB resident, and a 2 vCPU / 8 GB hosted runner has
# room for well under 40 of them.
DEFAULT_MIN_SECONDS = 0.5
DEFAULT_MAX_INFLIGHT = 25
DEFAULT_CALIBRATE = True
DEFAULT_RETRY_ATTEMPTS = 2


@dataclass
class FanoutTarget:
    """One manifest target taking part in the run, with its variable values."""

    name: str
    overrides: Dict[str, Any] = field(default_factory=dict)

    def variable_arguments(self) -> List[str]:
        """Render ``overrides`` as ``-D`` values, in a stable order."""
        return [
            f"{key}={as_sql_string_literal(value)}"
            for key, value in sorted(self.overrides.items())
        ]


@dataclass
class PacingConfig:
    min_seconds: float = DEFAULT_MIN_SECONDS
    max_inflight: int = DEFAULT_MAX_INFLIGHT
    calibrate: bool = DEFAULT_CALIBRATE


def select_targets(manifest: DCMManifest, pattern: str) -> List[FanoutTarget]:
    """The manifest targets whose names match ``pattern``.

    Matching is a glob over target names (``TENANT_*``, or ``*`` for every
    target), case-insensitive because the manifest upper-cases target names on
    load. Order follows the manifest rather than the match order, so a rerun
    starts targets in the same order as the original run.
    """
    cleaned = (pattern or "").strip()
    if not cleaned:
        raise CliError(
            "--fanout needs a target name pattern, for example --fanout 'TENANT_*' "
            "or --fanout '*' for every target in manifest.yml."
        )

    wanted = cleaned.upper()
    selected = [
        FanoutTarget(name=name, overrides=dict(target.variable_overrides))
        for name, target in manifest.targets.items()
        if fnmatch.fnmatchcase(name, wanted)
    ]
    if not selected:
        known = ", ".join(sorted(manifest.targets)) or "(none)"
        raise CliError(
            f"No target in manifest.yml matches --fanout '{cleaned}'. "
            f"Targets defined in the manifest: {known}."
        )

    log.info(
        "Selected fan-out targets from the manifest (pattern=%s, matched=%d, of=%d).",
        cleaned,
        len(selected),
        len(manifest.targets),
    )
    return selected


def filter_targets(
    targets: List[FanoutTarget], only: Optional[str]
) -> List[FanoutTarget]:
    """Restrict the run to the comma-separated names in ``--only``.

    Order follows the selection rather than the ``--only`` value, so a rerun of a
    failed subset starts them in the same order as the original run.
    """
    if only is None:
        return targets

    requested = [name.strip().upper() for name in only.split(",") if name.strip()]
    if not requested:
        raise CliError(
            "--only was given no target names. Pass a comma-separated list, "
            "for example --only TENANT_A,TENANT_B."
        )

    available = {target.name for target in targets}
    unknown = sorted(set(requested) - available)
    if unknown:
        raise CliError(
            f"--only names target(s) that the --fanout pattern did not select: "
            f"{', '.join(unknown)}."
        )

    selected = set(requested)
    return [target for target in targets if target.name in selected]
