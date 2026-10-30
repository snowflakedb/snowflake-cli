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
"""How fast starts are issued, and how that rate is derived.

Two independent limits govern a run. The pace is a minimum interval between
every invocation start, enforced for the whole run rather than only during
ramp-up: a pool that refills the instant a slot frees produces synchronized
starts, which is exactly what provokes the templating-capacity rejection. The
in-flight ceiling is the second limit, capping how many invocations run at once.

Calibration exists because the two commands are not comparable in duration: a
deploy measures several times longer than a plan on the same project. Since
steady-state in-flight is duration divided by pace, reusing a plan-derived pace
for a deploy over-subscribes the runner by roughly the same factor.

The duration is measured from the run itself rather than from a probe. Each
completed invocation feeds a rolling mean, and the pace is re-derived from that
mean as the run proceeds. Measuring a single target on its own would serialize a
whole invocation before any fan-out began, which on a deploy costs minutes, and
would assume every target takes as long as the first one, which is only true when
the targets are generated from one template.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional

from snowflake.cli._plugins.dcm.fanout.targets import PacingConfig
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)

# How many recent invocations the rolling mean is taken over. Short enough to
# follow a run whose targets differ in size, long enough that one outlier does
# not move the pace far.
DEFAULT_PACE_WINDOW = 10


@dataclass
class ResolvedPacing:
    """The pacing a run will actually use, and where each value came from."""

    min_seconds: float
    max_inflight: int
    calibrate: bool
    pace: float
    window: int = DEFAULT_PACE_WINDOW
    calibrated_from_seconds: Optional[float] = None
    recent_durations: Deque[float] = field(default_factory=deque)

    def __post_init__(self) -> None:
        # Bound the window here rather than only at the call site, so a directly
        # constructed ResolvedPacing cannot grow an unbounded history.
        if self.recent_durations.maxlen != self.window:
            self.recent_durations = deque(self.recent_durations, maxlen=self.window)

    @property
    def is_calibrated(self) -> bool:
        return self.calibrated_from_seconds is not None

    @property
    def pace_samples(self) -> int:
        """How many measured invocations the current pace rests on."""
        return len(self.recent_durations)


def derive_pace(
    min_seconds: float, max_inflight: int, measured_duration: float
) -> float:
    """The start interval implied by one measured invocation.

    Holding steady-state in-flight at ``max_inflight`` requires a start every
    ``measured_duration / max_inflight`` seconds. ``min_seconds`` is a floor, so
    a fast command never paces tighter than the operator allowed.
    """
    if max_inflight <= 0:
        raise CliError("The in-flight ceiling must be greater than zero.")
    return max(min_seconds, measured_duration / max_inflight)


def resolve_pacing(
    pace: Optional[float],
    max_inflight: Optional[int],
    calibrate: Optional[bool],
    window: int = DEFAULT_PACE_WINDOW,
) -> ResolvedPacing:
    """Combine the pacing flags with the built-in defaults.

    Each flag is independent: passing ``--pace`` alone leaves the default
    in-flight ceiling in force.
    """
    defaults = PacingConfig()
    resolved = PacingConfig(
        min_seconds=defaults.min_seconds if pace is None else pace,
        max_inflight=defaults.max_inflight if max_inflight is None else max_inflight,
        calibrate=defaults.calibrate if calibrate is None else calibrate,
    )

    if resolved.min_seconds < 0:
        raise CliError("--pace must be zero or greater.")
    if resolved.max_inflight <= 0:
        raise CliError("--max-inflight must be greater than zero.")
    if window <= 0:
        raise CliError("The pace window must be greater than zero.")

    log.info(
        "Resolved fan-out pacing (min_seconds=%s, max_inflight=%d, calibrate=%s, "
        "window=%d).",
        resolved.min_seconds,
        resolved.max_inflight,
        resolved.calibrate,
        window,
    )
    return ResolvedPacing(
        min_seconds=resolved.min_seconds,
        max_inflight=resolved.max_inflight,
        calibrate=resolved.calibrate,
        window=window,
        # Until the first invocations report a duration, the pace is the floor.
        pace=resolved.min_seconds,
        recent_durations=deque(maxlen=window),
    )


def observe_duration(
    pacing: ResolvedPacing, measured_duration: float
) -> ResolvedPacing:
    """Fold one completed invocation into the rolling mean and re-derive the pace.

    Durations of zero or less are ignored: they mean the invocation never
    reported a time, and treating that as a fast target would tighten the pace on
    no evidence.
    """
    if measured_duration <= 0:
        return pacing

    pacing.recent_durations.append(measured_duration)
    mean_duration = sum(pacing.recent_durations) / len(pacing.recent_durations)
    pacing.calibrated_from_seconds = mean_duration
    pacing.pace = derive_pace(pacing.min_seconds, pacing.max_inflight, mean_duration)
    log.info(
        "Re-derived fan-out pace from the rolling mean "
        "(samples=%d, mean_seconds=%.1f, max_inflight=%d, pace_seconds=%.3f).",
        len(pacing.recent_durations),
        mean_duration,
        pacing.max_inflight,
        pacing.pace,
    )
    return pacing
