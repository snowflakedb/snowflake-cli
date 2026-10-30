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
"""The paced runner: when each tenant starts, and what happens when one fails.

Two limits are enforced independently. No two starts are closer together than the
pace, for the whole run rather than only while ramping up, because a pool that
refills the instant a slot frees produces synchronized starts and reintroduces
the capacity rejections that pacing exists to avoid. Separately, no more than
``max_inflight`` invocations run at once.

A tenant that fails on a capacity rejection is requeued with exponential backoff
and jitter. Every other failure is terminal for that tenant, and its siblings
continue regardless.
"""

from __future__ import annotations

import logging
import random
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from types import FrameType
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Union

from snowflake.cli._plugins.dcm.fanout.pacing import ResolvedPacing, observe_duration
from snowflake.cli._plugins.dcm.fanout.runner import (
    AttemptResult,
    InvocationTemplate,
    TenantOutcome,
    run_attempt,
)
from snowflake.cli._plugins.dcm.fanout.targets import FanoutTarget

log = logging.getLogger(__name__)

# Backoff between retries of a capacity rejection.
_BACKOFF_BASE_SECONDS = 2.0
_BACKOFF_CEILING_SECONDS = 60.0
_BACKOFF_JITTER_SECONDS = 1.0

# How often the scheduling loop wakes to re-check its limits.
_LOOP_SLEEP_SECONDS = 0.05

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"


@dataclass
class FanoutRunResult:
    """Everything a finished (or interrupted) run has to report."""

    outcomes: List[TenantOutcome] = field(default_factory=list)
    pace_seconds: float = 0.0
    max_inflight: int = 0
    calibrated_from_seconds: Optional[float] = None
    pace_samples: int = 0
    peak_inflight: int = 0
    inflight_limited_events: int = 0
    capacity_rejections: int = 0
    observed_cap: Optional[int] = None
    wall_seconds: float = 0.0
    interrupted: bool = False

    @property
    def succeeded(self) -> List[TenantOutcome]:
        return [
            outcome for outcome in self.outcomes if outcome.status == STATUS_SUCCESS
        ]

    @property
    def failed(self) -> List[TenantOutcome]:
        return [outcome for outcome in self.outcomes if outcome.status == STATUS_FAILED]

    @property
    def skipped(self) -> List[TenantOutcome]:
        return [
            outcome for outcome in self.outcomes if outcome.status == STATUS_SKIPPED
        ]

    @property
    def is_inflight_limited(self) -> bool:
        """Whether the ceiling, rather than the pace, was what held the run back.

        A run in this state is the signal to shard across more jobs or move to a
        larger runner, since raising the pace alone would not make it faster.
        """
        return self.inflight_limited_events > 0


@dataclass
class ProgressSnapshot:
    """A point-in-time view of the run, for periodic progress output."""

    total: int
    completed: int
    succeeded: int
    failed: int
    inflight: int
    pace_seconds: float
    elapsed_seconds: float


ProgressCallback = Callable[[ProgressSnapshot], None]


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter, for the ``attempt``-th retry (1-based).

    Jitter matters as much as the backoff itself here: retrying a batch of
    rejected tenants on an identical delay would land their starts together,
    which is the very pattern that caused the rejection.
    """
    exponential = _BACKOFF_BASE_SECONDS * (2 ** max(0, attempt - 1))
    return min(exponential, _BACKOFF_CEILING_SECONDS) + random.uniform(
        0, _BACKOFF_JITTER_SECONDS
    )


class FanoutScheduler:
    """Runs every tenant once, pacing the starts and retrying capacity rejections."""

    def __init__(
        self,
        template: InvocationTemplate,
        targets: List[FanoutTarget],
        pacing: ResolvedPacing,
        retry_attempts: int,
        project_names: Dict[str, str],
        on_progress: Optional[ProgressCallback] = None,
        progress_interval_seconds: float = 10.0,
        attempt_runner: Callable[[InvocationTemplate, FanoutTarget], AttemptResult] = (
            run_attempt
        ),
    ) -> None:
        self._template = template
        self._targets = targets
        self._pacing = pacing
        self._retry_attempts = retry_attempts
        self._project_names = project_names
        self._on_progress = on_progress
        self._progress_interval = progress_interval_seconds
        self._run_attempt = attempt_runner

        self._lock = threading.Lock()
        # Not named `_stop`: threading.Thread already defines a `_stop()` method,
        # and shadowing it with an Event breaks `join()`.
        self._interrupt_requested = threading.Event()
        self._outcomes: Dict[str, TenantOutcome] = {}
        self._pending: Deque[Tuple[FanoutTarget, int, float]] = deque()
        self._inflight = 0
        self._peak_inflight = 0
        self._inflight_limited_events = 0
        self._capacity_rejections = 0
        self._observed_cap: Optional[int] = None
        self._threads: List[threading.Thread] = []
        self._started_at = 0.0

    def run(self) -> FanoutRunResult:
        started = self._started_at = time.monotonic()
        with self._sigint_handler():
            self._schedule_all()
        wall_seconds = time.monotonic() - started

        return FanoutRunResult(
            outcomes=[self._outcomes[target.name] for target in self._targets],
            pace_seconds=self._pacing.pace,
            max_inflight=self._pacing.max_inflight,
            calibrated_from_seconds=self._pacing.calibrated_from_seconds,
            pace_samples=self._pacing.pace_samples,
            peak_inflight=self._peak_inflight,
            inflight_limited_events=self._inflight_limited_events,
            capacity_rejections=self._capacity_rejections,
            observed_cap=self._observed_cap,
            wall_seconds=wall_seconds,
            interrupted=self._interrupt_requested.is_set(),
        )

    @property
    def interrupt_requested(self) -> bool:
        return self._interrupt_requested.is_set()

    def request_interrupt(self) -> None:
        """Ask the run to stop starting new work and drain what is in flight."""
        self._interrupt_requested.set()

    def _outcome_for(self, target: FanoutTarget) -> TenantOutcome:
        outcome = self._outcomes.get(target.name)
        if outcome is None:
            outcome = TenantOutcome(
                name=target.name,
                project_name=self._project_names.get(target.name, ""),
            )
            self._outcomes[target.name] = outcome
        return outcome

    def _schedule_all(self) -> None:
        """Start every tenant, no closer together than the pace allows."""
        with self._lock:
            for target in self._targets:
                if target.name not in self._outcomes:
                    self._pending.append((target, 1, 0.0))

        last_start = 0.0
        last_progress = time.monotonic()

        while True:
            with self._lock:
                pending_count = len(self._pending)
                inflight = self._inflight
            if pending_count == 0 and inflight == 0:
                break

            now = time.monotonic()
            if now - last_progress >= self._progress_interval:
                self._emit_progress()
                last_progress = now

            if self._interrupt_requested.is_set():
                self._drain_pending()
                self._join_threads()
                break

            started_one = False
            with self._lock:
                ready = self._pending and self._pending[0][2] <= now
                pace_satisfied = now - last_start >= self._pacing.pace
                if ready and pace_satisfied:
                    if self._inflight < self._pacing.max_inflight:
                        target, attempt, _ = self._pending.popleft()
                        self._start_worker(target, attempt)
                        started_one = True
                    else:
                        # Ready to start and allowed by the pace, held back only
                        # by the ceiling. Counting this distinguishes a run that
                        # is ceiling-bound from one that is pace-bound.
                        self._inflight_limited_events += 1

            if started_one:
                last_start = now
            else:
                time.sleep(_LOOP_SLEEP_SECONDS)

        self._join_threads()
        self._emit_progress()

    def _start_worker(self, target: FanoutTarget, attempt: int) -> None:
        """Begin one attempt. Caller holds the lock."""
        self._inflight += 1
        self._peak_inflight = max(self._peak_inflight, self._inflight)
        thread = threading.Thread(
            target=self._worker,
            args=(target, attempt),
            name=f"dcm-fanout-{target.name}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()

    def _worker(self, target: FanoutTarget, attempt: int) -> None:
        try:
            self._execute(target, attempt, paced=True)
        finally:
            with self._lock:
                self._inflight -= 1

    def _execute(self, target: FanoutTarget, attempt: int, paced: bool) -> None:
        result = self._run_attempt(self._template, target)
        outcome = self._outcome_for(target)

        with self._lock:
            outcome.attempts = attempt
            outcome.duration_seconds = result.duration_seconds
            if result.observed_cap and self._observed_cap is None:
                self._observed_cap = result.observed_cap
            if result.retryable:
                self._capacity_rejections += 1
                outcome.capacity_rejections += 1

            if result.succeeded:
                outcome.status = STATUS_SUCCESS
                outcome.operations = result.operations
                outcome.error = None
                if self._pacing.calibrate:
                    # Only successful attempts feed the mean. A capacity
                    # rejection returns in a fraction of the time a real
                    # invocation takes, so counting one would tighten the pace at
                    # exactly the moment the run needs it loosened.
                    observe_duration(self._pacing, result.duration_seconds)
                return

            outcome.error = result.error
            retries_left = attempt <= self._retry_attempts
            if (
                paced
                and result.retryable
                and retries_left
                and not self._interrupt_requested.is_set()
            ):
                delay = backoff_delay(attempt)
                log.info(
                    "Requeueing tenant after a capacity rejection "
                    "(target=%s, attempt=%d, delay_seconds=%.1f).",
                    target.name,
                    attempt,
                    delay,
                )
                outcome.status = "RETRYING"
                self._pending.append((target, attempt + 1, time.monotonic() + delay))
                return

            outcome.status = STATUS_FAILED

    def _drain_pending(self) -> None:
        """On interrupt, mark whatever never started as skipped."""
        with self._lock:
            while self._pending:
                target, _, _ = self._pending.popleft()
                outcome = self._outcome_for(target)
                if outcome.status in ("PENDING", "RETRYING"):
                    outcome.status = STATUS_SKIPPED
                    outcome.error = outcome.error or "Not started (run interrupted)."

    def _join_threads(self) -> None:
        for thread in list(self._threads):
            thread.join()

    def _emit_progress(self) -> None:
        if not self._on_progress:
            return
        with self._lock:
            outcomes = list(self._outcomes.values())
            inflight = self._inflight
        succeeded = sum(1 for outcome in outcomes if outcome.status == STATUS_SUCCESS)
        failed = sum(1 for outcome in outcomes if outcome.status == STATUS_FAILED)
        self._on_progress(
            ProgressSnapshot(
                total=len(self._targets),
                completed=succeeded + failed,
                succeeded=succeeded,
                failed=failed,
                inflight=inflight,
                pace_seconds=self._pacing.pace,
                elapsed_seconds=time.monotonic() - self._started_at,
            )
        )

    class _SigintScope:
        """Installs a SIGINT handler that asks the run to drain rather than die."""

        def __init__(self, scheduler: "FanoutScheduler") -> None:
            self._scheduler = scheduler
            self._previous: Union[
                Callable[[int, Optional[FrameType]], Any], int, None
            ] = None
            self._installed = False

        def __enter__(self) -> "FanoutScheduler._SigintScope":
            try:
                self._previous = signal.getsignal(signal.SIGINT)
                signal.signal(signal.SIGINT, self._handle)
                self._installed = True
            except ValueError:
                # signal.signal() only works on the main thread. Off it, the run
                # simply has no interrupt handling of its own.
                log.debug("Could not install a SIGINT handler off the main thread.")
            return self

        def _handle(self, signum: int, frame: Optional[FrameType]) -> None:
            if self._scheduler.interrupt_requested:
                # A second interrupt means the user wants out now.
                raise KeyboardInterrupt
            self._scheduler.request_interrupt()

        def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
            if self._installed and self._previous is not None:
                signal.signal(signal.SIGINT, self._previous)

    def _sigint_handler(self) -> "FanoutScheduler._SigintScope":
        return self._SigintScope(self)
