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
from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import ClassVar, Dict, Iterator, List, Optional


class CLIMetricsInvalidUsageError(RuntimeError):
    """
    Indicative of bug in the code where a call to CLIMetrics was made erroneously

    We do not want metrics errors to break the execution of commands,
    so only raise this error in the event that an invariant was broken during setup
    """

    pass


class _TypePrefix:
    FEATURES = "features"


class _DomainPrefix:
    GLOBAL = "global"
    APP = "app"
    SQL = "sql"


class CLICounterField:
    """
    for each counter field we're adopting a convention of
    <type>.<domain>.<name>
    for example, if we're tracking a global feature, then the field name would be
    features.global.feature_name

    The metrics API is implemented to be generic, but we are adopting a convention
    for feature tracking with the following model for a given command execution:
    * counter not present -> feature is not available
    * counter == 0 -> feature is available, but not used
    * counter == 1 -> feature is used
    this makes it easy to compute percentages for feature dashboards in Snowsight
    """

    TEMPLATES_PROCESSOR = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.GLOBAL}.templates_processor"
    )
    SQL_TEMPLATES = f"{_TypePrefix.FEATURES}.{_DomainPrefix.SQL}.sql_templates"
    PDF_TEMPLATES = f"{_TypePrefix.FEATURES}.{_DomainPrefix.GLOBAL}.pdf_templates"
    SNOWPARK_PROCESSOR = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.snowpark_processor"
    )
    POST_DEPLOY_SCRIPTS = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.post_deploy_scripts"
    )
    PACKAGE_SCRIPTS = f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.package_scripts"


@dataclass
class CLIMetricsSpan:
    """
    class for holding metrics span data and encapsulating related operations
    """

    # keys for dict representation
    ID_KEY: ClassVar[str] = "id"
    NAME_KEY: ClassVar[str] = "name"
    PARENT_KEY: ClassVar[str] = "parent"
    PARENT_ID_KEY: ClassVar[str] = "parent_id"
    START_TIME_KEY: ClassVar[str] = "start_time"
    EXECUTION_TIME_KEY: ClassVar[str] = "execution_time"
    ERROR_KEY: ClassVar[str] = "error"

    # ensure we get unique ids for each step for the parent-child link in case of steps with the same name
    step_id: str = field(init=False, default_factory=lambda: uuid.uuid4().hex)
    name: str
    # start time of this step relative to when the command first started executing
    start_time: float
    parent: Optional[CLIMetricsSpan] = field(default=None)
    execution_time: Optional[float] = field(init=False, default=None)
    error: Optional[BaseException] = field(init=False, default=None)

    # start time of the step from the monotonic clock in order to calculate execution time
    _monotonic_start: float = field(init=False, default_factory=time.monotonic)

    def __post_init__(self):
        if not self.name:
            raise CLIMetricsInvalidUsageError("step name must not be empty")

    def finish(self, error: Optional[BaseException] = None) -> None:
        """
        Sets the execution time and (optionally) error raised for the span

        If already called, this method is a no-op
        """
        if self.execution_time is not None:
            return

        if error:
            self.error = error

        self.execution_time = time.monotonic() - self._monotonic_start

    def to_dict(self) -> Dict:
        """
        Custom dict conversion function to be used for reporting telemetry, with only the required fields
        """

        return {
            self.ID_KEY: self.step_id,
            self.NAME_KEY: self.name,
            self.START_TIME_KEY: self.start_time,
            self.PARENT_KEY: self.parent.name if self.parent is not None else None,
            self.PARENT_ID_KEY: self.parent.step_id
            if self.parent is not None
            else None,
            self.EXECUTION_TIME_KEY: self.execution_time,
            self.ERROR_KEY: type(self.error).__name__ if self.error else None,
        }


class CLIMetrics:
    """
    Class to track various metrics across the execution of a command
    """

    # limit for number of nested spans throughout execution
    IN_PROGRESS_SPANS_DEPTH_LIMIT = 5
    # limit for number of total spans being reported
    COMPLETED_SPANS_TOTAL_LIMIT = 100

    def __init__(self):
        self._counters: Dict[str, int] = {}
        # stack of in progress steps as command is executing
        self._in_progress_spans: List[CLIMetricsSpan] = []
        # list of finished steps for telemetry to process
        self._completed_spans: List[CLIMetricsSpan] = []
        # count of spans dropped due to reaching depth limit
        self.num_spans_past_depth_limit: int = 0
        # count of spans dropped due to reaching total limit
        self.num_spans_past_total_limit: int = 0
        # monotonic clock time of when this class was initialized to approximate when the command first started executing
        self.monotonic_start = time.monotonic()

    def __eq__(self, other):
        if isinstance(other, CLIMetrics):
            return (
                self._counters == other._counters
                and self._completed_spans == other._completed_spans
            )
        return False

    def get_counter(self, name: str) -> Optional[int]:
        return self._counters.get(name)

    def set_counter(self, name: str, value: int) -> None:
        self._counters[name] = value

    def set_counter_default(self, name: str, value: int) -> None:
        """
        sets the counter if it does not already exist
        """
        if name not in self._counters:
            self.set_counter(name, value)

    def increment_counter(self, name: str, value: int = 1) -> None:
        if name not in self._counters:
            self.set_counter(name, value)
        else:
            self._counters[name] += value

    @property
    def current_span(self) -> Optional[CLIMetricsSpan]:
        return self._in_progress_spans[-1] if len(self._in_progress_spans) > 0 else None

    @contextmanager
    def start_span(self, name: str) -> Iterator[CLIMetricsSpan]:
        """
        Starts a new span that tracks various metrics throughout its execution

        Assumes that parent steps contain the entirety of their child steps
        Parent steps are automatically populated as the most recently executed step, like a stack

        Only one span can be active at a time; once one span finishes its parent becomes the active span

        Spans are not emitted in telemetry if depth/total limits are exceeded

        :raises CliMetricsInvalidUsageError: if the step name is empty
        """
        new_step = CLIMetricsSpan(
            name=name,
            start_time=time.monotonic() - self.monotonic_start,
            parent=self.current_span,
        )

        if len(self._in_progress_spans) >= self.IN_PROGRESS_SPANS_DEPTH_LIMIT:
            self.num_spans_past_depth_limit += 1
        else:
            self._in_progress_spans.append(new_step)

        try:
            yield new_step
        except BaseException as err:
            new_step.finish(error=err)
            raise
        else:
            new_step.finish()
        finally:
            if len(self._completed_spans) >= self.COMPLETED_SPANS_TOTAL_LIMIT:
                self.num_spans_past_total_limit += 1

            if new_step in self._in_progress_spans:
                self._in_progress_spans.remove(new_step)

                if len(self._completed_spans) < self.COMPLETED_SPANS_TOTAL_LIMIT:
                    self._completed_spans.append(new_step)

    @property
    def counters(self) -> Dict[str, int]:
        # return a copy of the original dict to avoid mutating the original
        return self._counters.copy()

    @property
    def completed_spans(self) -> List[Dict]:
        """
        returns the completed spans tracked throughout a command, sorted by start time, for reporting telemetry
        """
        return [
            step.to_dict()
            for step in sorted(self._completed_spans, key=lambda step: step.start_time)
        ]
