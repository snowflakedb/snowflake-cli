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
from contextlib import contextmanager
from itertools import count
from typing import Dict, List, Optional, Union


class CLIMetricsInvalidUsageError(RuntimeError):
    """
    Indicative of bug in the code where a call to CLIMetrics was made erroneously
    """

    def __init__(self, message: str):
        super().__init__(message)


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


class _CLIMetricsStep:
    """
    class for holding metrics step data and encapsulating related operations
    """

    _id_counter = count(start=1, step=1)

    # keys for dict representation
    ID_KEY = "id"
    NAME_KEY = "name"
    PARENT_KEY = "parent"
    PARENT_ID_KEY = "parent_id"
    START_TIME_KEY = "start_time"
    EXECUTION_TIME_KEY = "execution_time"
    ERROR_KEY = "error"

    def __init__(self, name: str, parent: _CLIMetricsStep | None = None):
        if not name:
            raise CLIMetricsInvalidUsageError("step name must not be empty")

        # to disambiguate steps with same name
        self._step_id: int = next(self._id_counter)
        self._name: str = name

        self._parent: Optional[str] = parent.name if parent is not None else None
        self._parent_id: Optional[int] = parent.step_id if parent is not None else None

        self._monotonic_start: Optional[
            float
        ] = None  # monotonic time for execution time
        self._start_time: Optional[float] = None  # timestamp
        self._execution_time: Optional[float] = None

        self._error: Optional[str] = None

    def start(self) -> None:
        if self._monotonic_start:
            raise CLIMetricsInvalidUsageError("step has already started")
        self._start_time = time.time()
        self._monotonic_start = time.monotonic()

    def end(self, error: Optional[BaseException] = None) -> None:
        if self._execution_time:
            raise CLIMetricsInvalidUsageError("step has already ended")
        if not self._monotonic_start:
            raise CLIMetricsInvalidUsageError("step has not started")

        if error:
            self._error = type(error).__name__

        self._execution_time = time.monotonic() - self._monotonic_start

    @property
    def step_id(self) -> int:
        return self._step_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> Optional[str]:
        return self._parent

    @property
    def parent_id(self) -> Optional[int]:
        return self._parent_id

    @property
    def start_time(self) -> float:
        if not self._start_time:
            raise CLIMetricsInvalidUsageError("start_time accessed before step started")
        return self._start_time

    @property
    def execution_time(self) -> float:
        if not self._execution_time:
            raise CLIMetricsInvalidUsageError(
                "execution_time accessed before step ended"
            )
        return self._execution_time

    @property
    def error(self) -> Optional[str]:
        """
        name of the error class that was raised during the execution of this step
        """
        return self._error

    def to_dict(self) -> Dict[str, Union[str, int, float, None]]:
        return {
            self.ID_KEY: self.step_id,
            self.NAME_KEY: self.name,
            self.PARENT_KEY: self.parent,
            self.PARENT_ID_KEY: self.parent_id,
            self.START_TIME_KEY: self.start_time,
            self.EXECUTION_TIME_KEY: self.execution_time,
            self.ERROR_KEY: self.error,
        }


class CLIMetrics:
    """
    Class to track various metrics across the execution of a command
    """

    def __init__(self):
        self._counters: Dict[str, int] = {}
        # stack of current steps as command is executing
        self._executing_steps: List[_CLIMetricsStep] = []
        # list of finished steps for telemetry to process
        self._finished_steps: List[_CLIMetricsStep] = []

    def __eq__(self, other):
        if isinstance(other, CLIMetrics):
            return (
                self._counters == other._counters
                and self._finished_steps == other._finished_steps
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
    def _current_step(self) -> Optional[_CLIMetricsStep]:
        return self._executing_steps[-1] if len(self._executing_steps) > 0 else None

    @contextmanager
    def track_step(self, name: str):
        """
        Recommended general use API for tracking a step throughout the execution of a command
        Assumes that parent steps contain the entirety of their child steps

        start_step and end_step are exposed in case the caller needs more flexibility

        :raises CliMetricsInvalidUsageError: if the step name is empty
        """
        step_id = self.start_step(name)
        try:
            yield
        except BaseException as err:
            self.end_step(step_id=step_id, error=err)
            raise
        else:
            self.end_step(step_id=step_id)

    def start_step(self, name: str) -> int:
        """
        Command for manually tracking steps throughout the execution of a command
        E.g. through different scopes/files for when a context manager is not enough
        Requires a corresponding call to end_step with the (optional) exception caught

        :returns: id of the started step which can be passed to end_step

        :raises CliMetricsInvalidUsageError: if the step name is empty
        """
        parent_step = self._current_step
        new_step = _CLIMetricsStep(name, parent_step)
        self._executing_steps.append(new_step)
        new_step.start()
        return new_step.step_id

    def end_step(
        self,
        step_id: Optional[int] = None,
        step_name: Optional[str] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        """
        Manually ends either the most recently started step or the step with the provided id/name

        Context managers can automatically check if there were errors, but if manually
        starting/ending steps, then the caller will need to catch and pass the error themselves

        :raises CliMetricsInvalidUsageError: if step could not be found or there are no executing steps
        """
        if step_id is not None:
            found_step = next(
                (step for step in self._executing_steps if step.step_id == step_id),
                None,
            )
        elif step_name is not None:
            found_step = next(
                (
                    step
                    for step in reversed(self._executing_steps)
                    if step.name == step_name
                ),
                None,
            )
        else:
            found_step = self._current_step

        if not found_step:
            if step_id or step_name:
                raise CLIMetricsInvalidUsageError(
                    f"step with {'id' if step_id else 'name'} '{step_id or step_name}' could not be ended because it could not be found"
                )
            else:
                raise CLIMetricsInvalidUsageError(
                    "current step could not be ended because no steps are executing"
                )

        found_step.end(error)
        self._executing_steps.remove(found_step)
        self._finished_steps.append(found_step)

    def flush_steps(self, error: Optional[BaseException] = None) -> None:
        """
        Useful if you are using the manual start/end steps and an error
        propagated up, requiring you to clear out all the executing steps
        """
        while self._current_step:
            self.end_step(error=error)

    @property
    def counters(self) -> Dict[str, int]:
        # return a copy of the original dict to avoid mutating the original
        return self._counters.copy()

    @property
    def steps(self) -> List[Dict[str, Union[str, int, float, None]]]:
        """
        returns the finished steps tracked throughout a command, sorted by start time
        """
        return [
            step.to_dict()
            for step in sorted(self._finished_steps, key=lambda step: step.start_time)
        ]
