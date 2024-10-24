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
import time

import pytest
from snowflake.cli.api.metrics import (
    CLIMetrics,
    CLIMetricsInvalidUsageError,
    _CLIMetricsStep,
)


def test_metrics_steps_initialization_empty():
    # given
    metrics = CLIMetrics()

    # when

    # then
    assert metrics.steps == []


def test_metrics_steps_time_is_valid():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("step1"):
        pass

    # then
    assert len(metrics.steps) == 1
    step1 = metrics.steps[0]
    assert step1[_CLIMetricsStep.NAME_KEY] == "step1"


def test_metrics_steps_name_is_valid():
    # given
    metrics = CLIMetrics()
    time_before_step = time.time()

    # when
    with metrics.track_step("step1"):
        pass

    # then
    assert len(metrics.steps) == 1
    step1 = metrics.steps[0]
    assert step1[_CLIMetricsStep.START_TIME_KEY] >= time_before_step
    assert step1[_CLIMetricsStep.EXECUTION_TIME_KEY] > 0


def test_metrics_steps_error_caught():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("step1"):
        try:
            raise RuntimeError()
        except RuntimeError:
            pass

    # then
    assert len(metrics.steps) == 1
    step1 = metrics.steps[0]
    assert step1[_CLIMetricsStep.ERROR_KEY] is None


def test_metrics_steps_error_uncaught():
    # given
    metrics = CLIMetrics()

    # when
    try:
        with metrics.track_step("step1"):
            raise RuntimeError()
    except RuntimeError:
        pass

    # then
    assert len(metrics.steps) == 1
    step1 = metrics.steps[0]
    assert step1[_CLIMetricsStep.ERROR_KEY] == "RuntimeError"


def test_metrics_steps_no_parent():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("step1"):
        pass

    # then
    assert len(metrics.steps) == 1
    step1 = metrics.steps[0]
    assert step1[_CLIMetricsStep.PARENT_KEY] is None


def test_metrics_steps_with_parent_proper_names():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("parent"):
        with metrics.track_step("child"):
            pass

    # then
    assert len(metrics.steps) == 2
    parent, child = metrics.steps

    assert child[_CLIMetricsStep.NAME_KEY] == "child"
    assert child[_CLIMetricsStep.PARENT_KEY] == "parent"
    assert parent[_CLIMetricsStep.NAME_KEY] == "parent"


def test_metrics_steps_with_parent_proper_ids():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("parent"):
        with metrics.track_step("child"):
            pass

    # then
    assert len(metrics.steps) == 2
    parent, child = metrics.steps
    assert child[_CLIMetricsStep.ID_KEY] != parent[_CLIMetricsStep.ID_KEY]
    assert child[_CLIMetricsStep.PARENT_ID_KEY] == parent[_CLIMetricsStep.ID_KEY]
    assert parent[_CLIMetricsStep.PARENT_ID_KEY] is None


def test_metrics_steps_with_duplicate_names_proper_timing():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("duplicate"):
        with metrics.track_step("duplicate"):
            pass

    # then
    assert len(metrics.steps) == 2
    parent, child = metrics.steps

    assert (
        child[_CLIMetricsStep.START_TIME_KEY] > parent[_CLIMetricsStep.START_TIME_KEY]
    )
    assert (
        parent[_CLIMetricsStep.EXECUTION_TIME_KEY]
        > child[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )


def test_metrics_steps_error_caught_in_outer_step():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.track_step("parent"):
        try:
            with metrics.track_step("child"):
                raise RuntimeError()
        except RuntimeError:
            pass

    # then
    assert len(metrics.steps) == 2
    parent, child = metrics.steps
    assert parent[_CLIMetricsStep.ERROR_KEY] is None
    assert child[_CLIMetricsStep.ERROR_KEY] == "RuntimeError"


def test_metrics_steps_manual_start_and_end_overlapping_proper_parent():
    # given
    metrics = CLIMetrics()

    # when
    step1_id = metrics.start_step("step1")
    step2_id = metrics.start_step("step2")

    metrics.end_step(step_id=step2_id)
    metrics.end_step(step_id=step1_id)

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps

    assert step2[_CLIMetricsStep.PARENT_KEY] == "step1"
    assert step1[_CLIMetricsStep.ID_KEY] == step2[_CLIMetricsStep.PARENT_ID_KEY]


def test_metrics_steps_manual_end_most_recent_step():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    metrics.start_step("step2")

    metrics.end_step()
    metrics.end_step()

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    step1_end_time = (
        step1[_CLIMetricsStep.START_TIME_KEY]
        + step1[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    step2_end_time = (
        step2[_CLIMetricsStep.START_TIME_KEY]
        + step2[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    assert step1_end_time > step2_end_time


def test_metrics_steps_manual_end_step_name_proper_names():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    metrics.start_step("step2")

    metrics.end_step(step_name="step2")
    metrics.end_step(step_name="step1")

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    assert step1[_CLIMetricsStep.NAME_KEY] == "step1"
    assert step2[_CLIMetricsStep.NAME_KEY] == "step2"


def test_metrics_steps_manual_end_step_name_proper_timing():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    metrics.start_step("step2")

    metrics.end_step(step_name="step1")
    metrics.end_step(step_name="step2")

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    step1_end_time = (
        step1[_CLIMetricsStep.START_TIME_KEY]
        + step1[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    step2_end_time = (
        step2[_CLIMetricsStep.START_TIME_KEY]
        + step2[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    assert step1_end_time < step2_end_time


def test_metrics_steps_manual_end_step_id_proper_timing():
    # given
    metrics = CLIMetrics()

    # when
    step1_id = metrics.start_step("step1")
    step2_id = metrics.start_step("step2")

    metrics.end_step(step_id=step1_id)
    metrics.end_step(step_id=step2_id)

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    step1_end_time = (
        step1[_CLIMetricsStep.START_TIME_KEY]
        + step1[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    step2_end_time = (
        step2[_CLIMetricsStep.START_TIME_KEY]
        + step2[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    assert step1_end_time < step2_end_time


def test_metrics_steps_end_step_no_step_with_name_raises_error():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    with pytest.raises(CLIMetricsInvalidUsageError) as err:
        metrics.end_step(step_name="step2")

    # then
    assert err.match(
        "step with name 'step2' could not be ended because it could not be found"
    )


def test_metrics_steps_end_step_no_step_with_id_raises_error():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    with pytest.raises(CLIMetricsInvalidUsageError) as err:
        metrics.end_step(step_id=2)

    # then
    assert err.match(
        "step with id '2' could not be ended because it could not be found"
    )


def test_metrics_steps_end_step_no_executing_steps_raises_error():
    # given
    metrics = CLIMetrics()

    # when
    with pytest.raises(CLIMetricsInvalidUsageError) as err:
        metrics.end_step()

    # then
    assert err.match("current step could not be ended because no steps are executing")


def test_metrics_steps_step_name_empty_raises_error():
    # given
    metrics = CLIMetrics()

    # when
    with pytest.raises(CLIMetricsInvalidUsageError) as err:
        with metrics.track_step(""):
            pass

    # then
    assert err.match("step name must not be empty")


def test_metrics_steps_flush_empty_does_nothing():
    # given
    metrics = CLIMetrics()

    # when
    metrics.flush_steps()

    # then
    assert metrics.steps == []


def test_metrics_steps_flush_steps_with_error():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    metrics.start_step("step2")
    metrics.flush_steps(RuntimeError())

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    assert step1[_CLIMetricsStep.ERROR_KEY] == "RuntimeError"
    assert step2[_CLIMetricsStep.ERROR_KEY] == "RuntimeError"


def test_metrics_steps_flush_steps_lifo_timing():
    # given
    metrics = CLIMetrics()

    # when
    metrics.start_step("step1")
    metrics.start_step("step2")
    metrics.flush_steps()

    # then
    assert len(metrics.steps) == 2
    step1, step2 = metrics.steps
    assert (
        step1[_CLIMetricsStep.EXECUTION_TIME_KEY]
        > step2[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )

    step1_end_time = (
        step1[_CLIMetricsStep.START_TIME_KEY]
        + step1[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    step2_end_time = (
        step2[_CLIMetricsStep.START_TIME_KEY]
        + step2[_CLIMetricsStep.EXECUTION_TIME_KEY]
    )
    assert step1_end_time > step2_end_time
