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
import uuid

import pytest
from snowflake.cli.api.metrics import (
    CLIMetrics,
    CLIMetricsInvalidUsageError,
    CLIMetricsSpan,
)


# helper for testing span depth limit edge case
def create_nested_spans_recursively(metrics: CLIMetrics, num_spans: int = 1) -> None:
    @metrics.start_span("nested_span")
    def create_span():
        nonlocal num_spans
        num_spans -= 1

        if num_spans > 0:
            create_span()

    create_span()


# helper for testing span total limit edge case
def create_spans_sequentially(metrics: CLIMetrics, num_spans: int = 1) -> None:
    while num_spans > 0:
        with metrics.start_span("sequential_span"):
            pass
        num_spans -= 1


def test_metrics_spans_initialization_empty():
    # given
    metrics = CLIMetrics()

    # when
    assert metrics.current_span is None

    # then
    assert metrics.completed_spans == []
    assert metrics.num_spans_past_total_limit == 0
    assert metrics.num_spans_past_depth_limit == 0


def test_metrics_spans_single_span_no_error_or_parent():
    # given
    metrics = CLIMetrics()

    # when
    time.sleep(0.001)
    with metrics.start_span("span1") as span1:
        time.sleep(0.001)
        assert metrics.current_span is span1

    assert metrics.current_span is None

    # then
    assert len(metrics.completed_spans) == 1
    span1_dict = metrics.completed_spans[0]

    assert uuid.UUID(
        hex=span1_dict[CLIMetricsSpan.ID_KEY]
    )  # will raise ValueError if not valid uuid
    assert span1_dict[CLIMetricsSpan.NAME_KEY] == "span1"
    assert span1_dict[CLIMetricsSpan.START_TIME_KEY] > 0
    assert span1_dict[CLIMetricsSpan.EXECUTION_TIME_KEY] > 0
    assert span1_dict[CLIMetricsSpan.ERROR_KEY] is None
    assert span1_dict[CLIMetricsSpan.PARENT_KEY] is None
    assert span1_dict[CLIMetricsSpan.PARENT_ID_KEY] is None


def test_metrics_spans_finish_early_is_idempotent():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.start_span("span1") as span1:
        start_time = span1.start_time
        span1.finish()
        execution_time = span1.execution_time

    # then
    assert len(metrics.completed_spans) == 1
    span1_dict = metrics.completed_spans[0]
    assert span1_dict[CLIMetricsSpan.START_TIME_KEY] == start_time
    assert span1_dict[CLIMetricsSpan.EXECUTION_TIME_KEY] == execution_time


def test_metrics_spans_parent_with_one_child():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.start_span("parent") as parent:
        assert metrics.current_span is parent

        with metrics.start_span("child") as child:
            assert metrics.current_span is child

        assert metrics.current_span is parent

    assert metrics.current_span is None

    # then
    assert len(metrics.completed_spans) == 2
    parent_dict, child_dict = metrics.completed_spans

    assert parent_dict[CLIMetricsSpan.ID_KEY] != child_dict[CLIMetricsSpan.ID_KEY]
    assert (
        child_dict[CLIMetricsSpan.PARENT_ID_KEY] == parent_dict[CLIMetricsSpan.ID_KEY]
    )

    assert child_dict[CLIMetricsSpan.NAME_KEY] == "child"
    assert (
        child_dict[CLIMetricsSpan.PARENT_KEY]
        == parent_dict[CLIMetricsSpan.NAME_KEY]
        == "parent"
    )

    assert (
        parent_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
        >= child_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )
    assert (
        parent_dict[CLIMetricsSpan.START_TIME_KEY]
        <= child_dict[CLIMetricsSpan.START_TIME_KEY]
    )


def test_metrics_spans_parent_with_two_children_same_name():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.start_span("parent") as parent:
        assert metrics.current_span is parent

        with metrics.start_span("child") as child1:
            assert metrics.current_span is child1

        assert metrics.current_span is parent

        with metrics.start_span("child") as child2:
            assert metrics.current_span is child2

        assert metrics.current_span is parent

    assert metrics.current_span is None

    # then
    assert len(metrics.completed_spans) == 3
    parent_dict, child1_dict, child2_dict = metrics.completed_spans

    assert (
        parent_dict[CLIMetricsSpan.ID_KEY]
        != child1_dict[CLIMetricsSpan.ID_KEY]
        != child2_dict[CLIMetricsSpan.ID_KEY]
    )

    assert (
        child1_dict[CLIMetricsSpan.PARENT_ID_KEY]
        == child2_dict[CLIMetricsSpan.PARENT_ID_KEY]
        == parent_dict[CLIMetricsSpan.ID_KEY]
    )

    assert (
        child1_dict[CLIMetricsSpan.NAME_KEY]
        == child2_dict[CLIMetricsSpan.NAME_KEY]
        == "child"
    )

    assert (
        child1_dict[CLIMetricsSpan.PARENT_KEY]
        == child2_dict[CLIMetricsSpan.PARENT_KEY]
        == parent_dict[CLIMetricsSpan.NAME_KEY]
        == "parent"
    )

    assert (
        parent_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
        >= child1_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )
    assert (
        parent_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
        >= child2_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )

    assert (
        parent_dict[CLIMetricsSpan.START_TIME_KEY]
        <= child1_dict[CLIMetricsSpan.START_TIME_KEY]
        <= child2_dict[CLIMetricsSpan.START_TIME_KEY]
    )


def test_metrics_spans_error_is_propagated():
    # given
    metrics = CLIMetrics()

    # when
    with pytest.raises(RuntimeError):
        with metrics.start_span("step1"):
            raise RuntimeError()

    # then
    assert len(metrics.completed_spans) == 1
    step1_dict = metrics.completed_spans[0]
    assert step1_dict[CLIMetricsSpan.ERROR_KEY] == "RuntimeError"


def test_metrics_spans_empty_name_raises_error():
    # given
    metrics = CLIMetrics()

    # when
    with pytest.raises(CLIMetricsInvalidUsageError) as err:
        with metrics.start_span(""):
            pass

    # then
    assert err.match("step name must not be empty")


def test_metrics_spans_passing_depth_limit_should_add_to_counter_and_not_emit():
    # given
    metrics = CLIMetrics()

    # when
    create_nested_spans_recursively(
        metrics, num_spans=CLIMetrics.IN_PROGRESS_SPANS_DEPTH_LIMIT + 3
    )

    # then
    assert len(metrics.completed_spans) == CLIMetrics.IN_PROGRESS_SPANS_DEPTH_LIMIT
    assert metrics.num_spans_past_depth_limit == 3


def test_metrics_spans_passing_total_limit_should_add_to_counter_and_not_emit():
    # given
    metrics = CLIMetrics()

    # when
    create_spans_sequentially(
        metrics, num_spans=CLIMetrics.COMPLETED_SPANS_TOTAL_LIMIT + 5
    )

    # then
    assert len(metrics.completed_spans) == CLIMetrics.COMPLETED_SPANS_TOTAL_LIMIT
    assert metrics.num_spans_past_total_limit == 5


def test_metrics_spans_passing_total_and_depth_limit_should_add_to_both_counters_and_not_emit():
    # given
    metrics = CLIMetrics()

    # when
    # the extra 10 spans are dropped from both the in progress and completed spans lists
    create_nested_spans_recursively(
        metrics, num_spans=CLIMetrics.IN_PROGRESS_SPANS_DEPTH_LIMIT + 10
    )
    create_spans_sequentially(metrics, num_spans=CLIMetrics.COMPLETED_SPANS_TOTAL_LIMIT)

    # then
    assert len(metrics.completed_spans) == CLIMetrics.COMPLETED_SPANS_TOTAL_LIMIT
    assert (
        metrics.num_spans_past_total_limit == CLIMetrics.IN_PROGRESS_SPANS_DEPTH_LIMIT
    )
    assert metrics.num_spans_past_depth_limit == 10
