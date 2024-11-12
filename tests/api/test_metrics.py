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

import uuid
from itertools import count

import pytest
from snowflake.cli.api.metrics import (
    CLIMetrics,
    CLIMetricsInvalidUsageError,
    CLIMetricsSpan,
)


def test_metrics_no_counters():
    # given
    metrics = CLIMetrics()

    # when

    # then
    assert metrics.counters == {}
    assert metrics.get_counter("counter1") is None


def test_metrics_set_one_counter():
    # given
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 1)

    # then
    assert metrics.counters == {"counter1": 1}
    assert metrics.get_counter("counter1") == 1


def test_metrics_increment_new_counter():
    # given
    metrics = CLIMetrics()

    # when
    metrics.increment_counter("counter1")

    # then
    assert metrics.counters == {"counter1": 1}
    assert metrics.get_counter("counter1") == 1


def test_metrics_increment_existing_counter():
    # given
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 2)
    metrics.increment_counter(name="counter1", value=2)

    # then
    assert metrics.counters == {"counter1": 4}
    assert metrics.get_counter("counter1") == 4


def test_metrics_set_multiple_counters():
    # given
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 1)
    metrics.set_counter("counter2", 0)
    metrics.set_counter(name="counter2", value=2)

    # then
    assert metrics.counters == {"counter1": 1, "counter2": 2}
    assert metrics.get_counter("counter1") == 1
    assert metrics.get_counter("counter2") == 2


def test_metrics_set_default_new_counter():
    # given
    metrics = CLIMetrics()

    # when
    metrics.set_counter_default("c1", 3)

    # then
    assert metrics.counters == {"c1": 3}


def test_metrics_set_default_existing_counter():
    # given
    metrics = CLIMetrics()

    # when
    metrics.set_counter("c2", 2)
    metrics.set_counter_default("c2", 1)

    # then
    assert metrics.counters == {"c2": 2}


# helper for testing span limits
def create_spans(metrics: CLIMetrics, width: int, depth: int):
    counter = count()

    def create_span(num_spans: int):
        if num_spans <= 0:
            return

        with metrics.span(f"span-{next(counter)}"):
            create_span(num_spans - 1)

    for _ in range(width):
        create_span(num_spans=depth)


def test_metrics_spans_initialization_empty():
    # given
    metrics = CLIMetrics()

    # when
    assert metrics.current_span is None

    # then
    assert metrics.completed_spans == []
    assert metrics.num_spans_past_depth_limit == 0
    assert metrics.num_spans_past_total_limit == 0


def test_metrics_spans_single_span_no_error_or_parent():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.span("span1") as span1:
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
    assert span1_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 1
    assert span1_dict[CLIMetricsSpan.SPAN_DEPTH_KEY] == 1
    assert span1_dict[CLIMetricsSpan.TRIMMED_KEY] == False


def test_metrics_spans_finish_early_is_idempotent():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.span("span1") as span1:
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
    with metrics.span("parent") as parent:
        assert metrics.current_span is parent

        with metrics.span("child") as child:
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
        > child_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )
    assert (
        parent_dict[CLIMetricsSpan.START_TIME_KEY]
        < child_dict[CLIMetricsSpan.START_TIME_KEY]
    )

    assert parent_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 2
    assert child_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 1

    assert parent_dict[CLIMetricsSpan.SPAN_DEPTH_KEY] == 1
    assert child_dict[CLIMetricsSpan.SPAN_DEPTH_KEY] == 2

    assert (
        parent_dict[CLIMetricsSpan.TRIMMED_KEY]
        == child_dict[CLIMetricsSpan.TRIMMED_KEY]
        == False
    )


def test_metrics_spans_parent_with_two_children_same_name():
    # given
    metrics = CLIMetrics()

    # when
    with metrics.span("parent") as parent:
        assert metrics.current_span is parent

        with metrics.span("child") as child1:
            assert metrics.current_span is child1

        assert metrics.current_span is parent

        with metrics.span("child") as child2:
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
        > child1_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )
    assert (
        parent_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
        > child2_dict[CLIMetricsSpan.EXECUTION_TIME_KEY]
    )

    assert (
        parent_dict[CLIMetricsSpan.START_TIME_KEY]
        < child1_dict[CLIMetricsSpan.START_TIME_KEY]
        < child2_dict[CLIMetricsSpan.START_TIME_KEY]
    )

    assert parent_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 3
    assert (
        child1_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY]
        == child1_dict[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY]
        == 1
    )

    assert parent_dict[CLIMetricsSpan.SPAN_DEPTH_KEY] == 1
    assert (
        child1_dict[CLIMetricsSpan.SPAN_DEPTH_KEY]
        == child1_dict[CLIMetricsSpan.SPAN_DEPTH_KEY]
        == 2
    )

    assert (
        parent_dict[CLIMetricsSpan.TRIMMED_KEY]
        == child1_dict[CLIMetricsSpan.TRIMMED_KEY]
        == child2_dict[CLIMetricsSpan.TRIMMED_KEY]
        == False
    )


def test_metrics_spans_error_is_propagated():
    # given
    metrics = CLIMetrics()

    # when
    with pytest.raises(RuntimeError):
        with metrics.span("step1"):
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
        with metrics.span(""):
            pass

    # then
    assert err.match("span name must not be empty")


def test_metrics_spans_passing_depth_limit_should_add_to_counter_and_not_emit():
    # given
    metrics = CLIMetrics()

    # when
    create_spans(metrics, width=1, depth=CLIMetrics.SPAN_DEPTH_LIMIT + 3)

    # then
    assert metrics.num_spans_past_total_limit == 0
    assert metrics.num_spans_past_depth_limit == 3

    completed_spans = metrics.completed_spans
    assert len(completed_spans) == CLIMetrics.SPAN_DEPTH_LIMIT

    assert completed_spans[-1][CLIMetricsSpan.TRIMMED_KEY] == True
    assert completed_spans[-2][CLIMetricsSpan.TRIMMED_KEY] == False

    assert (
        completed_spans[-1][CLIMetricsSpan.SPAN_DEPTH_KEY]
        == CLIMetrics.SPAN_DEPTH_LIMIT
    )

    # should match the total number of spans created regardless of limit
    assert (
        completed_spans[0][CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY]
        == CLIMetrics.SPAN_DEPTH_LIMIT + 3
    )
    # the 3 spans created under this one that went beyond the limit + itself
    assert completed_spans[-1][CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 3 + 1


def test_metrics_spans_passing_total_limit_are_collected_breadth_first():
    # given
    metrics = CLIMetrics()

    # when
    create_spans(metrics, width=CLIMetrics.SPAN_TOTAL_LIMIT + 1, depth=2)

    # then
    # extra (1 * 2) spans created past the limit
    assert metrics.num_spans_past_total_limit == CLIMetrics.SPAN_TOTAL_LIMIT + 2
    assert metrics.num_spans_past_depth_limit == 0

    completed_spans = metrics.completed_spans

    assert len(completed_spans) == CLIMetrics.SPAN_TOTAL_LIMIT

    assert all(span[CLIMetricsSpan.PARENT_KEY] is None for span in completed_spans)
    assert all(span[CLIMetricsSpan.TRIMMED_KEY] == True for span in completed_spans)
    assert all(
        span[CLIMetricsSpan.SPAN_COUNT_IN_SUBTREE_KEY] == 2 for span in completed_spans
    )


def test_metrics_spans_passing_both_limits_should_add_to_both_counts():
    # given
    metrics = CLIMetrics()

    # when
    create_spans(metrics, width=CLIMetrics.SPAN_TOTAL_LIMIT, depth=1)
    create_spans(metrics, width=1, depth=CLIMetrics.SPAN_DEPTH_LIMIT + 10)

    # then
    assert metrics.num_spans_past_total_limit == CLIMetrics.SPAN_DEPTH_LIMIT + 10
    assert metrics.num_spans_past_depth_limit == 10
