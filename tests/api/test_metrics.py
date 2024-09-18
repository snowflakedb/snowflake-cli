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

from snowflake.cli.api.metrics import CLIMetrics


def test_metrics_no_counters():
    # given
    expected = {}
    metrics = CLIMetrics()

    # when

    # then
    assert metrics.counters == expected


def test_metrics_set_one_counter():
    # given
    expected = {"counter1": 1}
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 1)

    # then
    assert metrics.counters == expected


def test_metrics_add_new_counter():
    # given
    expected = {"counter1": 2}
    metrics = CLIMetrics()

    # when
    metrics.add_counter("counter1", 2)

    # then
    assert metrics.counters == expected


def test_metrics_add_existing_counter():
    # given
    expected = {"counter1": 3}
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 2)
    metrics.add_counter(name="counter1", value=1)

    # then
    assert metrics.counters == expected


def test_metrics_set_multiple_counters():
    # given
    expected = {"counter1": 1, "counter2": 2}
    metrics = CLIMetrics()

    # when
    metrics.set_counter("counter1", 1)
    metrics.set_counter("counter2", 0)
    metrics.set_counter(name="counter2", value=2)

    # then
    assert metrics.counters == expected
