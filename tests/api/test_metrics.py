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
