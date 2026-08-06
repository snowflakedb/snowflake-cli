# Copyright (c) 2026 Snowflake Inc.
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

"""Unit tests for the pure parsing / formatting helpers in ``apps.events``."""

import json
from datetime import datetime, timezone

import pytest
from snowflake.cli._plugins.apps.events import (
    EventStream,
    MetricCategory,
    format_bytes,
    format_log_lines,
    parse_event_stream,
    parse_lifecycle_records,
    parse_metric_category,
    parse_metric_records,
    resolve_time_window,
)
from snowflake.cli.api.exceptions import CliError

_NOW = datetime(2026, 7, 16, 23, 0, 0, tzinfo=timezone.utc)


class TestParseEventStream:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, EventStream.LOG),
            ("", EventStream.LOG),
            ("log", EventStream.LOG),
            ("LOG", EventStream.LOG),
            ("metric", EventStream.METRIC),
            ("Metric", EventStream.METRIC),
            ("lifecycle", EventStream.LIFECYCLE),
        ],
    )
    def test_valid(self, value, expected):
        assert parse_event_stream(value) is expected

    def test_lifecycle_maps_to_event_type(self):
        assert EventStream.LIFECYCLE.event_table_type == "EVENT"
        assert EventStream.METRIC.event_table_type == "METRIC"
        assert EventStream.LOG.event_table_type == "LOG"

    def test_invalid(self):
        with pytest.raises(CliError, match="Invalid --type 'span'"):
            parse_event_stream("span")


class TestParseMetricCategory:
    def test_none(self):
        assert parse_metric_category(None) is None

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("cpu", MetricCategory.CPU),
            ("MEMORY", MetricCategory.MEMORY),
            ("network", MetricCategory.NETWORK),
        ],
    )
    def test_valid(self, value, expected):
        assert parse_metric_category(value) is expected

    def test_invalid(self):
        with pytest.raises(CliError, match="Invalid --metric 'disk'"):
            parse_metric_category("disk")


class TestResolveTimeWindow:
    def test_no_bounds_uses_default_lookback(self):
        start, end = resolve_time_window(None, None, now=_NOW)
        assert end == "2026-07-16T23:00:00Z"
        assert start == "2026-07-16T22:00:00Z"

    def test_since_only_ends_now(self):
        start, end = resolve_time_window("6h", None, now=_NOW)
        assert start == "2026-07-16T17:00:00Z"
        assert end == "2026-07-16T23:00:00Z"

    def test_until_only_defaults_start_one_lookback_before(self):
        start, end = resolve_time_window(None, "30m", now=_NOW)
        assert end == "2026-07-16T22:30:00Z"
        assert start == "2026-07-16T21:30:00Z"

    @pytest.mark.parametrize(
        "shorthand, expected_start",
        [
            ("30m", "2026-07-16T22:30:00Z"),
            ("6h", "2026-07-16T17:00:00Z"),
            ("2d", "2026-07-14T23:00:00Z"),
            ("1w", "2026-07-09T23:00:00Z"),
        ],
    )
    def test_relative_shorthand(self, shorthand, expected_start):
        start, _ = resolve_time_window(shorthand, None, now=_NOW)
        assert start == expected_start

    def test_absolute_timestamps(self):
        start, end = resolve_time_window(
            "2026-07-16 18:00:00", "2026-07-16 23:59:59", now=_NOW
        )
        assert start == "2026-07-16T18:00:00Z"
        assert end == "2026-07-16T23:59:59Z"

    def test_absolute_date_only(self):
        start, _ = resolve_time_window("2026-07-15", None, now=_NOW)
        assert start == "2026-07-15T00:00:00Z"

    def test_invalid_value(self):
        with pytest.raises(CliError, match="Could not parse time value"):
            resolve_time_window("yesterday", None, now=_NOW)

    def test_start_after_end(self):
        with pytest.raises(CliError, match="start of the requested window is after"):
            resolve_time_window("2026-07-16 23:00:00", "2026-07-16 18:00:00", now=_NOW)


class TestFormatBytes:
    @pytest.mark.parametrize(
        "num, expected",
        [
            (10737418240, "10.0 GiB"),
            (536870912, "512.0 MiB"),
            (196517888, "187.4 MiB"),
            (0, "0 B"),
            (512, "512 B"),
        ],
    )
    def test_format(self, num, expected):
        assert format_bytes(num) == expected


# Positional METRIC tuples: [ts, name, value, unit, instance, container, ...].
def _metric_row(ts, name, value, unit, container="runner", instance="0"):
    return [ts, name, value, unit, instance, container, None, "...", "..."]


class TestParseMetricRecords:
    def _payload(self):
        return json.dumps(
            [
                _metric_row(
                    "1784242528.051", "container.memory.limit", "10737418240", "byte"
                ),
                _metric_row(
                    "1784242528.051", "container.memory.usage", "196517888", "byte"
                ),
                _metric_row("1784242513.0", "container.cpu.usage", "0.038", "cpu"),
                _metric_row("1784242513.0", "container.cpu.limit", "2.000", "cpu"),
            ]
        )

    def test_all_metrics_latest_first(self):
        records = parse_metric_records(self._payload())
        assert len(records) == 4
        # Sorted latest-first by ISO timestamp.
        assert records[0]["time"] >= records[-1]["time"]

    def test_memory_conversion(self):
        records = parse_metric_records(self._payload(), category=MetricCategory.MEMORY)
        assert {r["metric"] for r in records} == {
            "container.memory.limit",
            "container.memory.usage",
        }
        usage = next(r for r in records if r["metric"] == "container.memory.usage")
        assert usage["value"] == "187.4 MiB"
        assert usage["bytes"] == 196517888
        assert usage["instance"] == "0"
        assert usage["container"] == "runner"

    def test_cpu_category(self):
        records = parse_metric_records(self._payload(), category=MetricCategory.CPU)
        assert {r["metric"] for r in records} == {
            "container.cpu.usage",
            "container.cpu.limit",
        }
        usage = next(r for r in records if r["metric"] == "container.cpu.usage")
        assert usage["value"] == "0.038 cores"
        assert usage["cores"] == 0.038

    def test_raw_values(self):
        records = parse_metric_records(
            self._payload(), category=MetricCategory.MEMORY, raw_values=True
        )
        usage = next(r for r in records if r["metric"] == "container.memory.usage")
        assert usage["value"] == 196517888
        assert usage["bytes"] == 196517888

    def test_empty_payload(self):
        assert parse_metric_records("") == []
        assert parse_metric_records("null") == []
        assert parse_metric_records("not json") == []

    def test_skips_unparseable_value(self):
        payload = json.dumps(
            [_metric_row("1784242528.0", "container.memory.usage", "N/A", "byte")]
        )
        assert parse_metric_records(payload) == []

    def test_distinguishes_multiple_instances(self):
        payload = json.dumps(
            [
                _metric_row(
                    "1784242528.0",
                    "container.cpu.usage",
                    "0.038",
                    "cpu",
                    instance="0",
                ),
                _metric_row(
                    "1784242528.0",
                    "container.cpu.usage",
                    "0.091",
                    "cpu",
                    instance="1",
                ),
            ]
        )
        records = parse_metric_records(payload)
        by_instance = {r["instance"]: r for r in records}
        assert by_instance["0"]["cores"] == 0.038
        assert by_instance["1"]["cores"] == 0.091


def _event_row(ts, name, message, status, container=None, instance="0"):
    body = json.dumps({"message": message, "status": status})
    return [ts, "INFO", name, body, instance, container, "{}", None]


class TestParseLifecycleRecords:
    def test_parse(self):
        payload = json.dumps(
            [
                _event_row(
                    "1784242000.0",
                    "SERVICE.STATUS_CHANGE",
                    "Service is pending",
                    "PENDING",
                ),
                _event_row(
                    "1784242528.0",
                    "CONTAINER.STATUS_CHANGE",
                    "Running",
                    "READY",
                    container="runner",
                    instance="1",
                ),
            ]
        )
        records = parse_lifecycle_records(payload)
        assert len(records) == 2
        # Latest first.
        assert records[0]["event"] == "CONTAINER.STATUS_CHANGE"
        assert records[0]["status"] == "READY"
        assert records[0]["message"] == "Running"
        assert records[0]["instance"] == "1"
        assert records[0]["container"] == "runner"
        assert records[0]["severity"] == "INFO"
        assert records[1]["status"] == "PENDING"
        assert records[1]["instance"] == "0"
        assert records[1]["container"] == ""
        assert records[0]["time"].endswith("Z")

    def test_non_json_body_falls_back_to_raw(self):
        payload = json.dumps(
            [["1784242528.0", "INFO", "SOME.EVENT", "plain text", None, None]]
        )
        records = parse_lifecycle_records(payload)
        assert records[0]["message"] == "plain text"
        assert records[0]["status"] == ""
        assert records[0]["instance"] == ""
        assert records[0]["container"] == ""

    def test_empty(self):
        assert parse_lifecycle_records("") == []


class TestFormatLogLines:
    def test_format(self):
        payload = json.dumps(
            [
                ["1784242528.0", "0", "runner", "INFO: app started", "{}"],
                ["1784242530.0", "0", "runner", "INFO: listening", "{}"],
            ]
        )
        text = format_log_lines(payload)
        lines = text.splitlines()
        assert len(lines) == 2
        assert lines[0] == "2026-07-16T22:55:28Z [0/runner] INFO: app started"
        assert lines[1] == "2026-07-16T22:55:30Z [0/runner] INFO: listening"
        # Attributes column is not rendered.
        assert "{}" not in text

    def test_distinguishes_multiple_instances_and_containers(self):
        payload = json.dumps(
            [
                ["1784242528.0", "0", "runner", "from instance 0", "{}"],
                ["1784242528.0", "1", "runner", "from instance 1", "{}"],
                ["1784242528.0", "0", "sidecar", "from sidecar", "{}"],
            ]
        )
        lines = format_log_lines(payload).splitlines()
        assert "[0/runner] from instance 0" in lines[0]
        assert "[1/runner] from instance 1" in lines[1]
        assert "[0/sidecar] from sidecar" in lines[2]

    def test_empty(self):
        assert format_log_lines("") == ""
