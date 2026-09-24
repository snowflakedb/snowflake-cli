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

import json
from unittest.mock import Mock

import pytest
from click import ClickException
from snowflake.cli._plugins.spcs.common import (
    format_event_row,
    format_metric_row,
    handle_object_already_exists,
    validate_and_set_instances,
)
from snowflake.cli.api.exceptions import ObjectAlreadyExistsError, ObjectType
from snowflake.connector.errors import ProgrammingError


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_max",
    [
        (2, None, 2),  # max_instances is None, set max_instances to min_instances
        (
            5,
            10,
            10,
        ),  # max_instances is valid non-None value, return max_instances unchanged
    ],
)
def test_validate_and_set_instances(min_instances, max_instances, expected_max):
    assert expected_max == validate_and_set_instances(
        min_instances, max_instances, "name"
    )


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_msg",
    [
        (0, 1, "min_name must be positive"),  # non-positive min_instances
        (-1, 1, "min_name must be positive"),  # negative min_instances
        (
            2,
            1,
            "max_name must be greater or equal to min_name",
        ),  # min_instances > max_instances
    ],
)
def test_validate_and_set_instances_invalid(min_instances, max_instances, expected_msg):
    with pytest.raises(ClickException) as exc:
        validate_and_set_instances(min_instances, max_instances, "name")
    assert expected_msg in exc.value.message


SPCS_OBJECT_EXISTS_ERROR = ProgrammingError(
    msg="Object 'TEST_OBJECT' already exists.", errno=2002
)


def test_handle_object_exists_error():
    mock_type = Mock(spec=ObjectType)
    test_name = "TEST_OBJECT"
    with pytest.raises(ObjectAlreadyExistsError):
        handle_object_already_exists(SPCS_OBJECT_EXISTS_ERROR, mock_type, test_name)


def test_handle_object_exists_error_other_error():
    # For any errors other than 'Object 'XYZ' already exists.', simply pass the error through
    other_error = ProgrammingError(msg="Object does not already exist.", errno=0)
    with pytest.raises(ProgrammingError) as e:
        handle_object_already_exists(other_error, Mock(spec=ObjectType), "TEST_OBJECT")
    assert other_error == e.value


def test_format_event_row_reads_name_from_record():
    # Service-/instance-level events carry NULL RECORD_ATTRIBUTES and put the event
    # name in RECORD["name"] rather than RECORD_ATTRIBUTES["event.name"].
    event_dict = {
        "TIMESTAMP": "2026-06-23 23:10:12.689608",
        "RESOURCE_ATTRIBUTES": json.dumps(
            {"snow.service.name": "SVC001", "snow.service.instance": "0"}
        ),
        "RECORD_ATTRIBUTES": None,
        "RECORD": json.dumps(
            {"name": "SERVICE_INSTANCE.STATUS_CHANGE", "severity_text": "INFO"}
        ),
        "VALUE": json.dumps({"message": "Service instance is pending"}),
    }

    formatted = format_event_row(event_dict)

    assert formatted["SERVICE NAME"] == "SVC001"
    assert formatted["INSTANCE ID"] == "0"
    assert formatted["EVENT NAME"] == "SERVICE_INSTANCE.STATUS_CHANGE"
    assert formatted["SEVERITY"] == "INFO"


def test_format_event_row_handles_null_json_columns():
    # All JSON columns NULL must not crash; fall back to the placeholder values.
    event_dict = {
        "TIMESTAMP": "2024-12-14 22:27:25.420",
        "RESOURCE_ATTRIBUTES": None,
        "RECORD_ATTRIBUTES": None,
        "RECORD": None,
        "VALUE": "READY",
    }

    formatted = format_event_row(event_dict)

    assert formatted["EVENT NAME"] == "Unknown Event"
    assert formatted["SEVERITY"] == "Unknown Severity"
    assert formatted["EVENT VALUE"] == "READY"


_METRIC_RESOURCE_ATTRIBUTES = json.dumps(
    {
        "snow.database.name": "DB",
        "snow.schema.name": "SCH",
        "snow.service.name": "SVC",
        "snow.service.container.instance": "0",
        "snow.service.container.name": "main",
    }
)


@pytest.mark.parametrize(
    "record, value, expected_name, expected_value",
    [
        # Legacy event table: nested record.metric, bare scalar VALUE.
        (
            {"metric": {"name": "container.cpu.usage", "unit": "cpu"}},
            "0.25",
            "container.cpu.usage",
            "0.25",
        ),
        # Next-gen event table: flattened record.name, tagged scalar VALUE.
        (
            {"name": "container.cpu.usage", "unit": "cpu"},
            json.dumps({"double_value": 0.25}),
            "container.cpu.usage",
            "0.25",
        ),
        (
            {"name": "container.memory.usage", "unit": "By"},
            json.dumps({"int_value": 1048576}),
            "container.memory.usage",
            "1048576",
        ),
        # Non-scalar values (histograms) are passed through unchanged.
        (
            {"name": "request.latency", "unit": "ms"},
            json.dumps({"count": 3, "sum": 9.0}),
            "request.latency",
            json.dumps({"count": 3, "sum": 9.0}),
        ),
    ],
)
def test_format_metric_row_dual_reads_legacy_and_next_gen_shapes(
    record, value, expected_name, expected_value
):
    formatted = format_metric_row(
        {
            "TIMESTAMP": "2024-12-14 22:27:25.420",
            "RESOURCE_ATTRIBUTES": _METRIC_RESOURCE_ATTRIBUTES,
            "RECORD": json.dumps(record),
            "VALUE": value,
        }
    )

    assert formatted["METRIC NAME"] == expected_name
    assert formatted["METRIC VALUE"] == expected_value
    assert formatted["SERVICE NAME"] == "SVC"
