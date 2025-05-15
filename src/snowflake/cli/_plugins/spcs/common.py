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

import json
import sys
from datetime import datetime
from typing import TextIO

from click import ClickException
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import ObjectAlreadyExistsError
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.connector.errors import ProgrammingError

EVENT_COLUMN_NAMES = [
    "TIMESTAMP",
    "START_TIMESTAMP",
    "OBSERVED_TIMESTAMP",
    "TRACE",
    "RESOURCE",
    "RESOURCE_ATTRIBUTES",
    "SCOPE",
    "SCOPE_ATTRIBUTES",
    "RECORD_TYPE",
    "RECORD",
    "RECORD_ATTRIBUTES",
    "VALUE",
    "EXEMPLARS",
]

if not sys.stdout.closed and sys.stdout.isatty():
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    ORANGE = "\033[38:2:238:76:44m"
    GRAY = "\033[2m"
    ENDC = "\033[0m"
else:
    GREEN = ""
    ORANGE = ""
    BLUE = ""
    GRAY = ""
    ENDC = ""


def _prefix_line(prefix: str, line: str) -> str:
    """
    _prefix_line ensure the prefix is still present even when dealing with return characters
    """
    if "\r" in line:
        line = line.replace("\r", f"\r{prefix}")
    if "\n" in line[:-1]:
        line = line[:-1].replace("\n", f"\n{prefix}") + line[-1:]
    if not line.startswith("\r"):
        line = f"{prefix}{line}"
    return line


def print_log_lines(file: TextIO, name, identifier, logs):
    prefix = f"{GREEN}{name}/{identifier}{ENDC} "
    logs = logs[0:-1]
    for log in logs:
        print(_prefix_line(prefix, log + "\n"), file=file, end="", flush=True)


def strip_empty_lines(lines: list[str]) -> str:
    return "\n".join(stripped for l in lines if (stripped := l.strip()))


def validate_and_set_instances(min_instances, max_instances, instance_name):
    """
    Used to validate that min_instances is positive and that max_instances is not less than min_instances. In the
    case that max_instances is none, sets it equal to min_instances by default. Used like `max_instances =
    validate_and_set_instances(min_instances, max_instances, "name")`.
    """
    if min_instances < 1:
        raise ClickException(f"min_{instance_name} must be positive")

    if max_instances is None:
        max_instances = min_instances
    elif max_instances < min_instances:
        raise ClickException(
            f"max_{instance_name} must be greater or equal to min_{instance_name}"
        )
    return max_instances


def handle_object_already_exists(
    error: ProgrammingError,
    object_type: ObjectType,
    object_name: str,
    replace_available: bool = False,
):
    if error.errno == 2002:
        raise ObjectAlreadyExistsError(
            object_type=object_type,
            name=unquote_identifier(object_name),
            replace_available=replace_available,
        )
    else:
        raise error


def filter_log_timestamp(log: str, include_timestamps: bool) -> str:
    if include_timestamps:
        return log
    else:
        return log.split(" ", 1)[1] if " " in log else log


def new_logs_only(prev_log_records: list[str], new_log_records: list[str]) -> list[str]:
    # Sort the log records, we get time-ordered logs
    # due to ISO 8601 timestamp format in the log content
    # eg: 2024-10-22T01:12:29.873896187Z Count: 1
    new_log_records_sorted = sorted(new_log_records)

    # Get the first new log record to establish the overlap point
    first_new_log_record = new_log_records_sorted[0]

    # Traverse previous logs in reverse and remove duplicates from new logs
    for prev_log in reversed(prev_log_records):
        # Stop if the previous log is earlier than the first new log
        if prev_log < first_new_log_record:
            break

        # Remove matching previous logs from the new logs list
        if prev_log in new_log_records_sorted:
            new_log_records_sorted.remove(prev_log)

    return new_log_records_sorted


def build_resource_clause(
    service_name: str, instance_id: str, container_name: str
) -> str:
    resource_filters = []
    if service_name:
        resource_filters.append(
            f"resource_attributes:\"snow.service.name\" = '{service_name}'"
        )
    if instance_id:
        resource_filters.append(
            f"(resource_attributes:\"snow.service.instance\" = '{instance_id}' "
            f"OR resource_attributes:\"snow.service.container.instance\" = '{instance_id}')"
        )
    if container_name:
        resource_filters.append(
            f"resource_attributes:\"snow.service.container.name\" = '{container_name}'"
        )
    return " and ".join(resource_filters) if resource_filters else "1=1"


def build_time_clauses(
    since: str | datetime | None, until: str | datetime | None
) -> tuple[str, str]:
    since_clause = ""
    until_clause = ""

    if isinstance(since, datetime):
        since_clause = f"and timestamp >= '{since}'"
    elif isinstance(since, str) and since:
        since_clause = f"and timestamp >= sysdate() - interval '{since}'"

    if isinstance(until, datetime):
        until_clause = f"and timestamp <= '{until}'"
    elif isinstance(until, str) and until:
        until_clause = f"and timestamp <= sysdate() - interval '{until}'"

    return since_clause, until_clause


def build_db_and_schema_clause(database_name: str, schema_name: str | None) -> str:
    return f"""
        and resource_attributes:"snow.database.name" = '{database_name}'
        and resource_attributes:"snow.schema.name" = '{schema_name or 'PUBLIC'}'
"""


def format_event_row(event_dict: dict) -> dict:
    try:
        resource_attributes = json.loads(event_dict.get("RESOURCE_ATTRIBUTES", "{}"))
        record_attributes = json.loads(event_dict.get("RECORD_ATTRIBUTES", "{}"))
        record = json.loads(event_dict.get("RECORD", "{}"))

        database_name = resource_attributes.get("snow.database.name", "N/A")
        schema_name = resource_attributes.get("snow.schema.name", "N/A")
        service_name = resource_attributes.get("snow.service.name", "N/A")
        instance_name = resource_attributes.get("snow.service.instance", "N/A")
        container_name = resource_attributes.get("snow.service.container.name", "N/A")
        event_name = record_attributes.get("event.name", "Unknown Event")
        event_value = event_dict.get("VALUE", "Unknown Value")
        severity = record.get("severity_text", "Unknown Severity")

        return {
            "TIMESTAMP": event_dict.get("TIMESTAMP", "N/A"),
            "DATABASE NAME": database_name,
            "SCHEMA NAME": schema_name,
            "SERVICE NAME": service_name,
            "INSTANCE ID": instance_name,
            "CONTAINER NAME": container_name,
            "SEVERITY": severity,
            "EVENT NAME": event_name,
            "EVENT VALUE": event_value,
        }
    except (json.JSONDecodeError, KeyError) as e:
        raise RecordProcessingError(f"Error processing event row.")


def format_metric_row(metric_dict: dict) -> dict:
    try:
        resource_attributes = json.loads(metric_dict["RESOURCE_ATTRIBUTES"])
        record = json.loads(metric_dict["RECORD"])

        database_name = resource_attributes.get("snow.database.name", "N/A")
        schema_name = resource_attributes.get("snow.schema.name", "N/A")
        service_name = resource_attributes.get("snow.service.name", "N/A")
        instance_name = resource_attributes.get(
            "snow.service.container.instance", "N/A"
        )
        container_name = resource_attributes.get("snow.service.container.name", "N/A")

        metric_name = record["metric"].get("name", "Unknown Metric")
        metric_value = metric_dict.get("VALUE", "Unknown Value")

        return {
            "TIMESTAMP": metric_dict.get("TIMESTAMP", "N/A"),
            "DATABASE NAME": database_name,
            "SCHEMA NAME": schema_name,
            "SERVICE NAME": service_name,
            "INSTANCE ID": instance_name,
            "CONTAINER NAME": container_name,
            "METRIC NAME": metric_name,
            "METRIC VALUE": metric_value,
        }
    except (json.JSONDecodeError, KeyError) as e:
        raise RecordProcessingError(f"Error processing metric row.")


class RecordProcessingError(ClickException):
    """Raised when processing an event or metric record fails due to invalid data."""

    pass


class SPCSEventTableError(ClickException):
    """Raised when there is an issue related to the SPCS event table."""

    pass


class NoPropertiesProvidedError(ClickException):
    pass
