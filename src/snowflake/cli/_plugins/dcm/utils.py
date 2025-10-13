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


def format_test_failures(
    failed_expectations: list, total_tests: int, failed_count: int
) -> str:
    """Format test failures into a nice error message."""
    lines = [
        "Failed expectations:",
    ]

    for failed in failed_expectations:
        table_name = failed.get("table_name", "Unknown")
        expectation_name = failed.get("expectation_name", "Unknown")
        metric_name = failed.get("metric_name", "Unknown")
        expectation_expr = failed.get("expectation_expression", "N/A")
        value = failed.get("value", "N/A")

        lines.append(f"  Table: {table_name}")
        lines.append(f"  Expectation: {expectation_name}")
        lines.append(f"  Metric: {metric_name}")
        lines.append(f"  Expression: {expectation_expr}")
        lines.append(f"  Actual value: {value}")
        lines.append("")

    passed_tests = total_tests - failed_count
    lines.append(
        f"Tests completed: {passed_tests} passed, {failed_count} failed out of {total_tests} total."
    )

    return "\n".join(lines)


def format_refresh_results(refreshed_tables: list) -> str:
    """Format refresh results into a concise user-friendly message."""
    if not refreshed_tables:
        return "No dynamic tables found in the project."

    total_tables = len(refreshed_tables)
    refreshed_count = sum(
        1 for table in refreshed_tables if table.get("refreshed_dt_count", 0) > 0
    )
    up_to_date_count = total_tables - refreshed_count

    return f"{refreshed_count} dynamic table(s) refreshed. {up_to_date_count} dynamic table(s) up-to-date."
