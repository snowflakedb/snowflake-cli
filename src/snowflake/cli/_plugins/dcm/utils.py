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
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List
from xml.etree import ElementTree


class TestResultFormat(str, Enum):
    JSON = "json"
    JUNIT = "junit"
    TAP = "tap"


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


def _normalize_table_name(table_name: str) -> str:
    """Normalize table name to lowercase with hyphens for file naming."""
    return table_name.lower().replace(".", "-").replace("_", "-")


def _group_expectations_by_table(
    expectations: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """Group expectations by table name."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for expectation in expectations:
        table_name = expectation.get("table_name", "unknown")
        if table_name not in grouped:
            grouped[table_name] = []
        grouped[table_name].append(expectation)
    return grouped


def export_test_results_as_json(result_data: Dict[str, Any], output_path: Path) -> None:
    """Export test results as JSON format."""
    with open(output_path, "w") as f:
        json.dump(result_data, f, indent=2)


def export_test_results_as_junit(
    result_data: Dict[str, Any], output_dir: Path
) -> List[Path]:
    """Export test results as JUnit XML format, one file per table."""
    expectations = result_data.get("expectations", [])
    grouped = _group_expectations_by_table(expectations)

    junit_dir = output_dir / "junit"
    junit_dir.mkdir(parents=True, exist_ok=True)

    saved_files = []

    for table_name, table_expectations in grouped.items():
        normalized_name = _normalize_table_name(table_name)
        output_path = junit_dir / f"{normalized_name}.xml"

        testsuites = ElementTree.Element("testsuites")
        testsuite = ElementTree.SubElement(
            testsuites,
            "testsuite",
            name=f"DCM Tests - {table_name}",
            tests=str(len(table_expectations)),
            failures=str(
                sum(
                    1
                    for e in table_expectations
                    if e.get("expectation_violated", False)
                )
            ),
            errors="0",
            skipped="0",
        )

        for expectation in table_expectations:
            expectation_name = expectation.get("expectation_name", "Unknown")
            metric_name = expectation.get("metric_name", "Unknown")

            testcase = ElementTree.SubElement(
                testsuite,
                "testcase",
                name=expectation_name,
                classname=table_name,
            )

            if expectation.get("expectation_violated", False):
                failure = ElementTree.SubElement(
                    testcase,
                    "failure",
                    message=f"Expectation '{expectation_name}' violated",
                    type="AssertionError",
                )
                expectation_expr = expectation.get("expectation_expression", "N/A")
                value = expectation.get("value", "N/A")
                failure.text = (
                    f"Metric: {metric_name}\n"
                    f"Expression: {expectation_expr}\n"
                    f"Actual value: {value}"
                )

        tree = ElementTree.ElementTree(testsuites)
        ElementTree.indent(tree, space="  ")
        tree.write(output_path, encoding="utf-8", xml_declaration=True)
        saved_files.append(output_path)

    return saved_files


def export_test_results_as_tap(
    result_data: Dict[str, Any], output_dir: Path
) -> List[Path]:
    """Export test results as TAP (Test Anything Protocol) format, one file per table."""
    expectations = result_data.get("expectations", [])
    grouped = _group_expectations_by_table(expectations)

    tap_dir = output_dir / "tap"
    tap_dir.mkdir(parents=True, exist_ok=True)

    saved_files = []

    for table_name, table_expectations in grouped.items():
        normalized_name = _normalize_table_name(table_name)
        output_path = tap_dir / f"{normalized_name}.tap"

        lines = [f"1..{len(table_expectations)}"]

        for idx, expectation in enumerate(table_expectations, start=1):
            expectation_name = expectation.get("expectation_name", "Unknown")
            metric_name = expectation.get("metric_name", "Unknown")

            if expectation.get("expectation_violated", False):
                lines.append(f"not ok {idx} - {expectation_name}")
                lines.append(f"  ---")
                lines.append(f"  message: Expectation '{expectation_name}' violated")
                lines.append(f"  severity: fail")
                lines.append(f"  data:")
                lines.append(f"    table: {table_name}")
                lines.append(f"    metric: {metric_name}")
                lines.append(
                    f"    expression: {expectation.get('expectation_expression', 'N/A')}"
                )
                lines.append(f"    actual_value: {expectation.get('value', 'N/A')}")
                lines.append(f"  ...")
            else:
                lines.append(f"ok {idx} - {expectation_name}")

        with open(output_path, "w") as f:
            f.write("\n".join(lines) + "\n")

        saved_files.append(output_path)

    return saved_files


def export_test_results(
    result_data: Dict[str, Any],
    formats: List[TestResultFormat],
    output_dir: Path,
) -> List[Path]:
    """
    Export test results in multiple formats.

    Args:
        result_data: The test result data from the backend
        formats: List of formats to export to
        output_dir: Directory to save the results

    Returns:
        List of paths where results were saved
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files = []

    for format_type in formats:
        if format_type == TestResultFormat.JSON:
            output_path = output_dir / "test_result.json"
            export_test_results_as_json(result_data, output_path)
            saved_files.append(output_path)
        elif format_type == TestResultFormat.JUNIT:
            files = export_test_results_as_junit(result_data, output_dir)
            saved_files.extend(files)
        elif format_type == TestResultFormat.TAP:
            files = export_test_results_as_tap(result_data, output_dir)
            saved_files.extend(files)

    return saved_files


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
