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
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional

from rich.console import Group
from rich.style import Style
from rich.table import Table
from rich.text import Text
from rich.tree import Tree
from snowflake.cli._plugins.dcm.styles import (
    ALTER_STYLE,
    BOLD_STYLE,
    CREATE_STYLE,
    DOMAIN_STYLE,
    DROP_STYLE,
    ERROR_STYLE,
    FAIL_STYLE,
    OK_STYLE,
    PASS_STYLE,
    REFRESHED_STYLE,
    UP_TO_DATE_STYLE,
)
from snowflake.cli._plugins.dcm.utils import dump_json_result
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import MessageResult


class Reporter(ABC):
    """
    Base class for DCM command reporters.

    A reporter knows how to:
    - Extract relevant data from the result JSON
    - Generate formatted output using Rich renderables
    - Create summary lines
    - Check for errors and generate appropriate error messages
    """

    @abstractmethod
    def get_command_name(self) -> str:
        """Return the command name for JSON file naming."""
        pass

    @abstractmethod
    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        """Extract the relevant data from the result JSON."""
        pass

    @abstractmethod
    def generate_renderables(self, data: Any) -> Iterator[Group | Text | Tree]:
        """Generate Rich renderables for the data."""
        pass

    @abstractmethod
    def generate_summary(self, data: Any) -> Text:
        """Generate a summary Text object."""
        pass

    def check_for_errors(self, result_json: Dict[str, Any]) -> Optional[str]:
        """
        Check if result contains errors and return error message if so.

        Returns None if no errors, otherwise returns error message string.
        """
        return None

    def handle_empty_result(self) -> Optional[MessageResult]:
        """
        Handle case when no data is returned.

        Returns MessageResult if command should exit early, None to continue processing.
        """
        return MessageResult("No data returned from command.")


class PlanReporter(Reporter):
    """Reporter for plan and deploy commands."""

    # Column widths for consistent formatting
    OPERATION_WIDTH = 8  # "CREATE" or "ALTER" or "DROP" (with padding)
    TYPE_WIDTH = 32  # "WAREHOUSE", "TABLE", etc.

    def __init__(self, command_name: str = "plan"):
        self.command_name = command_name

    def get_command_name(self) -> str:
        return self.command_name

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        # For plan/deploy, the result is already a list of operations
        return result_json if isinstance(result_json, list) else []

    def generate_renderables(self, data: Any) -> Iterator[Table | Text | Tree]:
        """
        Generate Rich renderables for plan/deploy operations.

        Uses Table for consistent column alignment like RefreshReporter.
        Object types are neutral/informative, entity names in cyan for quick identification.
        """
        operations = data
        operation_styles = {
            "CREATE": CREATE_STYLE,
            "ALTER": ALTER_STYLE,
            "DROP": DROP_STYLE,
        }

        for op in operations:
            operation_type = op.get("operationType", "UNKNOWN")
            object_domain = op.get("objectDomain", "")
            object_name = op.get("objectName", "")
            association = op.get("association")
            details = op.get("details", {})

            op_style = operation_styles.get(operation_type, CREATE_STYLE)

            if association:
                # Handle associations (GRANT, DMF_ATTACHMENT)
                subject = op.get("subject", {})
                target = op.get("target", {})

                subject_name = subject.get("objectName", "")
                subject_domain = subject.get("objectDomain", "")
                target_name = target.get("objectName", "")
                target_domain = target.get("objectDomain", "")

                if association == "GRANT":
                    privilege = subject.get("objectPrivilege", "")

                    # Use Table for GRANT - single line with "to/from" connector
                    grant_table = Table(
                        show_header=False, box=None, padding=(0, 1, 0, 0)
                    )
                    grant_table.add_column(
                        "Operation", width=self.OPERATION_WIDTH, no_wrap=True
                    )
                    grant_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
                    grant_table.add_column("Name", no_wrap=True)

                    operation_text = Text(operation_type, style=op_style)

                    # Determine connector based on operation type
                    connector = "to" if operation_type == "CREATE" else "from"

                    if privilege:
                        type_text = Text(f"GRANT {privilege}")  # Type neutral
                    else:
                        type_text = Text("GRANT ROLE")  # Type neutral

                    # Build name with source → connector → target, all entity names in cyan
                    name_text = Text()
                    name_text.append(subject_name, style=DOMAIN_STYLE)  # Source in cyan
                    name_text.append(f" {connector} ")  # Connector neutral
                    name_text.append(target_name, style=DOMAIN_STYLE)  # Target in cyan

                    grant_table.add_row(operation_text, type_text, name_text)
                    yield grant_table

                elif association == "DMF_ATTACHMENT":
                    expectations = details.get("expectations", {})
                    columns = target.get("columns", [])

                    # Use Table for EXPECTATION main line
                    exp_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
                    exp_table.add_column(
                        "Operation", width=self.OPERATION_WIDTH, no_wrap=True
                    )
                    exp_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
                    exp_table.add_column("Name", no_wrap=True)

                    operation_text = Text(operation_type, style=op_style)
                    type_text = Text(f"EXPECTATION on {target_domain}")  # Type neutral

                    # Build name with optional columns
                    name_parts = [target_name]
                    if columns:
                        name_parts.append(f" ({', '.join(columns)})")
                    name_text = Text(
                        "".join(name_parts), style=DOMAIN_STYLE
                    )  # Name in cyan

                    exp_table.add_row(operation_text, type_text, name_text)

                    yield exp_table
                else:
                    # Other associations - use Table
                    assoc_table = Table(
                        show_header=False, box=None, padding=(0, 1, 0, 0)
                    )
                    assoc_table.add_column(
                        "Operation", width=self.OPERATION_WIDTH, no_wrap=True
                    )
                    assoc_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
                    assoc_table.add_column("Name", no_wrap=True)

                    operation_text = Text(operation_type, style=op_style)
                    type_text = Text(association)  # Association type neutral
                    name_text = Text(
                        f"{subject_name} → {target_name}", style=DOMAIN_STYLE
                    )  # Names in cyan

                    assoc_table.add_row(operation_text, type_text, name_text)
                    yield assoc_table
            else:
                # Handle regular objects using Table for proper alignment
                row_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
                row_table.add_column(
                    "Operation", width=self.OPERATION_WIDTH, no_wrap=True
                )
                row_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
                row_table.add_column("Name", no_wrap=True)

                # Operation in color (green/yellow/red)
                operation_text = Text(operation_type, style=op_style)

                # Object domain/type neutral (informative)
                type_text = Text(object_domain)

                # Object name in cyan for quick identification
                name_text = Text(object_name, style=DOMAIN_STYLE)

                row_table.add_row(operation_text, type_text, name_text)

                # For ALTER operations with property changes, combine table and tree in Group
                if operation_type == "ALTER" and details:
                    properties = details.get("properties", {})
                    if properties:
                        # Create tree for properties
                        prop_tree = Tree("")

                        for prop_name, prop_value in properties.items():
                            if isinstance(prop_value, dict):
                                change_from = prop_value.get("changeFrom", "")
                                change_to = prop_value.get("changeTo", "")
                                if change_to == "<unset>":
                                    change_text = Text(
                                        f"{prop_name}: {change_from} → (unset)"
                                    )
                                else:
                                    change_text = Text(
                                        f"{prop_name}: {change_from} → {change_to}"
                                    )
                            else:
                                change_text = Text(f"{prop_name}: {prop_value}")
                            prop_tree.add(change_text)

                        # Yield table and tree together in a Group to prevent blank line
                        yield Group(row_table, prop_tree)
                    else:
                        yield row_table
                else:
                    yield row_table

    def generate_summary(self, data: Any) -> Text:
        """Generate summary for plan/deploy operations."""
        operations = data
        create_count = sum(
            1 for op in operations if op.get("operationType") == "CREATE"
        )
        alter_count = sum(1 for op in operations if op.get("operationType") == "ALTER")
        drop_count = sum(1 for op in operations if op.get("operationType") == "DROP")

        if create_count == 0 and alter_count == 0 and drop_count == 0:
            return Text("No changes to apply.")

        # No "Summary:" prefix - start directly with counts
        summary = Text()

        parts = []
        if drop_count > 0:
            part = Text(str(drop_count), style=DROP_STYLE)
            part.append(" to be dropped")
            parts.append(part)
        if create_count > 0:
            part = Text(str(create_count), style=CREATE_STYLE)
            part.append(" to be created")
            parts.append(part)
        if alter_count > 0:
            part = Text(str(alter_count), style=ALTER_STYLE)
            part.append(" to be altered")
            parts.append(part)

        for i, part in enumerate(parts):
            if i > 0:
                summary.append(", ")
            summary.append_text(part)
        summary.append(".")

        return summary


class TestReporter(Reporter):
    """Reporter for test command."""

    # Column widths for consistent formatting
    STATUS_WIDTH = 13  # "✓ PASS" or "✗ FAIL" (unicode symbols + padding)
    TABLE_WIDTH = 55  # Table name column

    def __init__(self):
        self._status = "SUCCESS"  # Store status for summary generation

    def get_command_name(self) -> str:
        return "test"

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        # Store status for use in summary
        self._status = result_json.get("status", "SUCCESS")
        return result_json.get("expectations", [])

    def generate_renderables(self, data: Any) -> Iterator[Table | Text]:
        """
        Generate Rich Table for test expectations.

        Using single table exactly like RefreshReporter for proper style handling.
        Shows failure details after the main table.
        """
        expectations = data

        # Process each expectation and yield immediately (so details appear right after failures)
        for exp in expectations:
            table_name = exp.get("table_name", "UNKNOWN")
            expectation_name = exp.get("expectation_name", "UNKNOWN")
            violated = exp.get("expectation_violated", False)

            # Truncate table name if too long
            if len(table_name) > self.TABLE_WIDTH:
                table_name = "..." + table_name[-(self.TABLE_WIDTH - 3) :]

            # Create single-row table for this expectation
            row_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
            row_table.add_column("Status", width=self.STATUS_WIDTH, no_wrap=True)
            row_table.add_column("Table", width=self.TABLE_WIDTH, no_wrap=True)
            row_table.add_column("Expectation", no_wrap=True)

            # Build status cell - create Text with style (exactly like RefreshReporter)
            if violated:
                status_text = Text("✗ FAIL", style=FAIL_STYLE)
            else:
                status_text = Text("✓ PASS", style=PASS_STYLE)

            # Build table name cell (cyan for visibility)
            table_text = Text(table_name, style=DOMAIN_STYLE)

            # Build expectation cell (neutral style)
            expectation_text = Text(expectation_name)

            # Add row to table and yield immediately
            row_table.add_row(status_text, table_text, expectation_text)
            yield row_table

            # If failed, show details immediately after THIS expectation
            if violated:
                expectation_expr = exp.get("expectation_expression", "N/A")
                value = exp.get("value", "N/A")
                metric_name = exp.get("metric_name", "Unknown")

                detail_text = Text(
                    f"  └─ Expected: {expectation_expr}, Got: {value} (Metric: {metric_name})"
                )
                yield detail_text

    def generate_summary(self, data: Any) -> Text:
        """Generate summary for test results."""
        expectations = data
        total = len(expectations)
        failed = sum(
            1 for exp in expectations if exp.get("expectation_violated", False)
        )
        passed = total - failed

        # No "Summary:" prefix - start directly with counts
        summary = Text()
        summary.append(str(passed), style=PASS_STYLE)
        summary.append(" passed")

        if failed > 0:
            summary.append(", ")
            summary.append(str(failed), style=FAIL_STYLE)
            summary.append(" failed")

        summary.append(" out of ")
        summary.append(str(total), style=BOLD_STYLE)
        summary.append(" total.")

        return summary

    def check_for_errors(self, result_json: Dict[str, Any]) -> Optional[str]:
        status = result_json.get("status", "SUCCESS")
        expectations = result_json.get("expectations", [])

        if status == "EXPECTATION_VIOLATED":
            failed = sum(
                1 for exp in expectations if exp.get("expectation_violated", False)
            )
            passed = len(expectations) - failed
            return f"Test completed with failures: {passed} passed, {failed} failed out of {len(expectations)} total."
        return None

    def handle_empty_result(self) -> Optional[MessageResult]:
        # For test, empty expectations is valid but should be handled specially
        return None  # Let the handler check for empty expectations


class RefreshReporter(Reporter):
    """Reporter for refresh command."""

    # Column widths for consistent formatting
    STATUS_WIDTH = 13  # "✓ REFRESHED" or "○ UP-TO-DATE" (padding adds space)
    STATS_WIDTH = 18  # "(+  123 -  456)" (padding adds space)

    def get_command_name(self) -> str:
        return "refresh"

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        return result_json.get("refreshed_tables", [])

    @staticmethod
    def _format_number(num: int) -> str:
        """
        Format large numbers in human-readable format.

        Supports:
        - Thousands: 1k, 1.5k
        - Millions: 1M, 1.2M
        - Billions: 1B, 5.6B
        - Trillions: 1T, 7.8T
        - Quadrillions: 1P, 3.2P
        - Quintillions: 1E, 2.5E

        Removes trailing .0 for cleaner display (1.0k -> 1k)
        """
        abs_num = abs(num)
        sign = "-" if num < 0 else ""

        if abs_num >= 1_000_000_000_000_000_000:  # Quintillions (10^18)
            formatted = f"{abs_num / 1_000_000_000_000_000_000:.1f}E"
        elif abs_num >= 1_000_000_000_000_000:  # Quadrillions (10^15)
            formatted = f"{abs_num / 1_000_000_000_000_000:.1f}P"
        elif abs_num >= 1_000_000_000_000:  # Trillions
            formatted = f"{abs_num / 1_000_000_000_000:.1f}T"
        elif abs_num >= 1_000_000_000:  # Billions
            formatted = f"{abs_num / 1_000_000_000:.1f}B"
        elif abs_num >= 1_000_000:  # Millions
            formatted = f"{abs_num / 1_000_000:.1f}M"
        elif abs_num >= 1_000:  # Thousands
            formatted = f"{abs_num / 1_000:.1f}k"
        else:
            return str(num)

        # Remove trailing .0 for cleaner display
        formatted = formatted.replace(".0", "")
        return sign + formatted

    def generate_renderables(self, data: Any) -> Iterator[Table]:
        """
        Generate Rich Table for refresh results.

        Using Table provides automatic column alignment without brittleness of hardcoded widths.
        """
        refreshed_tables = data

        # Create table without header or box for clean output
        table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        table.add_column("Status", width=self.STATUS_WIDTH, no_wrap=True)
        table.add_column("Stats", width=self.STATS_WIDTH, no_wrap=True)
        table.add_column("Name", no_wrap=True)  # Don't wrap table names

        for tbl in refreshed_tables:
            dt_name = tbl.get("dt_name", "UNKNOWN")
            refreshed_count = tbl.get("refreshed_dt_count", 0)
            statistics = tbl.get("statistics", "")

            # Build status cell
            if refreshed_count > 0:
                status_text = Text("✓ REFRESHED", style=REFRESHED_STYLE)
            else:
                status_text = Text("○ UP-TO-DATE", style=UP_TO_DATE_STYLE)

            # Build statistics cell
            stats_text = Text()
            if (
                refreshed_count > 0
                and isinstance(statistics, str)
                and statistics.startswith("{")
            ):
                try:
                    stats_json = json.loads(statistics)
                    inserted = stats_json.get("insertedRows", 0)
                    deleted = stats_json.get("deletedRows", 0)

                    # Format with human-readable numbers
                    inserted_str = self._format_number(inserted)
                    deleted_str = self._format_number(deleted)

                    stats_text.append(f"(+{inserted_str:>5s} ")
                    stats_text.append(f"-{deleted_str:>5s}", style=Style(color="red"))
                    stats_text.append(")")
                except json.JSONDecodeError:
                    pass  # stats_text stays empty

            # Build name cell
            name_text = Text(dt_name, style=DOMAIN_STYLE)

            # Add row to table
            table.add_row(status_text, stats_text, name_text)

        # Yield the complete table as a single renderable
        yield table

    def generate_summary(self, data: Any) -> Text:
        """Generate summary for refresh results."""
        refreshed_tables = data
        total = len(refreshed_tables)
        refreshed = sum(
            1 for t in refreshed_tables if t.get("refreshed_dt_count", 0) > 0
        )
        up_to_date = total - refreshed

        if total == 0:
            return Text("No dynamic tables found in the project.")

        # No "Summary:" prefix - start directly with counts
        summary = Text()

        parts = []
        if refreshed > 0:
            part = Text(str(refreshed), style=REFRESHED_STYLE)
            part.append(" refreshed")
            parts.append(part)
        if up_to_date > 0:
            part = Text(str(up_to_date), style=UP_TO_DATE_STYLE)
            part.append(" up-to-date")
            parts.append(part)

        for i, part in enumerate(parts):
            if i > 0:
                summary.append(", ")
            summary.append_text(part)
        summary.append(".")

        return summary


class AnalyzeReporter(Reporter):
    """Reporter for analyze command."""

    def get_command_name(self) -> str:
        return "analyze"

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        return result_json.get("files", [])

    @staticmethod
    def _clean_error_message(error_msg: str) -> str:
        """Remove redundant prefix from analyze error messages."""
        prefix = "DCM project ANALYZE error: SQL compilation error:"
        if error_msg.startswith(prefix):
            return error_msg[len(prefix) :].lstrip()
        prefix2 = "DCM project ANALYZE error:"
        if error_msg.startswith(prefix2):
            return error_msg[len(prefix2) :].lstrip()
        return error_msg

    def generate_renderables(self, data: Any) -> Iterator[Tree | Text]:
        """Generate Rich renderables for analyze results."""
        files = data

        for file_info in files:
            source_path = file_info.get("sourcePath", "UNKNOWN")
            definitions = file_info.get("definitions", [])
            file_errors = file_info.get("errors", [])

            # Show file-level errors
            for error in file_errors:
                error_msg = self._clean_error_message(
                    error.get("message", "Unknown error")
                )

                main_text = Text("✗ ERROR   ", style=ERROR_STYLE)
                main_text.append("FILE           ")
                main_text.append(source_path)

                error_lines = [
                    line.strip() for line in error_msg.split("\n") if line.strip()
                ]
                if error_lines:
                    tree = Tree(main_text)
                    for line in error_lines:
                        tree.add(Text(line[:120], style=ERROR_STYLE))
                    yield tree
                else:
                    yield main_text

            # Show definitions
            for definition in definitions:
                def_id = definition.get("id", {})
                name = def_id.get("name", "UNKNOWN")
                domain = def_id.get("domain", "UNKNOWN")
                database = def_id.get("database", "")
                schema = def_id.get("schema", "")

                # Build full name
                full_name_parts = []
                if database:
                    full_name_parts.append(database)
                if schema:
                    full_name_parts.append(schema)
                full_name_parts.append(name)
                full_name = ".".join(full_name_parts)

                # Check for definition-level errors
                def_errors = definition.get("errors", [])

                if def_errors:
                    # Status in red, domain neutral, name in cyan
                    main_text = Text("✗ ERROR   ", style=ERROR_STYLE)
                    main_text.append(f"{domain:20s} ")  # Domain neutral/informative
                    main_text.append(
                        full_name, style=DOMAIN_STYLE
                    )  # Name in cyan for quick identification

                    tree = Tree(main_text)
                    for error in def_errors:
                        error_msg = self._clean_error_message(
                            error.get("message", "Unknown error")
                        )
                        error_lines = [
                            line.strip()
                            for line in error_msg.split("\n")
                            if line.strip()
                        ]
                        for line in error_lines:
                            tree.add(Text(line[:120], style=ERROR_STYLE))
                    yield tree
                else:
                    # Status in green, domain neutral, name in cyan
                    main_text = Text("✓ OK      ", style=OK_STYLE)
                    main_text.append(f"{domain:20s} ")  # Domain neutral/informative
                    main_text.append(
                        full_name, style=DOMAIN_STYLE
                    )  # Name in cyan for quick identification
                    yield main_text

    def generate_summary(self, data: Any) -> Text:
        """Generate summary for analyze results."""
        files = data
        total_files = len(files)
        total_definitions = 0
        total_errors = 0

        for file_info in files:
            definitions = file_info.get("definitions", [])
            file_errors = file_info.get("errors", [])

            total_definitions += len(definitions)
            total_errors += len(file_errors)

            for definition in definitions:
                def_errors = definition.get("errors", [])
                total_errors += len(def_errors)

        # No "Summary:" prefix - start directly with counts
        summary = Text(f"{total_files} files analyzed, ")
        summary.append(str(total_definitions), style=OK_STYLE)
        summary.append(" definitions found")

        if total_errors > 0:
            summary.append(", ")
            summary.append(str(total_errors), style=ERROR_STYLE)
            summary.append(" errors")

        summary.append(".")

        return summary

    def check_for_errors(self, result_json: Dict[str, Any]) -> Optional[str]:
        # Compute error summary
        files = result_json.get("files", [])
        total_files = len(files)
        total_definitions = 0
        total_errors = 0

        for file_info in files:
            definitions = file_info.get("definitions", [])
            file_errors = file_info.get("errors", [])

            total_definitions += len(definitions)
            total_errors += len(file_errors)

            for definition in definitions:
                def_errors = definition.get("errors", [])
                total_errors += len(def_errors)

        if total_errors > 0:
            return (
                f"Analyze completed with errors: {total_files} files analyzed, "
                f"{total_definitions} definitions found, {total_errors} errors."
            )
        return None


class DCMCommandResult:
    """
    Handles DCM command result processing with a reporter.

    Encapsulates the common pattern of:
    1. Fetching and parsing result
    2. Dumping JSON
    3. Rendering output
    4. Showing summary
    5. Handling errors
    """

    def __init__(
        self, cursor, reporter: Reporter, result_json: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize command result handler.

        Args:
            cursor: Database cursor with query result (if result_json not provided)
            reporter: Reporter instance for this command type
            result_json: Optional pre-parsed JSON result (skips cursor fetch if provided)
        """
        self.cursor = cursor
        self.reporter = reporter
        self._result_json = result_json
        self._skip_json_dump = (
            result_json is not None
        )  # If pre-parsed, assume JSON already dumped

    def process(self, skip_json_dump: bool = False) -> MessageResult:
        """
        Process the command result and return MessageResult.

        Args:
            skip_json_dump: If True, skips dumping JSON to file (useful when already dumped)

        Raises CliError if the result indicates an error condition.
        """
        # Get result_json (either from constructor or by fetching)
        if self._result_json is None:
            # Fetch row
            row = self.cursor.fetchone()
            if not row:
                early_exit = self.reporter.handle_empty_result()
                if early_exit:
                    return early_exit

            # Extract and parse JSON
            result_data = row[0]
            result_json = (
                json.loads(result_data) if isinstance(result_data, str) else result_data
            )
        else:
            result_json = self._result_json

        # Extract relevant data using reporter
        data = self.reporter.extract_data(result_json)

        # Handle special case for test command with no expectations
        if isinstance(self.reporter, TestReporter) and not data:
            return MessageResult("No expectations defined in the project.")

        # Dump raw JSON to file (unless skipped)
        if not skip_json_dump and not self._skip_json_dump:
            output_file = dump_json_result(
                self.reporter.get_command_name(), result_json
            )
            cli_console.step(f"Raw JSON saved to: {output_file}")

        # Render output
        for renderable in self.reporter.generate_renderables(data):
            cli_console._print(renderable)  # noqa: SLF001

        # Print summary
        summary = self.reporter.generate_summary(data)

        # Check for errors
        error_message = self.reporter.check_for_errors(result_json)
        if error_message:
            raise CliError(error_message)
        # cli_console._print(Text("\n") + summary)

        return MessageResult(str(Text("\n") + summary))
