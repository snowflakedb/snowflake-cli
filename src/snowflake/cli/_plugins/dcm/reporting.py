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
from collections import defaultdict
from typing import Any, Dict, Iterator, Optional

from rich.console import Group
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
    INSERTED_STYLE,
    OK_STYLE,
    PASS_STYLE,
    REMOVED_STYLE,
    STATUS_STYLE,
)
from snowflake.cli._plugins.dcm.utils import dump_json_result
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.sanitizers import sanitize_for_terminal


class DCMMessageResult(MessageResult):
    def __init__(self, message: Text | str) -> None:
        super().__init__(message)
        self._message = message


class Reporter(ABC):
    def __init__(self) -> None:
        self.command_name = ""

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

    def check_for_errors(self, result_json: Dict[str, Any]) -> bool:
        """
        Check if result contains errors and return True if so.
        """
        return False

    @staticmethod
    def handle_empty_result() -> Optional[MessageResult]:
        return MessageResult("No data.")


class PlanReporter(Reporter):
    """Reporter for plan and deploy commands."""

    # Column widths for consistent formatting
    OPERATION_WIDTH = 8  # "CREATE" or "ALTER" or "DROP" (with padding)
    TYPE_WIDTH = 15  # "WAREHOUSE", "TABLE", etc.

    def __init__(self, command_name: str = "plan"):
        super().__init__()
        self.command_name = command_name

    @property
    def is_plan(self) -> bool:
        return True if self.command_name == "plan" else False

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
                    yield from self._render_grant(op_style, op)

                elif association == "DMF_ATTACHMENT":
                    yield from self._render_attachment(
                        details,
                        op_style,
                        operation_type,
                        target,
                        target_domain,
                        target_name,
                    )
                else:
                    raise CliError(f"Unknown association: {association}")
            else:
                yield from self._handle_basic_entities(
                    details, object_domain, object_name, op_style, operation_type
                )

    def _handle_basic_entities(
        self, details, object_domain, object_name, op_style, operation_type
    ):
        # Handle regular objects using Table for proper alignment
        row_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        row_table.add_column("Operation", width=self.OPERATION_WIDTH, no_wrap=True)
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
                            change_text = Text(f"{prop_name}: {change_from} → (unset)")
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

    def _handle_unknown_associations(
        self, association, op_style, operation_type, subject_name, target_name
    ):
        # Other associations - use Table
        assoc_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        assoc_table.add_column("Operation", width=self.OPERATION_WIDTH, no_wrap=True)
        assoc_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
        assoc_table.add_column("Name", no_wrap=True)
        operation_text = Text(operation_type, style=op_style)
        type_text = Text(association)  # Association type neutral
        name_text = Text(
            f"{subject_name} → {target_name}", style=DOMAIN_STYLE
        )  # Names in cyan
        assoc_table.add_row(operation_text, type_text, name_text)
        yield assoc_table

    def _render_attachment(
        self, details, op_style, operation_type, target, target_domain, target_name
    ):
        expectations = details.get("expectations", {})
        columns = target.get("columns", [])
        # Use Table for EXPECTATION main line
        exp_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        exp_table.add_column("Operation", width=self.OPERATION_WIDTH, no_wrap=True)
        exp_table.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
        exp_table.add_column("Name", no_wrap=True)
        operation_text = Text(operation_type, style=op_style)
        type_text = Text(f"EXPECTATION on {target_domain}")  # Type neutral
        # Build name with optional columns
        name_parts = [target_name]
        if columns:
            name_parts.append(f" ({', '.join(columns)})")
        name_text = Text("".join(name_parts), style=DOMAIN_STYLE)  # Name in cyan
        exp_table.add_row(operation_text, type_text, name_text)
        yield exp_table

    def _render_grant(self, op_style, operation) -> Table:
        operation_type = operation.get("operationType", "UNKNOWN")
        subject = operation.get("subject", {})
        target = operation.get("target", {})

        subject_name = subject.get("objectName", "")
        subject_domain = subject.get("objectDomain", "")
        target_name = target.get("objectName", "")
        target_domain = target.get("objectDomain", "")
        privilege = subject.get("objectPrivilege", "")
        result = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        result.add_column("Operation", width=self.OPERATION_WIDTH, no_wrap=True)
        result.add_column("Type", width=self.TYPE_WIDTH, no_wrap=True)
        result.add_column("Name", no_wrap=True)
        operation_text = Text(operation_type, style=op_style)
        connector = "to" if operation_type == "CREATE" else "from"
        type_text = Text(f"GRANT")
        # Build name with source → connector → target, all entity names in cyan
        name_text = Text("")
        name_text.append(privilege if privilege else "ALL", style=BOLD_STYLE)
        name_text.append(" on ")
        name_text.append(subject_domain, style=BOLD_STYLE)
        name_text.append(f" {subject_name}", style=DOMAIN_STYLE)
        name_text.append(f" {connector} ")
        name_text.append(target_domain, style=BOLD_STYLE)
        name_text.append(f" {target_name}", style=DOMAIN_STYLE)
        result.add_row(operation_text, type_text, name_text)
        yield result

    def generate_summary(self, data: Any) -> Text:
        """Generate summary for plan/deploy operations."""
        operations = data
        create_count = sum(
            1 for op in operations if op.get("operationType") == "CREATE"
        )
        alter_count = sum(1 for op in operations if op.get("operationType") == "ALTER")
        drop_count = sum(1 for op in operations if op.get("operationType") == "DROP")

        if create_count == 0 and alter_count == 0 and drop_count == 0:
            return Text(f"No changes to {'plan' if self.is_plan else 'apply'}.")

        # No "Summary:" prefix - start directly with counts
        summary = Text()

        parts = []
        if drop_count > 0:
            part = Text(str(drop_count), style=DROP_STYLE)
            if self.is_plan:
                part.append(" to be")
            part.append(" dropped")
            parts.append(part)
        if create_count > 0:
            part = Text(str(create_count), style=CREATE_STYLE)
            if self.is_plan:
                part.append(" to be")
            part.append(" created")
            parts.append(part)
        if alter_count > 0:
            part = Text(str(alter_count), style=ALTER_STYLE)
            if self.is_plan:
                part.append(" to be")
            part.append(" altered")
            parts.append(part)

        for i, part in enumerate(parts):
            if i > 0:
                summary.append(", ")
            summary.append_text(part)
        summary.append(".")

        return summary


class TestReporter(Reporter):
    STATUS_WIDTH = 11  # "✓ PASS" or "✗ FAIL" (unicode symbols + padding)
    TABLE_WIDTH = 55

    def __init__(self) -> None:
        super().__init__()
        self.command_name = "test"
        self._summary_data: dict[str, int] = defaultdict(int)

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        return result_json.get("expectations", [])

    def generate_renderables(self, data: Any) -> Iterator[Table | Text]:
        for exp in data:
            table_name = sanitize_for_terminal(exp.get("table_name", "UNKNOWN"))
            expectation_name = sanitize_for_terminal(
                exp.get("expectation_name", "UNKNOWN")
            )
            has_failed = exp.get("expectation_violated", False)

            row_table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
            row_table.add_column("Status", width=self.STATUS_WIDTH, no_wrap=True)
            row_table.add_column("Table", no_wrap=False)

            if has_failed:
                status_text = Text("✗ FAIL", style=FAIL_STYLE)
                self._summary_data["failed"] += 1
            else:
                status_text = Text("✓ PASS", style=PASS_STYLE)
                self._summary_data["passed"] += 1

            table_text = Text()
            table_text.append(table_name, style=DOMAIN_STYLE)
            table_text.append(" (")
            table_text.append(expectation_name)
            table_text.append(")")

            row_table.add_row(status_text, table_text)
            yield row_table

            if has_failed:
                expectation_expr = exp.get("expectation_expression", "N/A")
                value = exp.get("value", "N/A")
                metric_name = exp.get("metric_name", "Unknown")

                detail_text = Text(
                    f"  └─ Expected: {expectation_expr}, Got: {value} (Metric: {metric_name})"
                )
                yield detail_text

    def generate_summary(self, data: Any) -> Text:
        failed = self._summary_data["failed"]
        passed = self._summary_data["passed"]
        total = failed + passed
        if total == 0:
            return Text("No data expectations found.")

        summary = Text()
        summary.append(f"{str(passed)} passed", style=PASS_STYLE)
        summary.append(", ")
        summary.append(f"{str(failed)} failed", style=FAIL_STYLE)
        summary.append(" out of ")
        summary.append(str(total), style=BOLD_STYLE)
        summary.append(" total.")

        return summary

    def check_for_errors(self, result_json: Dict[str, Any]) -> bool:
        if self._summary_data["failed"] > 0:
            return True
        return False


class RefreshReporter(Reporter):
    STATUS_WIDTH = 11
    STATS_WIDTH = 7

    def __init__(self):
        super().__init__()
        self.command_name = "refresh"
        self._summary_data = defaultdict(int)

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        return result_json.get("refreshed_tables", [])

    @staticmethod
    def _format_number(num: int) -> str:
        abs_num = abs(num)
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

        formatted = formatted.replace(".0", "")
        return formatted

    def generate_renderables(self, data: Any) -> Iterator[Table]:
        result = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
        result.add_column("Status", width=self.STATUS_WIDTH, no_wrap=True)
        result.add_column(
            "Added", width=self.STATS_WIDTH, no_wrap=True, justify="right"
        )
        result.add_column(
            "Removed", width=self.STATS_WIDTH, no_wrap=True, justify="right"
        )
        result.add_column("Name", no_wrap=True)

        for table in data:
            dt_name = table.get("dt_name", "UNKNOWN")
            statistics = table.get("statistics", "")

            inserted_text, deleted_text = Text(), Text()
            stats_json = None
            if isinstance(statistics, str) and statistics.startswith("{"):
                try:
                    stats_json = json.loads(statistics)
                except json.JSONDecodeError:
                    pass  # stats_text stays empty
            elif isinstance(statistics, dict):
                stats_json = statistics

            if (
                statistics == "No new data"
                or stats_json is not None
                and (
                    not stats_json.get("insertedRows")
                    and not stats_json.get("deletedRows")
                )
            ):
                status_text = Text("UP-TO-DATE", style=STATUS_STYLE)
                self._summary_data["up-to-date"] += 1
            elif stats_json is not None and (
                stats_json.get("insertedRows") or stats_json.get("deletedRows")
            ):
                status_text = Text("REFRESHED", style=STATUS_STYLE)
                self._summary_data["refreshed"] += 1
            else:
                status_text = Text("UNKNOWN", style=STATUS_STYLE)
                self._summary_data["unknown"] += 1

            if stats_json is not None:
                inserted = stats_json.get("insertedRows", 0)
                deleted = stats_json.get("deletedRows", 0)

                inserted_str = self._format_number(inserted)
                if inserted_str != "0":
                    inserted_str = "+" + inserted_str
                deleted_str = self._format_number(deleted)
                if deleted_str != "0":
                    deleted_str = "-" + deleted_str

                inserted_text.append(inserted_str, style=INSERTED_STYLE)
                deleted_text.append(deleted_str, style=REMOVED_STYLE)

            name_text = Text(sanitize_for_terminal(dt_name), style=DOMAIN_STYLE)
            result.add_row(status_text, inserted_text, deleted_text, name_text)
        yield result

    def generate_summary(self, *args, **kwargs) -> Text:
        total = sum(self._summary_data.values())
        if total == 0:
            return Text("No dynamic tables found in the project.")

        summary = Text()

        parts = []
        if (refreshed := self._summary_data.get("refreshed", 0)) > 0:
            part = Text(str(refreshed))
            part.append(" refreshed")
            parts.append(part)
        if (up_to_date := self._summary_data.get("up-to-date", 0)) > 0:
            part = Text(str(up_to_date))
            part.append(" up-to-date")
            parts.append(part)
        if (unknown := self._summary_data.get("unknown", 0)) > 0:
            part = Text(str(unknown))
            part.append(" unknown")
            parts.append(part)

        for i, part in enumerate(parts):
            if i > 0:
                summary.append(", ")
            summary.append_text(part)
        summary.append(".")

        return summary


class AnalyzeReporter(Reporter):
    """Reporter for analyze command."""

    def __init__(self):
        super().__init__()
        self.command_name = "analyze"

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

    def check_for_errors(self, result_json: Dict[str, Any]) -> bool:
        files = result_json.get("files", [])
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
            return True
        return False


class DCMCommandResult:
    def __init__(self, cursor, reporter: Reporter):
        self.cursor = cursor
        self.reporter = reporter

    def process(self) -> MessageResult:
        row = self.cursor.fetchone()
        if not row:
            if empty_message := self.reporter.handle_empty_result():
                return empty_message

        result_data = row[0]
        result_json = (
            json.loads(result_data) if isinstance(result_data, str) else result_data
        )

        data = self.reporter.extract_data(result_json)

        output_file = dump_json_result(self.reporter.command_name, result_json)
        cli_console.step(f"Raw JSON saved to: {output_file}")

        for renderable in self.reporter.generate_renderables(data):
            cli_console._print(renderable)  # noqa: SLF001

        summary = self.reporter.generate_summary(data)

        if self.reporter.check_for_errors(result_json):
            cli_console._print(Text("\n") + summary)  # noqa: SLF001
            raise SystemExit(1)

        return DCMMessageResult(Text("\n") + summary)
