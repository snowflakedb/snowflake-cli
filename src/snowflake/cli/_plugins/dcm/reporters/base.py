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
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Iterator, List, TypeVar

from rich.text import Text
from snowflake.cli._plugins.dcm.utils import save_command_response
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    EmptyResult,
    RespectingColumnTypesRowMapper,
)
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

T = TypeVar("T")

# Visual divider printed after every DCM command's reporter output so the
# command's final line is clearly separated from the next shell prompt
# (or from a CliError box on failure).
_REPORT_SEPARATOR = "=" * 80


class Reporter(ABC, Generic[T]):
    def __init__(self, save_output: bool = False) -> None:
        self.result_raw_data = None
        self.command_name = ""
        self.save_output = save_output
        # When False, saving the raw response won't print the "Artifacts saved
        # to" step (the file is still written). Commands that render their own
        # output-location line (e.g. ``compile``) opt out of the default step.
        self.announce_save = True
        # When False, the raw cursor response is not written to
        # ``out/<command>_result.json``. Commands that download the backend's own
        # ``<command>_result.json`` (via ``collect_output``) opt out so we don't
        # duplicate/overwrite that richer file with the raw response.
        self.write_result_file = True

    @abstractmethod
    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract the relevant data from the result JSON."""
        ...

    @abstractmethod
    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[T]:
        """Parse raw data into domain objects."""
        ...

    @abstractmethod
    def print_renderables(self, data: Iterator[T]) -> None:
        """Print Rich renderables for the parsed data."""
        ...

    @abstractmethod
    def _is_success(self) -> bool:
        """Check if underlying operation passed without errors"""
        ...

    @abstractmethod
    def _generate_summary_renderables(self) -> List[Text]:
        """Generate a list of rich renderables to be printed as success or error message"""
        ...

    def print_summary(self) -> None:
        """Print operation summary when the result is successful."""
        renderables = self._generate_summary_renderables()
        cli_console.styled_message("\n")
        for renderable in renderables:
            cli_console.styled_message(renderable.plain, style=renderable.style)
        cli_console.styled_message("\n")

    def print_separator(self) -> None:
        """Print a divider line that marks the end of the command's output.

        Automatically muted in JSON/CSV output formats (cf. `cli_console`)."""
        cli_console.styled_message(_REPORT_SEPARATOR, style="dim")
        cli_console.styled_message("\n")

    def _try_save_response(self, result_json: Dict[str, Any]) -> None:
        """Save raw JSON response if save_output is enabled and raw data is available."""
        if self.save_output and self.write_result_file:
            save_command_response(
                self.command_name,
                result_json,
                announce=self.announce_save,
            )

    def process_payload(self, result_json: Dict[str, Any]) -> None:
        """Process already decoded response payload and print results."""
        self._try_save_response(result_json)

        raw_data = self.extract_data(result_json)
        parsed_data: Iterator[T] = self.parse_data(raw_data)
        self.print_renderables(parsed_data)
        if self._is_success():
            self.print_summary()
            self.print_separator()
        else:
            message = "".join(
                renderable.plain for renderable in self._generate_summary_renderables()
            )
            # Print the separator before raising so the user still sees a
            # clean divider between the partial body output and the error
            # box that the CLI framework renders for the CliError.
            self.print_separator()
            raise CliError(message)

    @staticmethod
    def format_aware_result(
        cursor: SnowflakeCursor, result_data: Any
    ) -> CollectionResult | EmptyResult:
        """Return EmptyResult for TABLE format (already printed), or CollectionResult for JSON/CSV."""
        if get_cli_context().output_format == OutputFormat.TABLE:
            return EmptyResult()
        return CollectionResult(
            [{cursor.description[0].name: result_data}],
            RespectingColumnTypesRowMapper(cursor.description),
        )

    def process(self, cursor: SnowflakeCursor) -> CommandResult:
        """Process cursor data and print results."""
        row = cursor.fetchone()
        if not row:
            cli_console.styled_message("No data.\n")
            return

        try:
            result_data = row[0]
            result_json = (
                json.loads(result_data) if isinstance(result_data, str) else result_data
            )
            self.result_raw_data = result_data
        except IndexError:
            log.info("Unexpected response format: %s", row)
            raise CliError("Could not process response.")
        except json.JSONDecodeError as e:
            log.info("Could not decode response: %s", e)
            raise CliError("Could not process response.")

        self.process_payload(result_json)

        return self.format_aware_result(cursor, result_data)
