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
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

T = TypeVar("T")


class Reporter(ABC, Generic[T]):
    def __init__(self) -> None:
        self.result_raw_data = None
        self.command_name = ""

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

    def process_payload(self, result_json: Dict[str, Any]) -> None:
        """Process already decoded response payload and print results."""
        raw_data = self.extract_data(result_json)
        parsed_data: Iterator[T] = self.parse_data(raw_data)
        self.print_renderables(parsed_data)
        if self._is_success():
            self.print_summary()
        else:
            message = "".join(
                renderable.plain for renderable in self._generate_summary_renderables()
            )
            raise CliError(message)

    def process(self, cursor: SnowflakeCursor) -> None:
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
            log.debug("Unexpected response format: %s", row)
            raise CliError("Could not process response.")
        except json.JSONDecodeError as e:
            log.debug("Could not decode response: %s", e)
            raise CliError("Could not process response.")

        self.process_payload(result_json)
