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

"""Manager for configuration sources."""

from typing import Any, Dict, List, Optional

from snowflake.cli.api.config_ng.constants import FILE_SOURCE_NAMES
from snowflake.cli.api.config_ng.core import ValueSource


class SourceManager:
    """
    Manages configuration sources and derives their priorities.

    Provides a clean interface for working with configuration sources
    without exposing implementation details.
    """

    def __init__(self, sources: List[ValueSource]):
        """
        Initialize with a list of sources.

        Args:
            sources: List of sources in precedence order (lowest to highest)
        """
        self._sources = sources

    @classmethod
    def with_default_sources(
        cls, cli_context: Optional[Dict[str, Any]] = None
    ) -> "SourceManager":
        """
        Class method constructor with default sources.

        Args:
            cli_context: Optional CLI context for CliParameters source

        Returns:
            SourceManager configured with default 7-source stack
        """
        from snowflake.cli.api.config_ng.source_factory import create_default_sources

        sources = create_default_sources(cli_context)
        return cls(sources)

    def get_source_priorities(self) -> Dict[str, int]:
        """
        Derive priorities from source list order.

        Priority numbers are 1-indexed (1 = lowest, higher = higher priority).
        This is dynamically derived from the source list order to eliminate
        duplication and ensure consistency.

        Returns:
            Dictionary mapping source names to priority levels
        """
        return {s.source_name: idx + 1 for idx, s in enumerate(self._sources)}

    def get_file_sources(self) -> List[ValueSource]:
        """
        Get only file-based sources.

        Returns:
            List of sources that are file-based
        """
        return [s for s in self._sources if s.source_name in FILE_SOURCE_NAMES]

    def get_sources(self) -> List[ValueSource]:
        """
        Get all sources.

        Returns:
            Copy of the sources list
        """
        return self._sources.copy()
