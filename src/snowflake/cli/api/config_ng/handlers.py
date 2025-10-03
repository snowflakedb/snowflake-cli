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

"""
Configuration handlers for specific formats and schemas.

This module will implement specific handlers for:
- Environment variables (SNOWFLAKE_*, SNOWSQL_*)
- File formats (TOML, SnowSQL config, JSON, YAML)

To be implemented in Phase 3-4.
"""

from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Dict, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, ValueSource


class SourceHandler(ValueSource):
    """
    Specific handler for a configuration format or schema.
    Examples: TOML files, SnowSQL config, SNOWFLAKE_* env vars, etc.
    """

    @property
    @abstractmethod
    def handler_type(self) -> str:
        """
        Type identifier for this handler.
        Examples: 'toml', 'json', 'snowsql_env', 'snowsql_config'
        """
        ...

    @abstractmethod
    def can_handle(self) -> bool:
        """
        Check if this handler is applicable/available.

        Returns:
            True if handler can be used, False otherwise
        """
        ...

    def can_handle_file(self, file_path: Path) -> bool:
        """
        Check if this handler can process the given file.

        Args:
            file_path: Path to file to check

        Returns:
            True if handler can process this file, False otherwise
        """
        return False

    def discover_from_file(
        self, file_path: Path, key: Optional[str] = None
    ) -> Dict[str, ConfigValue]:
        """
        Discover values from a file.

        Args:
            file_path: Path to file to read
            key: Specific key to discover, or None for all

        Returns:
            Dictionary of discovered values
        """
        return {}
