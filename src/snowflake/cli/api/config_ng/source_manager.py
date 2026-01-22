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

from typing import List

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

    def get_sources(self) -> List[ValueSource]:
        """
        Get all sources.

        Returns:
            Copy of the sources list
        """
        return self._sources.copy()
