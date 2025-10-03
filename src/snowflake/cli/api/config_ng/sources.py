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
Top-level configuration sources.

This module implements the top-level configuration sources that orchestrate
handlers and provide configuration values according to precedence rules.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority, ValueSource

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.handlers import SourceHandler

log = logging.getLogger(__name__)


class ConfigurationSource(ValueSource):
    """
    Base class for top-level sources that may delegate to handlers.
    Handlers are tried IN ORDER - first handler with value wins.
    """

    def __init__(self, handlers: Optional[List["SourceHandler"]] = None):
        """
        Initialize with ordered list of sub-handlers.

        Args:
            handlers: List of handlers in priority order (first = highest)
        """
        self._handlers = handlers or []

    @abstractmethod
    def discover_direct(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover values directly from this source (without handlers).
        Direct values always take precedence over handler values.

        Returns:
            Dictionary of directly discovered values
        """
        ...

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover values from handlers and direct sources.

        Precedence within this source:
        1. Direct values (highest)
        2. First handler with value
        3. Second handler with value
        4. ... and so on

        Args:
            key: Specific key to discover, or None for all

        Returns:
            Dictionary of all discovered values with precedence applied
        """
        discovered: Dict[str, ConfigValue] = {}

        # Process handlers in ORDER (first wins for same key)
        for handler in self._handlers:
            try:
                handler_values = handler.discover(key)
                for k, v in handler_values.items():
                    if k not in discovered:  # First handler wins
                        discovered[k] = v
            except Exception as e:
                log.debug("Handler %s failed: %s", handler.source_name, e)

        # Direct values override all handlers
        direct_values = self.discover_direct(key)
        discovered.update(direct_values)

        return discovered

    def add_handler(self, handler: "SourceHandler", position: int = -1) -> None:
        """
        Add handler at specific position.

        Args:
            handler: Handler to add
            position: Insert position (-1 = append, 0 = prepend)
        """
        if position == -1:
            self._handlers.append(handler)
        else:
            self._handlers.insert(position, handler)

    def set_handlers(self, handlers: List["SourceHandler"]) -> None:
        """Replace all handlers with new ordered list."""
        self._handlers = handlers

    def get_handlers(self) -> List["SourceHandler"]:
        """Get current handler list (for inspection/reordering)."""
        return self._handlers.copy()


class CliArgumentSource(ConfigurationSource):
    """
    Source for command-line arguments.
    Highest priority source with no sub-handlers.
    Values come directly from parsed CLI arguments.
    """

    def __init__(self, cli_context: Optional[Dict[str, Any]] = None):
        """
        Initialize with CLI context containing parsed arguments.

        Args:
            cli_context: Dictionary of CLI arguments (key -> value)
        """
        super().__init__(handlers=[])  # No handlers needed
        self._cli_context = cli_context or {}

    @property
    def source_name(self) -> str:
        return "cli_arguments"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.CLI_ARGUMENT

    def discover_direct(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Extract non-None values from CLI context.
        CLI arguments are already parsed by Typer/Click.
        """
        values = {}

        if key is not None:
            # Discover specific key
            if key in self._cli_context and self._cli_context[key] is not None:
                values[key] = ConfigValue(
                    key=key,
                    value=self._cli_context[key],
                    source_name=self.source_name,
                    priority=self.priority,
                    raw_value=self._cli_context[key],
                )
        else:
            # Discover all present values
            for k, v in self._cli_context.items():
                if v is not None:
                    values[k] = ConfigValue(
                        key=k,
                        value=v,
                        source_name=self.source_name,
                        priority=self.priority,
                        raw_value=v,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        """Check if key is present in CLI context."""
        return key in self._cli_context


class EnvironmentSource(ConfigurationSource):
    """
    Source for environment variables with handler precedence.

    Default Handler Order (supports migration):
    1. SnowCliEnvHandler (SNOWFLAKE_*) ← Check first
    2. SnowSqlEnvHandler (SNOWSQL_*)   ← Fallback for legacy

    This allows users to:
    - Start with only SNOWSQL_* vars (works)
    - Add SNOWFLAKE_* vars (automatically override SNOWSQL_*)
    - Gradually migrate without breaking anything
    """

    def __init__(self, handlers: Optional[List["SourceHandler"]] = None):
        """
        Initialize with ordered handlers.

        Args:
            handlers: Custom handler list, or None for default
        """
        super().__init__(handlers=handlers or [])

    @property
    def source_name(self) -> str:
        return "environment"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.ENVIRONMENT

    def discover_direct(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Environment source has no direct values.
        All values come from handlers.
        """
        return {}

    def supports_key(self, key: str) -> bool:
        """Check if any handler supports this key."""
        return any(h.supports_key(key) for h in self._handlers)


class FileSource(ConfigurationSource):
    """
    Source for configuration files with handler precedence.

    Default Handler Order (supports migration):
    1. SnowCLI TOML handlers (config.toml, connections.toml) ← Check first
    2. SnowSQL config handler (~/.snowsql/config)           ← Fallback

    File Path Order:
    - Earlier paths take precedence over later ones
    - Allows user-specific configs to override system configs
    """

    def __init__(
        self,
        file_paths: Optional[List[Path]] = None,
        handlers: Optional[List["SourceHandler"]] = None,
    ):
        """
        Initialize with file paths and handlers.

        Args:
            file_paths: Ordered list of file paths (first = highest precedence)
            handlers: Ordered list of format handlers (first = highest precedence)
        """
        super().__init__(handlers=handlers or [])
        self._file_paths = file_paths or []

    @property
    def source_name(self) -> str:
        return "configuration_files"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.FILE

    def discover_direct(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        File source has no direct values.
        All values come from file handlers.
        """
        return {}

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Try each file path with each handler.

        Precedence:
        1. First file path with value
           a. First handler that can read it with value
        2. Second file path with value
           a. First handler that can read it with value
        ...

        Args:
            key: Specific key to discover, or None for all

        Returns:
            Dictionary of discovered values with precedence applied
        """
        discovered: Dict[str, ConfigValue] = {}

        for file_path in self._file_paths:
            if not file_path.exists():
                continue

            for handler in self._handlers:
                if not handler.can_handle_file(file_path):
                    continue

                try:
                    handler_values = handler.discover_from_file(file_path, key)
                    # First file+handler combination wins
                    for k, v in handler_values.items():
                        if k not in discovered:
                            discovered[k] = v
                except Exception as e:
                    log.debug(
                        "Handler %s failed for %s: %s",
                        handler.source_name,
                        file_path,
                        e,
                    )

        return discovered

    def supports_key(self, key: str) -> bool:
        """Check if any handler supports this key."""
        return any(h.supports_key(key) for h in self._handlers)

    def get_file_paths(self) -> List[Path]:
        """Get current file paths list (for inspection)."""
        return self._file_paths.copy()

    def add_file_path(self, file_path: Path, position: int = -1) -> None:
        """
        Add file path at specific position.

        Args:
            file_path: Path to add
            position: Insert position (-1 = append, 0 = prepend)
        """
        if position == -1:
            self._file_paths.append(file_path)
        else:
            self._file_paths.insert(position, file_path)

    def set_file_paths(self, file_paths: List[Path]) -> None:
        """Replace all file paths with new ordered list."""
        self._file_paths = file_paths
