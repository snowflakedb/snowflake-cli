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
Core abstractions for the enhanced configuration system.

This module implements the foundational data structures and interfaces:
- ConfigValue: Immutable value container with provenance
- ValueSource: Common protocol for all configuration sources
- ResolutionHistory: Tracks the complete resolution process
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Literal, Optional

from snowflake.cli.api.config_ng.masking import mask_sensitive_value


class SourceType(Enum):
    """
    Classification of configuration sources by merging behavior.

    FILE sources use connection-level replacement (later file replaces entire connection).
    OVERLAY sources use field-level overlay (add/override individual fields).
    """

    FILE = "file"
    OVERLAY = "overlay"


@dataclass(frozen=True)
class ConfigValue:
    """
    Immutable configuration value with full provenance tracking.
    Stores both parsed value and original raw value.
    """

    key: str
    value: Any
    source_name: str
    raw_value: Optional[Any] = None

    def __repr__(self) -> str:
        """Readable representation showing conversion if applicable."""
        value_display = f"{self.value}"
        if self.raw_value is not None and self.raw_value != self.value:
            value_display = f"{self.raw_value} → {self.value}"
        return f"ConfigValue({self.key}={value_display}, from {self.source_name})"

    @classmethod
    def from_source(
        cls,
        key: str,
        raw_value: str,
        source_name: str,
        value_parser: Optional[Callable[[str], Any]] = None,
    ) -> ConfigValue:
        """
        Factory method to create ConfigValue from a source.

        Args:
            key: Configuration key
            raw_value: Raw string value from the source
            source_name: Name of the configuration source
            value_parser: Optional parser function; if None, raw_value is used as-is

        Returns:
            ConfigValue instance with parsed value
        """
        parsed_value = value_parser(raw_value) if value_parser else raw_value
        return cls(
            key=key,
            value=parsed_value,
            source_name=source_name,
            raw_value=raw_value,
        )


class ValueSource(ABC):
    """
    Common interface for all configuration sources.
    All implementations are READ-ONLY discovery mechanisms.
    Precedence is determined by the order sources are provided to the resolver.
    """

    # Allowed source names for config resolution
    SourceName = Literal[
        "snowsql_config",
        "cli_config_toml",
        "connections_toml",
        "snowsql_env",
        "connection_specific_env",
        "cli_env",
        "cli_arguments",
    ]

    @property
    @abstractmethod
    def source_name(self) -> SourceName:
        """
        Unique identifier for this source.
        Examples: "cli_arguments", "snowsql_config", "cli_env"
        """
        ...

    @property
    @abstractmethod
    def source_type(self) -> SourceType:
        """
        Classification of this source for merging behavior.
        FILE sources replace entire connections, OVERLAY sources merge per-field.
        """
        ...

    @abstractmethod
    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Discover configuration values as nested dict structure.

        Sources return configuration as nested dictionaries that reflect
        the natural structure of the configuration. For example:
            {"connections": {"prod": {"account": "val"}}}

        Empty connections are represented as empty dicts:
            {"connections": {"prod": {}}}

        General parameters (not connection-specific) are at the root level:
            {"database": "mydb", "role": "myrole"}

        Args:
            key: Specific key path to discover (dot-separated), or None for all

        Returns:
            Nested dictionary of configuration values. Returns empty dict
            if no values found.
        """
        ...

    @abstractmethod
    def supports_key(self, key: str) -> bool:
        """
        Check if this source can provide the given configuration key.

        Args:
            key: Configuration key to check

        Returns:
            True if this source supports the key, False otherwise
        """
        ...


@dataclass(frozen=True)
class ResolutionEntry:
    """
    Represents a single value discovery during resolution.
    Immutable record of what was found where and when.
    """

    config_value: ConfigValue
    timestamp: datetime
    was_used: bool
    overridden_by: Optional[str] = None


@dataclass
class ResolutionHistory:
    """
    Complete resolution history for a single configuration key.
    Shows the full precedence chain from lowest to highest priority.
    """

    key: str
    entries: List[ResolutionEntry] = field(default_factory=list)
    final_value: Optional[Any] = None
    default_used: bool = False

    @property
    def sources_consulted(self) -> List[str]:
        """List of all source names that were consulted."""
        return [entry.config_value.source_name for entry in self.entries]

    @property
    def values_considered(self) -> List[Any]:
        """List of all values that were considered."""
        return [entry.config_value.value for entry in self.entries]

    @property
    def selected_entry(self) -> Optional[ResolutionEntry]:
        """The entry that was ultimately selected."""
        for entry in self.entries:
            if entry.was_used:
                return entry
        return None

    @property
    def overridden_entries(self) -> List[ResolutionEntry]:
        """All entries that were overridden by higher priority sources."""
        return [entry for entry in self.entries if not entry.was_used]

    def format_chain(self) -> str:
        """
        Format the resolution chain as a readable string.

        Example output:
            account resolution chain (4 sources):
              1. ❌ snowsql_config: "old_account" (overridden by cli_arguments)
              2. ❌ toml:connections: "new_account" (overridden by cli_arguments)
              3. ❌ snowflake_cli_env: "env_account" (overridden by cli_arguments)
              4. ✅ cli_arguments: "final_account" (SELECTED)
        """
        lines = [f"{self.key} resolution chain ({len(self.entries)} sources):"]

        for i, entry in enumerate(self.entries, 1):
            cv = entry.config_value
            status_icon = "✅" if entry.was_used else "❌"

            if entry.was_used:
                status_text = "(SELECTED)"
            elif entry.overridden_by:
                status_text = f"(overridden by {entry.overridden_by})"
            else:
                status_text = "(not used)"

            # Show raw value if different from parsed value
            value_display = f'"{cv.value}"'
            if cv.raw_value is not None and cv.raw_value != cv.value:
                value_display = f'"{cv.raw_value}" → {cv.value}'

            lines.append(
                f"  {i}. {status_icon} {cv.source_name}: {value_display} {status_text}"
            )

        if self.default_used:
            lines.append(f"  Default value used: {self.final_value}")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization/export."""
        return {
            "key": self.key,
            "final_value": mask_sensitive_value(self.key, self.final_value),
            "default_used": self.default_used,
            "sources_consulted": self.sources_consulted,
            "entries": [
                {
                    "source": entry.config_value.source_name,
                    "value": mask_sensitive_value(
                        entry.config_value.key, entry.config_value.value
                    ),
                    "raw_value": mask_sensitive_value(
                        entry.config_value.key, entry.config_value.raw_value
                    ),
                    "was_used": entry.was_used,
                    "overridden_by": entry.overridden_by,
                    "timestamp": entry.timestamp.isoformat(),
                }
                for entry in self.entries
            ],
        }


@dataclass(frozen=True)
class SourceDiagnostic:
    """
    Diagnostic message emitted while discovering configuration sources.
    """

    source_name: str
    level: Literal["info", "warning", "error"]
    message: str
