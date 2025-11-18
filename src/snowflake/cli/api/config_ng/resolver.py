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
Configuration resolver with resolution history tracking.

This module implements:
- ResolutionHistoryTracker: Tracks configuration value discoveries and precedence
- ConfigurationResolver: Orchestrates sources and resolves configuration values
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
    SourceType,
)

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.core import ValueSource

log = logging.getLogger(__name__)


def _sanitize_source_error(exc: Exception) -> str:
    """
    Produce a logging-safe description of discovery errors.

    Keys and structural metadata (section/key/line) are preserved, but raw
    values are never rendered so sensitive data cannot leak through logs.
    """

    safe_parts: List[str] = [exc.__class__.__name__]
    attribute_labels = (
        ("section", "section"),
        ("option", "key"),
        ("key", "key"),
        ("lineno", "line"),
        ("colno", "column"),
        ("pos", "position"),
    )

    for attr_name, label in attribute_labels:
        attr_value = getattr(exc, attr_name, None)
        if attr_value:
            safe_parts.append(f"{label}={attr_value}")

    if len(safe_parts) == 1:
        safe_parts.append("details_masked")

    return ", ".join(safe_parts)


class ResolutionHistoryTracker:
    """
    Tracks the complete resolution process for all configuration keys.

    This class records:
    - Every value discovered from every source
    - The order in which values were considered
    - Which value was ultimately selected
    - Which values were overridden and by what

    Provides debugging utilities and export functionality.
    """

    def __init__(self):
        """Initialize empty history tracker."""
        self._histories: Dict[str, ResolutionHistory] = {}
        self._discoveries: Dict[str, List[tuple[ConfigValue, datetime]]] = defaultdict(
            list
        )
        self._enabled = True

    def enable(self) -> None:
        """Enable history tracking."""
        self._enabled = True

    def disable(self) -> None:
        """Disable history tracking for performance."""
        self._enabled = False

    def is_enabled(self) -> bool:
        """Check if history tracking is enabled."""
        return self._enabled

    def clear(self) -> None:
        """Clear all recorded history."""
        self._histories.clear()
        self._discoveries.clear()

    def _flatten_nested_dict(
        self, nested: Dict[str, Any], prefix: str = ""
    ) -> Dict[str, Any]:
        """
        Flatten nested dict to dot-separated keys for internal storage.

        Args:
            nested: Nested dictionary structure
            prefix: Current key prefix

        Returns:
            Flat dictionary with dot-separated keys

        Example:
            {"connections": {"test": {"account": "val"}}}
            -> {"connections.test.account": "val"}
        """
        result = {}
        for key, value in nested.items():
            flat_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict) and value:
                # Recursively flatten nested dicts
                result.update(self._flatten_nested_dict(value, flat_key))
            else:
                # Leaf value - store it
                result[flat_key] = value

        return result

    def record_nested_discovery(
        self, nested_data: Dict[str, Any], source_name: str
    ) -> None:
        """
        Record discoveries from a source that returns nested dict.

        Args:
            nested_data: Nested dictionary from source
            source_name: Name of the source providing this data
        """
        if not self._enabled:
            return

        flat_data = self._flatten_nested_dict(nested_data)

        timestamp = datetime.now()
        for flat_key, value in flat_data.items():
            config_value = ConfigValue(
                key=flat_key, value=value, source_name=source_name
            )
            self._discoveries[flat_key].append((config_value, timestamp))

    def record_discovery(self, key: str, config_value: ConfigValue) -> None:
        """
        Record a value discovery from a source.

        Args:
            key: Configuration key
            config_value: The discovered ConfigValue with metadata
        """
        if not self._enabled:
            return

        timestamp = datetime.now()
        self._discoveries[key].append((config_value, timestamp))

    def mark_selected(self, key: str, source_name: str) -> None:
        """
        Mark which source's value was selected for a key.

        Args:
            key: Configuration key
            source_name: Name of the source whose value was selected
        """
        if not self._enabled or key not in self._discoveries:
            return

        entries: List[ResolutionEntry] = []
        selected_value = None

        for config_value, timestamp in self._discoveries[key]:
            was_selected = config_value.source_name == source_name
            overridden_by = source_name if not was_selected else None

            entry = ResolutionEntry(
                config_value=config_value,
                timestamp=timestamp,
                was_used=was_selected,
                overridden_by=overridden_by,
            )
            entries.append(entry)

            if was_selected:
                selected_value = config_value.value

        self._histories[key] = ResolutionHistory(
            key=key, entries=entries, final_value=selected_value, default_used=False
        )

    def mark_default_used(self, key: str, default_value: Any) -> None:
        """
        Mark that a default value was used for a key.

        Args:
            key: Configuration key
            default_value: The default value used
        """
        if not self._enabled:
            return

        if key in self._histories:
            self._histories[key].default_used = True
            self._histories[key].final_value = default_value
        else:
            self._histories[key] = ResolutionHistory(
                key=key, entries=[], final_value=default_value, default_used=True
            )

    def get_history(self, key: str) -> Optional[ResolutionHistory]:
        """
        Get resolution history for a specific key.

        Args:
            key: Configuration key

        Returns:
            ResolutionHistory object or None if key not tracked
        """
        return self._histories.get(key)

    def get_all_histories(self) -> Dict[str, ResolutionHistory]:
        """
        Get all resolution histories.

        Returns:
            Dictionary mapping keys to their ResolutionHistory objects
        """
        return self._histories.copy()

    def finalize_with_result(self, final_config: Dict[str, Any]) -> None:
        """
        Mark which values were selected in the final configuration.

        This method flattens the final nested config and marks the selected
        source for each value.

        Args:
            final_config: The final resolved configuration (nested dict)
        """
        if not self._enabled:
            return

        flat_final = self._flatten_nested_dict(final_config)

        for flat_key, final_value in flat_final.items():
            if flat_key not in self._discoveries:
                continue

            discoveries = self._discoveries[flat_key]
            for config_value, timestamp in reversed(discoveries):
                if config_value.value == final_value:
                    self.mark_selected(flat_key, config_value.source_name)
                    break

    def record_general_params_merged_to_connections(
        self,
        general_params: Dict[str, Any],
        connection_names: List[str],
        source_name: str,
    ) -> None:
        """
        Record when general parameters are merged into connections.

        When overlay sources provide general params (like SNOWFLAKE_ACCOUNT),
        these get merged into each existing connection. This method records
        that merge operation for history tracking.

        Args:
            general_params: Dictionary of general parameters
            connection_names: List of connection names to merge into
            source_name: Name of the source providing these params
        """
        if not self._enabled:
            return

        timestamp = datetime.now()
        for param_key, param_value in general_params.items():
            for conn_name in connection_names:
                flat_key = f"connections.{conn_name}.{param_key}"
                config_value = ConfigValue(
                    key=flat_key, value=param_value, source_name=source_name
                )
                self._discoveries[flat_key].append((config_value, timestamp))

    def replicate_root_level_discoveries_to_connection(
        self, param_keys: List[str], connection_name: str
    ) -> None:
        """
        Replicate discoveries from root-level keys to connection-specific keys.

        This is used when creating a default connection from general parameters
        (e.g., SNOWFLAKE_ACCOUNT -> connections.default.account).

        Args:
            param_keys: List of parameter keys that exist at root level
            connection_name: Name of the connection to replicate discoveries to
        """
        if not self._enabled:
            return

        for param_key in param_keys:
            if param_key in self._discoveries:
                conn_key = f"connections.{connection_name}.{param_key}"
                for config_value, timestamp in self._discoveries[param_key]:
                    self._discoveries[conn_key].append((config_value, timestamp))

    def get_summary(self) -> dict:
        """
        Get summary statistics about configuration resolution.

        Returns:
            Dictionary with statistics:
            - total_keys_resolved: Number of keys resolved
            - keys_with_overrides: Number of keys where values were overridden
            - keys_using_defaults: Number of keys using default values
            - source_usage: Dict of source_name -> count of values provided
            - source_wins: Dict of source_name -> count of values selected
        """
        total_keys = len(self._histories)
        keys_with_overrides = sum(
            1 for h in self._histories.values() if len(h.overridden_entries) > 0
        )
        keys_using_defaults = sum(1 for h in self._histories.values() if h.default_used)

        source_usage: Dict[str, int] = defaultdict(int)
        source_wins: Dict[str, int] = defaultdict(int)

        for history in self._histories.values():
            for entry in history.entries:
                source_usage[entry.config_value.source_name] += 1
                if entry.was_used:
                    source_wins[entry.config_value.source_name] += 1

        return {
            "total_keys_resolved": total_keys,
            "keys_with_overrides": keys_with_overrides,
            "keys_using_defaults": keys_using_defaults,
            "source_usage": dict(source_usage),
            "source_wins": dict(source_wins),
        }


class ConfigurationResolver:
    """
    Orchestrates configuration sources with resolution history tracking.

    This is the main entry point for configuration resolution. It:
    - Manages multiple configuration sources in precedence order
    - Applies precedence rules based on source list order
    - Tracks complete resolution history

    Sources should be provided in precedence order (lowest to highest priority).
    Later sources in the list override earlier sources.

    For presentation/formatting of resolution data, use ResolutionPresenter
    from the presentation module.

    Example:
        from snowflake.cli.api.config_ng import ConfigurationResolver
        from snowflake.cli.api.config_ng.presentation import ResolutionPresenter

        resolver = ConfigurationResolver(
            sources=[
                snowsql_config,     # Lowest priority
                cli_config,
                env_source,
                cli_arguments,      # Highest priority
            ]
        )

        # Resolve all configuration
        config = resolver.resolve()

        # For debugging/presentation, use the presenter
        presenter = ResolutionPresenter(resolver)
        presenter.print_resolution_chain("account")
        presenter.export_history(Path("debug_config.json"))
    """

    def __init__(
        self,
        sources: Optional[List["ValueSource"]] = None,
    ):
        """
        Initialize resolver with sources and history tracking.

        Args:
            sources: List of configuration sources in precedence order
                    (first = lowest priority, last = highest priority)
        """
        self._sources = sources or []
        self._history_tracker = ResolutionHistoryTracker()

    def add_source(self, source: "ValueSource") -> None:
        """
        Add a configuration source to the end of the list (highest priority).

        Args:
            source: ValueSource to add
        """
        self._sources.append(source)

    def get_sources(self) -> List["ValueSource"]:
        """Get list of all sources in precedence order (for inspection)."""
        return self._sources.copy()

    def _parse_connection_key(self, key: str) -> Optional[Tuple[str, str]]:
        """
        Parse a connection key into (connection_name, parameter).

        Args:
            key: Configuration key (e.g., "connections.prod.account")

        Returns:
            Tuple of (connection_name, parameter) or None if not a connection key
        """
        if not key.startswith("connections."):
            return None

        parts = key.split(".", 2)
        if len(parts) != 3:
            return None

        return parts[1], parts[2]  # (conn_name, param)

    def _get_sources_by_type(self, source_type: SourceType) -> List["ValueSource"]:
        """
        Get all sources matching the specified type.

        Args:
            source_type: Type of source to filter by

        Returns:
            List of sources matching the type
        """
        return [s for s in self._sources if s.source_type is source_type]

    def _record_discoveries(self, source_values: Dict[str, ConfigValue]) -> None:
        """
        Record all discovered values in history tracker.

        Args:
            source_values: Dictionary of discovered configuration values
        """
        for k, config_value in source_values.items():
            self._history_tracker.record_discovery(k, config_value)

    def _finalize_history(self, all_values: Dict[str, ConfigValue]) -> None:
        """
        Mark which values were selected in resolution history.

        Args:
            all_values: Final dictionary of selected configuration values
        """
        for k, config_value in all_values.items():
            self._history_tracker.mark_selected(k, config_value.source_name)

    def _apply_default(
        self, resolved: Dict[str, Any], key: str, default: Any
    ) -> Dict[str, Any]:
        """
        Apply default value for a specific key if provided.

        Args:
            resolved: Current resolved configuration dictionary
            key: Configuration key
            default: Default value to apply

        Returns:
            Updated resolved dictionary
        """
        if default is not None:
            resolved[key] = default
            self._history_tracker.mark_default_used(key, default)
        return resolved

    def _group_by_connection(
        self, source_values: Dict[str, ConfigValue]
    ) -> Tuple[Dict[str, Dict[str, ConfigValue]], set[str]]:
        """
        Group connection parameters by connection name.

        Args:
            source_values: All values discovered from a source

        Returns:
            Tuple of (per_conn, empty_connections):
            - per_conn: Dict mapping connection name to its ConfigValue parameters
            - empty_connections: Set of connection names that are empty
        """
        per_conn: Dict[str, Dict[str, ConfigValue]] = defaultdict(dict)
        empty_connections: set[str] = set()

        for k, config_value in source_values.items():
            parsed = self._parse_connection_key(k)
            if parsed is None:
                continue

            conn_name, param = parsed

            # Track empty connection markers
            if param == "_empty_connection":
                empty_connections.add(conn_name)
            else:
                per_conn[conn_name][k] = config_value

        return per_conn, empty_connections

    def _extract_flat_values(
        self, source_values: Dict[str, ConfigValue]
    ) -> Dict[str, ConfigValue]:
        """
        Extract non-connection (flat) configuration values.

        Args:
            source_values: All values discovered from a source

        Returns:
            Dictionary of flat configuration values (non-connection keys)
        """
        return {
            k: v for k, v in source_values.items() if not k.startswith("connections.")
        }

    def _replace_connections(
        self,
        file_connections: Dict[str, Dict[str, ConfigValue]],
        per_conn: Dict[str, Dict[str, ConfigValue]],
        empty_connections: set[str],
        source: "ValueSource",
    ) -> None:
        """
        Replace entire connections with new definitions from source.

        This implements connection-level replacement: when a FILE source defines
        a connection, it completely replaces any previous definition.

        Args:
            file_connections: Accumulator for all file-based connections
            per_conn: New connection definitions from current source
            empty_connections: Set of empty connection names from current source
            source: The source providing these connections
        """
        all_conn_names = set(per_conn.keys()) | empty_connections

        for conn_name in all_conn_names:
            conn_params = per_conn.get(conn_name, {})
            log.debug(
                "Connection %s replaced by file source %s (%d params)",
                conn_name,
                source.source_name,
                len(conn_params),
            )
            file_connections[conn_name] = conn_params

    def _resolve_file_sources(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Process FILE sources with connection-level replacement semantics.

        FILE sources replace entire connections rather than merging fields.
        Later FILE sources override earlier ones completely.

        Args:
            key: Specific key to resolve (None = all keys)

        Returns:
            Nested dict with merged file source data
        """
        result: Dict[str, Any] = {}

        for source in self._get_sources_by_type(SourceType.FILE):
            try:
                source_data = source.discover(key)

                self._history_tracker.record_nested_discovery(
                    source_data, source.source_name
                )

                if "connections" in source_data:
                    if "connections" not in result:
                        result["connections"] = {}

                    for conn_name, conn_data in source_data["connections"].items():
                        result["connections"][conn_name] = conn_data

                for k, v in source_data.items():
                    if k != "connections":
                        result[k] = v

            except Exception as exc:
                sanitized_error = _sanitize_source_error(exc)
                log.warning(
                    "Error from source %s: %s", source.source_name, sanitized_error
                )
                log.debug(
                    "Error from source %s (full details hidden in warnings)",
                    source.source_name,
                    exc_info=exc,
                )

        return result

    def _merge_file_results(
        self,
        file_connections: Dict[str, Dict[str, ConfigValue]],
        file_flat_values: Dict[str, ConfigValue],
    ) -> Dict[str, ConfigValue]:
        """
        Merge file connections and flat values into single dictionary.

        Args:
            file_connections: Connection parameters from file sources
            file_flat_values: Flat configuration values from file sources

        Returns:
            Merged dictionary of all file-based configuration values
        """
        all_values: Dict[str, ConfigValue] = {}

        for conn_params in file_connections.values():
            all_values.update(conn_params)

        all_values.update(file_flat_values)

        return all_values

    def _apply_overlay_sources(
        self, base: Dict[str, Any], key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply OVERLAY sources with field-level merging.

        OVERLAY sources (env vars, CLI args) add or override individual fields
        without replacing entire connections. General params are merged into
        each existing connection.

        Args:
            base: Base configuration (typically from file sources)
            key: Specific key to resolve (None = all keys)

        Returns:
            Updated dictionary with overlay values applied
        """
        from snowflake.cli.api.config_ng.dict_utils import deep_merge
        from snowflake.cli.api.config_ng.merge_operations import (
            extract_root_level_connection_params,
            merge_params_into_connections,
        )

        result = base.copy()

        for source in self._get_sources_by_type(SourceType.OVERLAY):
            try:
                source_data = source.discover(key)

                self._history_tracker.record_nested_discovery(
                    source_data, source.source_name
                )

                general_params, other_data = extract_root_level_connection_params(
                    source_data
                )

                result = deep_merge(result, other_data)

                if general_params and "connections" in result and result["connections"]:
                    connection_names = [
                        name
                        for name in result["connections"]
                        if isinstance(result["connections"][name], dict)
                    ]

                    self._history_tracker.record_general_params_merged_to_connections(
                        general_params, connection_names, source.source_name
                    )

                    result["connections"] = merge_params_into_connections(
                        result["connections"], general_params
                    )
                elif general_params:
                    result = deep_merge(result, general_params)

            except Exception as exc:
                sanitized_error = _sanitize_source_error(exc)
                log.warning(
                    "Error from source %s: %s", source.source_name, sanitized_error
                )
                log.debug(
                    "Error from source %s (full details hidden in warnings)",
                    source.source_name,
                    exc_info=exc,
                )

        if "connections" in result and result["connections"]:
            remaining_general_params, _ = extract_root_level_connection_params(result)

            if remaining_general_params:
                for conn_name in result["connections"]:
                    if isinstance(result["connections"][conn_name], dict):
                        result["connections"][conn_name] = deep_merge(
                            remaining_general_params, result["connections"][conn_name]
                        )

                for key in remaining_general_params:
                    if key in result:
                        result.pop(key)

        return result

    def _ensure_default_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure a default connection exists when general connection params are present.

        Border conditions for creating default connection:
        1. No connections exist in config (empty or missing "connections" key)
        2. At least one general connection parameter exists at root level
        3. General params are NOT internal CLI parameters or variables

        This allows users to set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER etc. without
        needing --temporary-connection flag or defining connections in config files.

        Args:
            config: Resolved configuration dictionary

        Returns:
            Configuration with default connection created if conditions are met
        """
        from snowflake.cli.api.config_ng.constants import INTERNAL_CLI_PARAMETERS

        connections = config.get("connections", {})
        if connections:
            return config

        general_params = {}
        for key, value in config.items():
            if (
                key not in ("connections", "variables")
                and key not in INTERNAL_CLI_PARAMETERS
            ):
                general_params[key] = value

        if not general_params:
            return config

        result = config.copy()
        result["connections"] = {"default": general_params.copy()}

        self._history_tracker.replicate_root_level_discoveries_to_connection(
            list(general_params.keys()), "default"
        )

        for key in general_params:
            result.pop(key, None)

        return result

    def resolve(self, key: Optional[str] = None, default: Any = None) -> Dict[str, Any]:
        """
        Resolve configuration to nested dict.

        Resolution Process (Four-Phase):

        Phase A - File Sources (Connection-Level Replacement):
        - Process FILE sources in precedence order (lowest to highest priority)
        - For each connection, later FILE sources completely REPLACE earlier ones
        - Fields from earlier file sources are NOT inherited

        Phase B - Overlay Sources (Field-Level Overlay):
        - Start with the file-derived configuration
        - Process OVERLAY sources (env vars, CLI args) in precedence order
        - These add/override individual fields without replacing entire connections
        - Uses deep merge for nested structures

        Phase C - Default Connection Creation:
        - If no connections exist but general params present, create "default" connection
        - Allows env-only configuration without --temporary-connection flag

        Phase D - Resolution History Finalization:
        - Mark which values were selected in the final configuration
        - Enables debugging and diagnostics

        Args:
            key: Specific key to resolve (None = all keys)
            default: Default value if key not found

        Returns:
            Nested dictionary of resolved configuration
        """
        result = self._resolve_file_sources(key)

        result = self._apply_overlay_sources(result, key)

        result = self._ensure_default_connection(result)

        self._finalize_resolution_history(result)

        return result

    def resolve_value(self, key: str, default: Any = None) -> Any:
        """
        Resolve a single configuration value.

        Args:
            key: Configuration key
            default: Default value if not found

        Returns:
            Resolved value or default
        """
        resolved = self.resolve(key=key, default=default)
        return resolved.get(key, default)

    def get_value_metadata(self, key: str) -> Optional[ConfigValue]:
        """
        Get metadata for the selected value.

        Args:
            key: Configuration key

        Returns:
            ConfigValue for the selected value, or None if not found
        """
        history = self._history_tracker.get_history(key)
        if history and history.selected_entry:
            return history.selected_entry.config_value

        # Fallback to live query if history not available
        for source in self._sources:
            values = source.discover(key)
            if key in values:
                return values[key]

        return None

    def get_tracker(self) -> ResolutionHistoryTracker:
        """
        Get the history tracker for direct access to resolution data.

        Returns:
            ResolutionHistoryTracker instance
        """
        return self._history_tracker

    def _finalize_resolution_history(self, final_config: Dict[str, Any]) -> None:
        """
        Mark which values were selected in final configuration.

        Delegates to the history tracker which handles all history-related logic.

        Args:
            final_config: The final resolved configuration (nested dict)
        """
        self._history_tracker.finalize_with_result(final_config)

    def get_resolution_history(self, key: str) -> Optional[ResolutionHistory]:
        """
        Get complete resolution history for a key.

        Supports both formats:
        - Flat: "connections.test.account"
        - Root-level: "account" (checks connections for this key)

        Args:
            key: Configuration key (flat or simple)

        Returns:
            ResolutionHistory showing the full precedence chain
        """
        history = self._history_tracker.get_history(key)
        if history:
            return history

        # If not found and it's a simple key (no dots), search in connections
        if "." not in key:
            # Look for any connection that has this key
            all_histories = self._history_tracker.get_all_histories()
            for hist_key, hist in all_histories.items():
                # Match pattern: "connections.*.{key}" or root level "{key}"
                if hist_key.endswith(f".{key}"):
                    return hist

        return None

    def get_all_histories(self) -> Dict[str, ResolutionHistory]:
        """Get resolution histories for all keys."""
        return self._history_tracker.get_all_histories()
