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

import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
)
from snowflake.cli.api.console import cli_console

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.core import ValueSource

log = logging.getLogger(__name__)

# Sensitive configuration keys that should be masked when displayed
SENSITIVE_KEYS = {
    "password",
    "pwd",
    "oauth_client_secret",
    "token",
    "session_token",
    "master_token",
    "mfa_passcode",
    "private_key",  # Private key content (not path)
    "passphrase",
    "secret",
}

# Keys that contain file paths (paths are OK to display, but not file contents)
PATH_KEYS = {
    "private_key_file",
    "private_key_path",
    "token_file_path",
}


def _should_mask_value(key: str) -> bool:
    """
    Determine if a configuration value should be masked for security.

    Args:
        key: Configuration key name

    Returns:
        True if the value should be masked, False if it can be displayed
    """
    key_lower = key.lower()

    # Check if it's a path key (paths are OK to display)
    if any(path_key in key_lower for path_key in PATH_KEYS):
        return False

    # Check if it contains sensitive keywords
    return any(sensitive_key in key_lower for sensitive_key in SENSITIVE_KEYS)


def _mask_sensitive_value(key: str, value: Any) -> str:
    """
    Mask sensitive configuration values for display.

    Args:
        key: Configuration key name
        value: Configuration value

    Returns:
        Masked representation of the value
    """
    if _should_mask_value(key):
        return "****"
    return str(value)


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

        # Build resolution history for this key
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

        # Create or update history to indicate default usage
        if key in self._histories:
            self._histories[key].default_used = True
            self._histories[key].final_value = default_value
        else:
            # No discoveries, only default
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
    Orchestrates configuration sources with full resolution history tracking.

    This is the main entry point for configuration resolution. It:
    - Manages multiple configuration sources in precedence order
    - Applies precedence rules based on source list order
    - Tracks complete resolution history
    - Provides debugging and export utilities

    Sources should be provided in precedence order (lowest to highest priority).
    Later sources in the list override earlier sources.

    Example:
        resolver = ConfigurationResolver(
            sources=[
                snowsql_config,     # Lowest priority
                cli_config,
                env_source,
                cli_arguments,      # Highest priority
            ],
            track_history=True
        )

        # Resolve all configuration
        config = resolver.resolve()

        # Debug: where did 'account' come from?
        resolver.print_resolution_chain("account")

        # Export for support
        resolver.export_history(Path("debug_config.json"))
    """

    def __init__(
        self,
        sources: Optional[List["ValueSource"]] = None,
        track_history: bool = True,
    ):
        """
        Initialize resolver with sources and history tracking.

        Args:
            sources: List of configuration sources in precedence order
                    (first = lowest priority, last = highest priority)
            track_history: Enable resolution history tracking (default: True)
        """
        self._sources = sources or []
        self._history_tracker = ResolutionHistoryTracker()

        if not track_history:
            self._history_tracker.disable()

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

    def enable_history(self) -> None:
        """Enable resolution history tracking."""
        self._history_tracker.enable()

    def disable_history(self) -> None:
        """Disable history tracking (for performance)."""
        self._history_tracker.disable()

    def clear_history(self) -> None:
        """Clear all resolution history."""
        self._history_tracker.clear()

    def resolve(self, key: Optional[str] = None, default: Any = None) -> Dict[str, Any]:
        """
        Resolve configuration values from all sources with history tracking.

        Resolution Process:
        1. Iterate sources in order (lowest to highest priority)
        2. Record all discovered values in history
        3. For connection keys (connections.{name}.{param}):
           - Merge connection-by-connection: later sources extend/overwrite individual params
        4. For flat keys: later sources overwrite earlier sources
        5. Mark which value was selected
        6. Return final resolved values

        Args:
            key: Specific key to resolve (None = all keys)
            default: Default value if key not found

        Returns:
            Dictionary of resolved values (key -> value)
        """
        all_values: Dict[str, ConfigValue] = {}
        # Track connection values separately for intelligent merging
        connections: Dict[str, Dict[str, ConfigValue]] = defaultdict(dict)

        # Process sources in order (first = lowest priority, last = highest)
        for source in self._sources:
            try:
                source_values = source.discover(key)

                # Record discoveries in history
                for k, config_value in source_values.items():
                    self._history_tracker.record_discovery(k, config_value)

                # Separate connection keys from flat keys
                for k, config_value in source_values.items():
                    if k.startswith("connections."):
                        # Parse: connections.{name}.{param}
                        parts = k.split(".", 2)
                        if len(parts) == 3:
                            conn_name = parts[1]
                            param = parts[2]
                            param_key = f"connections.{conn_name}.{param}"

                            # Merge at parameter level: later source overwrites/extends
                            connections[conn_name][param_key] = config_value
                    else:
                        # Flat key: later source overwrites
                        all_values[k] = config_value

            except Exception as e:
                log.warning("Error from source %s: %s", source.source_name, e)

        # Flatten connection data back into all_values
        for conn_name, conn_params in connections.items():
            all_values.update(conn_params)

        # Mark which values were selected in history
        for k, config_value in all_values.items():
            self._history_tracker.mark_selected(k, config_value.source_name)

        # Convert ConfigValue objects to plain values
        resolved = {k: v.value for k, v in all_values.items()}

        # Handle default for specific key
        if key is not None and key not in resolved:
            if default is not None:
                resolved[key] = default
                self._history_tracker.mark_default_used(key, default)

        return resolved

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

    def get_resolution_history(self, key: str) -> Optional[ResolutionHistory]:
        """
        Get complete resolution history for a key.

        Args:
            key: Configuration key

        Returns:
            ResolutionHistory showing the full precedence chain
        """
        return self._history_tracker.get_history(key)

    def get_all_histories(self) -> Dict[str, ResolutionHistory]:
        """Get resolution histories for all keys."""
        return self._history_tracker.get_all_histories()

    def get_history_summary(self) -> dict:
        """
        Get summary statistics about configuration resolution.

        Returns:
            Dictionary with statistics:
            - total_keys_resolved
            - keys_with_overrides
            - keys_using_defaults
            - source_usage (how many values each source provided)
            - source_wins (how many final values came from each source)
        """
        return self._history_tracker.get_summary()

    def format_resolution_chain(self, key: str) -> str:
        """
        Format the resolution chain for a key (debugging helper).

        Args:
            key: Configuration key

        Returns:
            Formatted resolution chain as a string
        """
        history = self.get_resolution_history(key)
        if history:
            return history.format_chain()
        return f"No resolution history found for key: {key}"

    def format_all_chains(self) -> str:
        """
        Format resolution chains for all keys (debugging helper).

        Returns:
            Formatted resolution chains as a string
        """
        histories = self.get_all_histories()
        if not histories:
            return "No resolution history available"

        lines = [
            f"\n{'=' * 80}",
            f"Configuration Resolution History ({len(histories)} keys)",
            f"{'=' * 80}\n",
        ]

        for key in sorted(histories.keys()):
            lines.append(histories[key].format_chain())
            lines.append("")

        return "\n".join(lines)

    def print_resolution_chain(self, key: str) -> None:
        """
        Print the resolution chain for a key using cli_console formatting.
        Sensitive values (passwords, tokens, etc.) are automatically masked.

        Args:
            key: Configuration key
        """
        history = self.get_resolution_history(key)
        if not history:
            cli_console.warning(f"No resolution history found for key: {key}")
            return

        with cli_console.phase(
            f"{key} resolution chain ({len(history.entries)} sources):"
        ):
            for i, entry in enumerate(history.entries, 1):
                cv = entry.config_value
                status_icon = "✅" if entry.was_used else "❌"

                if entry.was_used:
                    status_text = "(SELECTED)"
                elif entry.overridden_by:
                    status_text = f"(overridden by {entry.overridden_by})"
                else:
                    status_text = "(not used)"

                # Mask sensitive values
                masked_value = _mask_sensitive_value(cv.key, cv.value)
                masked_raw = (
                    _mask_sensitive_value(cv.key, cv.raw_value)
                    if cv.raw_value is not None
                    else None
                )

                # Show raw value if different from parsed value
                value_display = f'"{masked_value}"'
                if masked_raw is not None and cv.raw_value != cv.value:
                    value_display = f'"{masked_raw}" → {masked_value}'

                cli_console.step(
                    f"{i}. {status_icon} {cv.source_name}: {value_display} {status_text}"
                )

            if history.default_used:
                masked_default = _mask_sensitive_value(key, history.final_value)
                cli_console.step(f"Default value used: {masked_default}")

    def print_all_chains(self) -> None:
        """
        Print resolution chains for all keys using cli_console formatting.
        Sensitive values (passwords, tokens, etc.) are automatically masked.
        """
        histories = self.get_all_histories()
        if not histories:
            cli_console.warning("No resolution history available")
            return

        with cli_console.phase(
            f"Configuration Resolution History ({len(histories)} keys)"
        ):
            for key in sorted(histories.keys()):
                history = histories[key]
                cli_console.message(
                    f"\n{key} resolution chain ({len(history.entries)} sources):"
                )
                with cli_console.indented():
                    for i, entry in enumerate(history.entries, 1):
                        cv = entry.config_value
                        status_icon = "✅" if entry.was_used else "❌"

                        if entry.was_used:
                            status_text = "(SELECTED)"
                        elif entry.overridden_by:
                            status_text = f"(overridden by {entry.overridden_by})"
                        else:
                            status_text = "(not used)"

                        # Mask sensitive values
                        masked_value = _mask_sensitive_value(cv.key, cv.value)
                        masked_raw = (
                            _mask_sensitive_value(cv.key, cv.raw_value)
                            if cv.raw_value is not None
                            else None
                        )

                        # Show raw value if different from parsed value
                        value_display = f'"{masked_value}"'
                        if masked_raw is not None and cv.raw_value != cv.value:
                            value_display = f'"{masked_raw}" → {masked_value}'

                        cli_console.step(
                            f"{i}. {status_icon} {cv.source_name}: {value_display} {status_text}"
                        )

                    if history.default_used:
                        masked_default = _mask_sensitive_value(key, history.final_value)
                        cli_console.step(f"Default value used: {masked_default}")

    def export_history(self, filepath: Path) -> None:
        """
        Export resolution history to JSON file.

        Args:
            filepath: Path to output file
        """
        histories = self.get_all_histories()
        data = {
            "summary": self.get_history_summary(),
            "histories": {key: history.to_dict() for key, history in histories.items()},
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        log.info("Resolution history exported to %s", filepath)
