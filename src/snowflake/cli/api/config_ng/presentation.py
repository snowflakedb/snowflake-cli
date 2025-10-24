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
Resolution presentation utilities.

This module handles all formatting, display, and export of configuration
resolution data. It separates presentation concerns from resolution logic.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Tuple

from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import CollectionResult, MessageResult

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.resolver import ConfigurationResolver

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

# Fixed table columns ordered from most important (left) to least (right)
SourceColumn = Literal[
    "params",
    "global_envs",
    "connections_env",
    "snowsql_env",
    "connections.toml",
    "config.toml",
    "snowsql",
]

TABLE_COLUMNS: Tuple[str, ...] = (
    "key",
    "value",
    "params",
    "global_envs",
    "connections_env",
    "snowsql_env",
    "connections.toml",
    "config.toml",
    "snowsql",
)

# Mapping of internal source names to fixed table columns
SOURCE_TO_COLUMN: Dict[str, SourceColumn] = {
    "cli_arguments": "params",
    "cli_env": "global_envs",
    "connection_specific_env": "connections_env",
    "snowsql_env": "snowsql_env",
    "connections_toml": "connections.toml",
    "cli_config_toml": "config.toml",
    "snowsql_config": "snowsql",
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

    if any(path_key in key_lower for path_key in PATH_KEYS):
        return False

    return any(sensitive_key in key_lower for sensitive_key in SENSITIVE_KEYS)


def _mask_sensitive_value(key: str, value: Any) -> str:
    """
    Mask sensitive configuration values for display.

    Args:
        key: Configuration key name
        value: Value to potentially mask

    Returns:
        Masked string if sensitive, otherwise string representation of value
    """
    if _should_mask_value(key):
        return "****"
    return str(value)


class ResolutionPresenter:
    """
    Handles all presentation, formatting, and export of resolution data.

    This class is responsible for:
    - Console output with colors and formatting
    - Building CommandResult objects for the output system
    - Exporting resolution data to files
    - Masking sensitive values in all outputs
    """

    def __init__(self, resolver: ConfigurationResolver):
        """
        Initialize presenter with a resolver.

        Args:
            resolver: ConfigurationResolver instance to present data from
        """
        self._resolver = resolver

    def get_summary(self) -> dict:
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
        return self._resolver.get_tracker().get_summary()

    def build_sources_table(self, key: Optional[str] = None) -> CollectionResult:
        """
        Build a tabular view of configuration sources per key.

        Columns (left to right): key, value, params, env, connections.toml, cli_config.toml, snowsql.
        - value: masked final selected value for the key
        - presence columns: "+" if a given source provided a value for the key, empty otherwise

        Args:
            key: Optional specific key to build table for, or None for all keys
        """
        tracker = self._resolver.get_tracker()
        if key is None and not tracker.get_all_histories():
            self._resolver.resolve()
        elif key is not None and tracker.get_history(key) is None:
            self._resolver.resolve(key=key)

        histories = (
            {key: tracker.get_history(key)}
            if key is not None
            else tracker.get_all_histories()
        )

        def _row_items():
            for k, history in histories.items():
                if history is None:
                    continue
                row: Dict[str, Any] = {c: "" for c in TABLE_COLUMNS}
                row["key"] = k

                masked_final = _mask_sensitive_value(k, history.final_value)
                row["value"] = masked_final

                for entry in history.entries:
                    source_column = SOURCE_TO_COLUMN.get(entry.config_value.source_name)
                    if source_column is not None:
                        row[source_column] = "+"

                ordered_row = {column: row[column] for column in TABLE_COLUMNS}
                yield ordered_row

        return CollectionResult(_row_items())

    def format_history_message(self, key: Optional[str] = None) -> MessageResult:
        """
        Build a masked, human-readable history of merging as a single message.
        If key is None, returns concatenated histories for all keys.

        Args:
            key: Optional specific key to format, or None for all keys
        """
        histories = (
            {key: self._resolver.get_resolution_history(key)}
            if key is not None
            else self._resolver.get_all_histories()
        )

        if not histories:
            return MessageResult("No resolution history available")

        lines = []
        lines.append("Configuration Resolution History")
        lines.append("=" * 80)
        lines.append("")

        for k in sorted(histories.keys()):
            history = histories[k]
            if history is None:
                continue

            lines.append(f"Key: {k}")
            lines.append(
                f"Final Value: {_mask_sensitive_value(k, history.final_value)}"
            )

            if history.entries:
                lines.append("Resolution Chain:")
                for i, entry in enumerate(history.entries, 1):
                    cv = entry.config_value
                    status = "SELECTED" if entry.was_used else "overridden"
                    masked_value = _mask_sensitive_value(cv.key, cv.value)
                    lines.append(f"  {i}. [{status}] {cv.source_name}: {masked_value}")

            if history.default_used:
                lines.append("  (default value used)")

            lines.append("")

        return MessageResult("\n".join(lines))

    def print_resolution_chain(self, key: str) -> None:
        """
        Print the resolution chain for a key using cli_console formatting.
        Sensitive values (passwords, tokens, etc.) are automatically masked.

        Args:
            key: Configuration key
        """
        history = self._resolver.get_resolution_history(key)
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
        histories = self._resolver.get_all_histories()
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
        histories = self._resolver.get_all_histories()
        data = {
            "summary": self.get_summary(),
            "histories": {key: history.to_dict() for key, history in histories.items()},
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
