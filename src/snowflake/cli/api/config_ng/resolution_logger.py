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
Configuration resolution logging utilities.

This module provides internal utilities for logging and displaying configuration
resolution information. It's designed to be used independently of CLI commands,
allowing it to be used in any context where configuration debugging is needed.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    AlternativeConfigProvider,
    get_config_provider_singleton,
)
from snowflake.cli.api.console import cli_console

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.resolver import ConfigurationResolver


def is_resolution_logging_available() -> bool:
    """
    Check if configuration resolution logging is available.

    Returns:
        True if the alternative config provider is enabled and has resolution history
    """
    provider = get_config_provider_singleton()
    return isinstance(provider, AlternativeConfigProvider)


def get_resolver() -> Optional[ConfigurationResolver]:
    """
    Get the ConfigurationResolver from the current provider.

    Returns:
        ConfigurationResolver instance if available, None otherwise
    """
    provider = get_config_provider_singleton()
    if not isinstance(provider, AlternativeConfigProvider):
        return None

    # Ensure provider is initialized
    provider._ensure_initialized()  # noqa: SLF001
    return provider._resolver  # noqa: SLF001


def show_resolution_chain(key: str) -> None:
    """
    Display the resolution chain for a specific configuration key.

    This shows:
    - All sources that provided values for the key
    - The order in which values were considered
    - Which value overrode which
    - The final selected value

    Args:
        key: Configuration key to show resolution for
    """
    resolver = get_resolver()

    if resolver is None:
        cli_console.warning(
            "Configuration resolution logging is not available. "
            f"Set {ALTERNATIVE_CONFIG_ENV_VAR}=true to enable it."
        )
        return

    resolver.print_resolution_chain(key)


def show_all_resolution_chains() -> None:
    """
    Display resolution chains for all configured keys.

    This provides a complete overview of the configuration resolution process,
    showing how every configuration value was determined.
    """
    resolver = get_resolver()

    if resolver is None:
        cli_console.warning(
            "Configuration resolution logging is not available. "
            f"Set {ALTERNATIVE_CONFIG_ENV_VAR}=true to enable it."
        )
        return

    resolver.print_all_chains()


def get_resolution_summary() -> Optional[Dict]:
    """
    Get summary statistics about configuration resolution.

    Returns:
        Dictionary with statistics including:
        - total_keys_resolved: Number of keys resolved
        - keys_with_overrides: Number of keys where values were overridden
        - keys_using_defaults: Number of keys using default values
        - source_usage: Dict of source_name -> count of values provided
        - source_wins: Dict of source_name -> count of values selected

        None if resolution logging is not available
    """
    resolver = get_resolver()

    if resolver is None:
        return None

    return resolver.get_history_summary()


def export_resolution_history(output_path: Path) -> bool:
    """
    Export complete resolution history to a JSON file.

    This creates a detailed JSON report that can be:
    - Attached to support tickets
    - Used for configuration debugging
    - Analyzed programmatically

    Args:
        output_path: Path where the JSON file should be saved

    Returns:
        True if export succeeded, False otherwise
    """
    resolver = get_resolver()

    if resolver is None:
        cli_console.warning(
            "Configuration resolution logging is not available. "
            f"Set {ALTERNATIVE_CONFIG_ENV_VAR}=true to enable it."
        )
        return False

    try:
        resolver.export_history(output_path)
        cli_console.message(f"✅ Resolution history exported to: {output_path}")
        return True
    except Exception as e:
        cli_console.warning(f"❌ Failed to export resolution history: {e}")
        return False


def format_summary_for_display() -> Optional[str]:
    """
    Format resolution summary as a human-readable string.

    Returns:
        Formatted summary string, or None if resolution logging not available
    """
    summary = get_resolution_summary()

    if summary is None:
        return None

    lines = [
        "\n" + "=" * 80,
        "Configuration Resolution Summary",
        "=" * 80,
        f"Total keys resolved: {summary['total_keys_resolved']}",
        f"Keys with overrides: {summary['keys_with_overrides']}",
        f"Keys using defaults: {summary['keys_using_defaults']}",
        "",
        "Source Usage:",
    ]

    # Sort sources by number of values provided (descending)
    source_usage = summary["source_usage"]
    source_wins = summary["source_wins"]

    for source_name in sorted(source_usage, key=source_usage.get, reverse=True):
        provided = source_usage[source_name]
        wins = source_wins.get(source_name, 0)
        lines.append(
            f"  {source_name:30s} provided: {provided:3d}  selected: {wins:3d}"
        )

    lines.append("=" * 80 + "\n")
    return "\n".join(lines)


def check_value_source(key: str) -> Optional[str]:
    """
    Check which source provided the value for a specific configuration key.

    Args:
        key: Configuration key to check

    Returns:
        Name of the source that provided the final value, or None if not found
    """
    resolver = get_resolver()

    if resolver is None:
        return None

    history = resolver.get_resolution_history(key)
    if history and history.selected_entry:
        return history.selected_entry.config_value.source_name

    return None


def explain_configuration(key: Optional[str] = None, verbose: bool = False) -> None:
    """
    Explain configuration resolution for a key or all keys.

    This is a high-level function that combines multiple resolution
    logging capabilities to provide comprehensive configuration explanation.

    Args:
        key: Specific key to explain, or None to explain all
        verbose: If True, show detailed resolution chains
    """
    resolver = get_resolver()

    if resolver is None:
        cli_console.warning(
            "Configuration resolution logging is not available. "
            f"Set {ALTERNATIVE_CONFIG_ENV_VAR}=true to enable the new config system."
        )
        return

    if key:
        # Explain specific key
        with cli_console.phase(f"Configuration Resolution: {key}"):
            source = check_value_source(key)
            if source:
                cli_console.message(f"Current value from: {source}")
            else:
                cli_console.message("No value found for this key")

            if verbose:
                resolver.print_resolution_chain(key)
    else:
        # Explain all configuration
        with cli_console.phase("Complete Configuration Resolution"):
            summary_text = format_summary_for_display()
            if summary_text:
                cli_console.message(summary_text)

            if verbose:
                resolver.print_all_chains()
