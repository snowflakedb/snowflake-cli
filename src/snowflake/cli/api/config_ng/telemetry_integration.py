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
Telemetry integration for config_ng system.

This module provides functions to track configuration source usage
and integrate with the CLI's telemetry system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.resolver import ConfigurationResolver


# Map source names to counter field names
SOURCE_TO_COUNTER = {
    "snowsql_config": "config_source_snowsql",
    "cli_config_toml": "config_source_cli_toml",
    "connections_toml": "config_source_connections_toml",
    "snowsql_env": "config_source_snowsql_env",
    "connection_specific_env": "config_source_connection_env",
    "cli_env": "config_source_cli_env",
    "cli_arguments": "config_source_cli_args",
}


def record_config_source_usage(resolver: ConfigurationResolver) -> None:
    """
    Record configuration source usage to CLI metrics.

    This should be called after configuration resolution completes.
    Sets counters to 1 for sources that provided winning values, 0 otherwise.

    Args:
        resolver: The ConfigurationResolver instance
    """
    try:
        from snowflake.cli.api.cli_global_context import get_cli_context
        from snowflake.cli.api.metrics import CLICounterField

        cli_context = get_cli_context()
        summary = resolver.get_resolution_summary()

        source_wins = summary.get("source_wins", {})

        for source_name, counter_name in SOURCE_TO_COUNTER.items():
            value = 1 if source_wins.get(source_name, 0) > 0 else 0
            counter_field = getattr(CLICounterField, counter_name.upper(), None)
            if counter_field:
                cli_context.metrics.set_counter(counter_field, value)

    except Exception:
        pass


def get_config_telemetry_payload(
    resolver: Optional[ConfigurationResolver],
) -> Dict[str, Any]:
    """
    Get configuration telemetry payload for inclusion in command telemetry.

    Args:
        resolver: Optional ConfigurationResolver instance

    Returns:
        Dictionary with config telemetry data
    """
    if resolver is None:
        return {}

    try:
        if hasattr(resolver, "get_resolution_summary"):
            summary = resolver.get_resolution_summary()
        else:
            tracker = resolver.get_tracker()
            summary = tracker.get_summary()

        return {
            "config_sources_used": list(summary.get("source_usage", {}).keys()),
            "config_source_wins": summary.get("source_wins", {}),
            "config_total_keys_resolved": summary.get("total_keys_resolved", 0),
            "config_keys_with_overrides": summary.get("keys_with_overrides", 0),
        }
    except Exception:
        return {}
