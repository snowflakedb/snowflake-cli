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

"""Pure functions for configuration merging operations."""

from typing import Any, Dict

from snowflake.cli.api.config_ng.constants import (
    INTERNAL_CLI_PARAMETERS,
    ConfigSection,
)


def extract_root_level_connection_params(
    config: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Extract root-level connection parameters from config.

    Connection parameters at root level (not under any section) should
    be treated as general connection parameters that apply to all connections.

    Args:
        config: Configuration dictionary with mixed sections and parameters

    Returns:
        Tuple of (connection_params, remaining_config)

    Example:
        Input:  {"account": "acc", "cli": {...}, "connections": {...}}
        Output: ({"account": "acc"}, {"cli": {...}, "connections": {...}})
    """
    known_sections = {s.value for s in ConfigSection}

    connection_params = {}
    remaining = {}

    for key, value in config.items():
        # Check if this key is a known section or internal parameter
        is_section = key in known_sections or any(
            key.startswith(s + ".") for s in known_sections
        )
        is_internal = key in INTERNAL_CLI_PARAMETERS

        if not is_section and not is_internal:
            # Root-level parameter that's not a section = connection parameter
            connection_params[key] = value
        else:
            remaining[key] = value

    return connection_params, remaining


def merge_params_into_connections(
    connections: Dict[str, Dict[str, Any]], params: Dict[str, Any]
) -> Dict[str, Dict[str, Any]]:
    """
    Merge parameters into all existing connections.

    Used for overlay sources where root-level connection params apply to all connections.
    The params overlay (override) values in each connection.

    Args:
        connections: Dictionary of connection configurations
        params: Parameters to merge into each connection

    Returns:
        Dictionary of connections with params merged in

    Example:
        Input:
            connections = {"dev": {"account": "dev_acc", "user": "dev_user"}}
            params = {"user": "override_user", "password": "new_pass"}
        Output:
            {"dev": {"account": "dev_acc", "user": "override_user", "password": "new_pass"}}
    """
    from snowflake.cli.api.config_ng.dict_utils import deep_merge

    result = {}
    for conn_name, conn_config in connections.items():
        if isinstance(conn_config, dict):
            result[conn_name] = deep_merge(conn_config, params)
        else:
            result[conn_name] = conn_config

    return result


def create_default_connection_from_params(
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Create a default connection from connection parameters.

    Args:
        params: Connection parameters

    Returns:
        Dictionary with "default" connection containing the params

    Example:
        Input: {"account": "acc", "user": "usr"}
        Output: {"default": {"account": "acc", "user": "usr"}}
    """
    if not params:
        return {}
    return {"default": params.copy()}
