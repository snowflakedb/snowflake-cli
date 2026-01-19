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

"""Factory for creating configuration sources."""

from typing import Any, Dict, List, Optional

from snowflake.cli.api.config_ng.core import ValueSource


def create_default_sources(
    cli_context: Optional[Dict[str, Any]] = None,
) -> List[ValueSource]:
    """
    Create default source list in precedence order.

    Creates the standard 7-source configuration stack from lowest
    to highest priority:
    1. SnowSQL config files (merged)
    2. CLI config.toml (first-found)
    3. Dedicated connections.toml
    4. SnowSQL environment variables (SNOWSQL_*)
    5. General CLI environment variables (SNOWFLAKE_*)
    6. Connection-specific environment variables (SNOWFLAKE_CONNECTIONS_*)
    7. CLI command-line arguments (highest priority)

    Args:
        cli_context: Optional CLI context dictionary for CliParameters source

    Returns:
        List of ValueSource instances in precedence order
    """
    from snowflake.cli.api.config_ng import (
        CliConfigFile,
        CliEnvironment,
        CliParameters,
        ConnectionsConfigFile,
        ConnectionSpecificEnvironment,
        SnowSQLConfigFile,
        SnowSQLEnvironment,
    )

    return [
        SnowSQLConfigFile(),
        CliConfigFile(),
        ConnectionsConfigFile(),
        SnowSQLEnvironment(),
        CliEnvironment(),
        ConnectionSpecificEnvironment(),
        CliParameters(cli_context=cli_context or {}),
    ]
