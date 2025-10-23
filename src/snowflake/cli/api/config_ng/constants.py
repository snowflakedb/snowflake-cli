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

"""Constants for configuration system."""

from enum import Enum
from typing import Final, Literal


class ConfigSection(str, Enum):
    """Configuration section names."""

    CONNECTIONS = "connections"
    VARIABLES = "variables"
    CLI = "cli"
    CLI_LOGS = "cli.logs"
    CLI_FEATURES = "cli.features"

    def __str__(self) -> str:
        """Return the string value for backward compatibility."""
        return self.value


# Environment variable names
SNOWFLAKE_HOME_ENV: Final[str] = "SNOWFLAKE_HOME"

# Internal CLI parameters that should not be treated as connection parameters
INTERNAL_CLI_PARAMETERS: Final[set[str]] = {
    "enable_diag",
    "temporary_connection",
    "default_connection_name",
    "connection_name",
    "diag_log_path",
    "diag_allowlist_path",
    "mfa_passcode",
}

# Define Literal type for file source names
FileSourceName = Literal[
    "snowsql_config",
    "cli_config_toml",
    "connections_toml",
]

# Source names that represent file-based configuration sources
FILE_SOURCE_NAMES: Final[set[str]] = {
    "snowsql_config",
    "cli_config_toml",
    "connections_toml",
}
