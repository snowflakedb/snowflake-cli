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

"""Tests for configuration constants."""

from snowflake.cli.api.config_ng.constants import (
    FILE_SOURCE_NAMES,
    INTERNAL_CLI_PARAMETERS,
    SNOWFLAKE_HOME_ENV,
    ConfigSection,
    ConfigSourceName,
)


class TestConfigSection:
    """Test ConfigSection enum."""

    def test_enum_values(self):
        """Test that enum has expected values."""
        assert ConfigSection.CONNECTIONS.value == "connections"
        assert ConfigSection.VARIABLES.value == "variables"
        assert ConfigSection.CLI.value == "cli"
        assert ConfigSection.CLI_LOGS.value == "cli.logs"
        assert ConfigSection.CLI_FEATURES.value == "cli.features"

    def test_enum_string_representation(self):
        """Test that enum converts to string correctly."""
        assert str(ConfigSection.CONNECTIONS) == "connections"
        assert str(ConfigSection.VARIABLES) == "variables"
        assert str(ConfigSection.CLI) == "cli"

    def test_enum_is_string(self):
        """Test that enum instances are strings."""
        assert isinstance(ConfigSection.CONNECTIONS, str)
        assert isinstance(ConfigSection.VARIABLES, str)

    def test_enum_comparison(self):
        """Test that enum can be compared with strings."""
        assert ConfigSection.CONNECTIONS == "connections"
        assert ConfigSection.VARIABLES == "variables"


class TestConstants:
    """Test other constants."""

    def test_snowflake_home_env(self):
        """Test SNOWFLAKE_HOME environment variable constant."""
        assert SNOWFLAKE_HOME_ENV == "SNOWFLAKE_HOME"

    def test_internal_cli_parameters(self):
        """Test INTERNAL_CLI_PARAMETERS set."""
        expected_params = {
            "enable_diag",
            "temporary_connection",
            "default_connection_name",
            "connection_name",
            "diag_log_path",
            "diag_allowlist_path",
            "mfa_passcode",
        }
        assert INTERNAL_CLI_PARAMETERS == expected_params

    def test_file_source_names(self):
        """Test FILE_SOURCE_NAMES set."""
        expected_sources = {
            ConfigSourceName.SNOWSQL_CONFIG,
            ConfigSourceName.CLI_CONFIG_TOML,
            ConfigSourceName.CONNECTIONS_TOML,
        }
        assert FILE_SOURCE_NAMES == expected_sources
