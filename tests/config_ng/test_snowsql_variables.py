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
Tests for SnowSQL [variables] section reading and merging with -D parameters.
"""

import tempfile
from pathlib import Path
from unittest import mock

from snowflake.cli.api.config_ng import (
    ConfigurationResolver,
    SnowSQLConfigFile,
    SnowSQLSection,
    get_merged_variables,
)


class TestSnowSQLVariablesSection:
    """Tests for reading [variables] section from SnowSQL config files."""

    def test_read_variables_section_from_snowsql_config(self):
        """Test that [variables] section is correctly read from SnowSQL config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[connections]
accountname = test_account
username = test_user

[variables]
var1=value1
var2=value2
example_variable=27
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            discovered = source.discover()

            # Check nested structure
            assert "variables" in discovered
            assert "var1" in discovered["variables"]
            assert "var2" in discovered["variables"]
            assert "example_variable" in discovered["variables"]

            # Values are plain strings now (not ConfigValue objects)
            assert discovered["variables"]["var1"] == "value1"
            assert discovered["variables"]["var2"] == "value2"
            assert discovered["variables"]["example_variable"] == "27"

    def test_variables_section_empty(self):
        """Test that empty [variables] section doesn't cause errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[connections]
accountname = test_account

[variables]
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            discovered = source.discover()

            # Should have connections but no variables (or empty variables dict)
            assert "connections" in discovered
            assert not discovered.get("variables", {})

    def test_no_variables_section(self):
        """Test that config without [variables] section works correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[connections]
accountname = test_account
username = test_user
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            discovered = source.discover()

            # Should have connections but no variables key
            assert "connections" in discovered
            assert "variables" not in discovered

    def test_variables_merged_from_multiple_files(self):
        """Test that variables from multiple SnowSQL config files are merged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file1 = Path(temp_dir) / "config1"
            config_file1.write_text(
                """
[variables]
var1=value1
var2=original_value2
"""
            )

            config_file2 = Path(temp_dir) / "config2"
            config_file2.write_text(
                """
[variables]
var2=overridden_value2
var3=value3
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file1, config_file2])

            discovered = source.discover()

            # Check nested structure with merged values
            assert discovered["variables"]["var1"] == "value1"
            assert discovered["variables"]["var2"] == "overridden_value2"
            assert discovered["variables"]["var3"] == "value3"

    def test_variables_with_special_characters(self):
        """Test that variables with special characters in values are handled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[variables]
var_with_equals=key=value
var_with_spaces=value with spaces
var_with_quotes='quoted value'
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            discovered = source.discover()

            assert discovered["variables"]["var_with_equals"] == "key=value"
            assert discovered["variables"]["var_with_spaces"] == "value with spaces"
            assert discovered["variables"]["var_with_quotes"] == "'quoted value'"


class TestAlternativeConfigProviderVariables:
    """Tests for getting variables section from AlternativeConfigProvider."""

    def test_get_variables_section(self):
        """Test get_section('variables') returns nested dict."""
        from snowflake.cli.api.config_provider import AlternativeConfigProvider

        provider = AlternativeConfigProvider()

        # Mock with nested structure
        mock_cache = {
            "variables": {"var1": "value1", "var2": "value2"},
            "connections": {"default": {"account": "test_account"}},
        }

        setattr(provider, "_initialized", True)
        setattr(provider, "_config_cache", mock_cache)

        from snowflake.cli.api.cli_global_context import get_cli_context

        try:
            setattr(
                provider,
                "_last_config_override",
                get_cli_context().config_file_override,
            )
        except Exception:
            setattr(provider, "_last_config_override", None)

        result = provider.get_section("variables")

        # Should return nested dict under "variables" key
        assert result == {"var1": "value1", "var2": "value2"}

    def test_get_variables_section_empty(self):
        """Test get_section('variables') with no variables returns empty dict."""
        from snowflake.cli.api.config_provider import AlternativeConfigProvider

        provider = AlternativeConfigProvider()

        mock_cache = {
            "connections": {"default": {"account": "test_account"}},
        }

        setattr(provider, "_initialized", True)
        setattr(provider, "_config_cache", mock_cache)

        from snowflake.cli.api.cli_global_context import get_cli_context

        try:
            setattr(
                provider,
                "_last_config_override",
                get_cli_context().config_file_override,
            )
        except Exception:
            setattr(provider, "_last_config_override", None)

        result = provider.get_section("variables")

        assert result == {}


class TestGetMergedVariables:
    """Tests for get_merged_variables() utility function."""

    def test_get_merged_variables_no_cli_params(self):
        """Test get_merged_variables with only SnowSQL variables."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.return_value = {
                "var1": "snowsql_value1",
                "var2": "snowsql_value2",
            }
            mock_provider.return_value = mock_instance

            result = get_merged_variables(None)

            assert result == {"var1": "snowsql_value1", "var2": "snowsql_value2"}
            mock_instance.get_section.assert_called_once_with("variables")

    def test_get_merged_variables_with_cli_params(self):
        """Test get_merged_variables with both SnowSQL and CLI -D parameters."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.return_value = {
                "var1": "snowsql_value1",
                "var2": "snowsql_value2",
            }
            mock_provider.return_value = mock_instance

            cli_vars = ["var2=cli_value2", "var3=cli_value3"]
            result = get_merged_variables(cli_vars)

            # var1 from SnowSQL
            assert result["var1"] == "snowsql_value1"
            # var2 should be overridden by CLI
            assert result["var2"] == "cli_value2"
            # var3 from CLI
            assert result["var3"] == "cli_value3"

    def test_get_merged_variables_cli_only(self):
        """Test get_merged_variables with only CLI -D parameters."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.return_value = {}
            mock_provider.return_value = mock_instance

            cli_vars = ["var1=cli_value1", "var2=cli_value2"]
            result = get_merged_variables(cli_vars)

            assert result == {"var1": "cli_value1", "var2": "cli_value2"}

    def test_get_merged_variables_precedence(self):
        """Test that CLI -D parameters have higher precedence than SnowSQL variables."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.return_value = {
                "database": "snowsql_db",
                "schema": "snowsql_schema",
                "custom_var": "snowsql_value",
            }
            mock_provider.return_value = mock_instance

            cli_vars = ["database=cli_db", "custom_var=cli_value"]
            result = get_merged_variables(cli_vars)

            # CLI should override SnowSQL
            assert result["database"] == "cli_db"
            assert result["custom_var"] == "cli_value"
            # SnowSQL value should remain for non-overridden keys
            assert result["schema"] == "snowsql_schema"

    def test_get_merged_variables_provider_error(self):
        """Test get_merged_variables handles provider errors gracefully."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.side_effect = Exception("Provider error")
            mock_provider.return_value = mock_instance

            cli_vars = ["var1=cli_value1"]
            result = get_merged_variables(cli_vars)

            # Should fall back to only CLI variables
            assert result == {"var1": "cli_value1"}

    def test_get_merged_variables_empty(self):
        """Test get_merged_variables with no variables at all."""
        with mock.patch(
            "snowflake.cli.api.config_provider.get_config_provider_singleton"
        ) as mock_provider:
            mock_instance = mock.Mock()
            mock_instance.get_section.return_value = {}
            mock_provider.return_value = mock_instance

            result = get_merged_variables(None)

            assert result == {}


class TestConfigurationResolverVariables:
    """Integration tests for variables in ConfigurationResolver."""

    def test_resolver_with_variables(self):
        """Test that resolver correctly processes variables from SnowSQL config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[connections]
accountname = test_account

[variables]
var1=value1
var2=value2
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            resolver = ConfigurationResolver(sources=[source])
            config = resolver.resolve()

            # Check nested structure
            assert "variables" in config
            assert "var1" in config["variables"]
            assert "var2" in config["variables"]
            assert config["variables"]["var1"] == "value1"
            assert config["variables"]["var2"] == "value2"


class TestSnowSQLSectionEnum:
    """Tests for SnowSQLSection enum."""

    def test_section_enum_values(self):
        """Test that SnowSQLSection enum has correct values."""
        assert SnowSQLSection.CONNECTIONS.value == "connections"
        assert SnowSQLSection.VARIABLES.value == "variables"
        assert SnowSQLSection.OPTIONS.value == "options"

    def test_section_enum_in_snowsql_source(self):
        """Test that SnowSQLConfigFile uses the enum."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config"
            config_file.write_text(
                """
[connections]
accountname = test_account

[variables]
var1=value1
"""
            )

            source = SnowSQLConfigFile(config_paths=[config_file])

            # Should discover both connections and variables
            discovered = source.discover()

            # Check nested structure contains both sections
            assert "connections" in discovered
            assert "variables" in discovered
