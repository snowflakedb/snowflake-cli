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
Tests for merged configuration from multiple sources.

These tests verify that configuration values are properly merged from:
- SnowSQL config files (.snowsql/config)
- SnowSQL environment variables (SNOWSQL_*)
- CLI config files (.snowflake/config.toml)
- CLI environment variables (SNOWFLAKE_*)
- CLI command-line parameters
- Connections TOML files (.snowflake/connections.toml)
"""

from textwrap import dedent

from .conftest import (
    CliConfig,
    CliEnvs,
    CliParams,
    ConnectionsToml,
    FinalConfig,
    SnowSQLConfig,
    SnowSQLEnvs,
    config_sources,
)


class TestConfigurationMerging:
    """Test configuration merging from multiple sources."""

    def test_all_sources_merged(self):
        """
        Test that all configuration sources are properly merged.

        Priority order (highest to lowest):
        1. CLI parameters
        2. CLI environment variables
        3. SnowSQL environment variables
        4. CLI config.toml
        5. Connections.toml
        6. SnowSQL config
        """
        sources = (
            SnowSQLConfig("config"),
            SnowSQLEnvs("snowsql.env"),
            CliConfig("config.toml"),
            CliEnvs("cli.env"),
            CliParams("--account", "cli-account", "--user", "cli-user"),
            ConnectionsToml("connections.toml"),
        )

        expected = FinalConfig(
            config_dict={
                "account": "cli-account",
                "user": "cli-user",
                "password": "abc",
            }
        )

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            # CLI params have highest priority
            assert merged["account"] == "cli-account"
            assert merged["user"] == "cli-user"

            # Password comes from config files
            assert merged.get("password") == "abc"

    def test_cli_envs_override_snowsql_envs(self):
        """Test that CLI environment variables override SnowSQL environment variables."""
        sources = (
            SnowSQLEnvs("snowsql.env"),
            CliEnvs("cli.env"),
        )

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            # CLI env (SNOWFLAKE_USER=Alice) overrides
            # SnowSQL env (SNOWSQL_USER=Bob)
            assert merged["user"] == "Alice"

    def test_cli_params_override_all(self):
        """Test that CLI parameters override all other sources."""
        sources = (
            SnowSQLConfig("config"),
            CliConfig("config.toml"),
            CliParams("--account", "override-account"),
        )

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            # CLI params override everything
            assert merged["account"] == "override-account"

    def test_config_files_precedence(self):
        """Test precedence among configuration files."""
        sources = (
            SnowSQLConfig("config"),
            CliConfig("config.toml"),
        )

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            # CLI config.toml has higher priority than SnowSQL config
            # Both have account-a, but config.toml should win
            assert merged["account"] == "account-a"
            assert merged["username"] == "user"

    def test_connections_toml_separate_connection(self):
        """Test that connections.toml can have separate connections."""
        sources = (ConnectionsToml("connections.toml"),)

        # Test connection 'b' which only exists in connections.toml
        with config_sources(sources, connection="b") as ctx:
            merged = ctx.get_merged_config()

            assert merged["account"] == "account-a"
            assert merged["username"] == "user"
            assert merged["password"] == "abc"

    def test_empty_sources(self):
        """Test that empty sources return minimal configuration."""
        sources = ()

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            # May contain default keys like 'home', but no connection-specific keys
            assert "account" not in merged
            assert "user" not in merged
            assert "password" not in merged

    def test_only_cli_params(self):
        """Test configuration with only CLI parameters."""
        sources = (CliParams("--account", "test-account", "--user", "test-user"),)

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()

            assert merged["account"] == "test-account"
            assert merged["user"] == "test-user"

    def test_final_config_from_dict(self):
        """Test FinalConfig creation from dictionary."""
        expected = FinalConfig(config_dict={"account": "test", "user": "alice"})

        assert expected.config_dict == {"account": "test", "user": "alice"}
        assert expected == {"account": "test", "user": "alice"}

    def test_final_config_from_toml_string(self):
        """Test FinalConfig creation from TOML string for readability."""
        toml_string = dedent(
            """
            [connections.prod]
            account = "prod-account"
            user = "prod-user"
            password = "secret"
            """
        )

        expected = FinalConfig(toml_string=toml_string)

        assert "connections" in expected.config_dict
        assert expected.config_dict["connections"]["prod"]["account"] == "prod-account"

    def test_final_config_equality(self):
        """Test FinalConfig equality comparison."""
        config1 = FinalConfig(config_dict={"account": "test", "user": "alice"})
        config2 = FinalConfig(config_dict={"account": "test", "user": "alice"})
        config3 = FinalConfig(config_dict={"account": "test", "user": "bob"})

        assert config1 == config2
        assert config1 != config3
        assert config1 == {"account": "test", "user": "alice"}


class TestConfigurationResolution:
    """Test configuration resolution details."""

    def test_resolution_history_tracking(self):
        """Test that resolution history is tracked correctly."""
        sources = (
            SnowSQLConfig("config"),
            CliConfig("config.toml"),
            CliParams("--account", "cli-account"),
        )

        with config_sources(sources) as ctx:
            resolver = ctx.get_resolver()
            config = resolver.resolve()

            # Check that account was overridden (flat key from CLI)
            assert config["account"] == "cli-account"

            # Also check that connection-specific key exists (from file sources)
            assert config.get("connections.a.account") == "account-a"

            # Check resolution history for flat key (from CLI params)
            cli_history = resolver.get_resolution_history("account")
            assert cli_history is not None
            assert (
                len(cli_history.entries) == 1
            )  # Only CLI param provides flat "account"
            assert cli_history.selected_entry
            assert (
                cli_history.selected_entry.config_value.source_name == "cli_arguments"
            )

            # Check resolution history for prefixed key (from file sources)
            file_history = resolver.get_resolution_history("connections.a.account")
            assert file_history is not None
            assert len(file_history.entries) >= 1  # Config files provide prefixed key

    def test_resolution_summary(self):
        """Test that resolution summary provides useful statistics."""
        sources = (
            SnowSQLConfig("config"),
            CliConfig("config.toml"),
            CliParams("--account", "cli-account"),
        )

        with config_sources(sources) as ctx:
            resolver = ctx.get_resolver()
            resolver.resolve()

            summary = resolver.get_history_summary()

            assert summary["total_keys_resolved"] > 0
            assert "source_usage" in summary
            assert "source_wins" in summary

            # CLI should have won for account
            assert summary["source_wins"].get("cli_arguments", 0) >= 1
