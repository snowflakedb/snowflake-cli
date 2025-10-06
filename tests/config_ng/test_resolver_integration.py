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
End-to-end integration tests for ConfigurationResolver.

Tests verify:
- Complete resolution workflow with all sources
- Real-world migration scenarios
- Complete precedence chains
- History tracking in production scenarios
"""

from snowflake.cli.api.config_ng.env_handlers import (
    SnowCliEnvHandler,
    SnowSqlEnvHandler,
)
from snowflake.cli.api.config_ng.file_handlers import (
    SnowSqlConfigHandler,
    TomlFileHandler,
)
from snowflake.cli.api.config_ng.resolver import ConfigurationResolver
from snowflake.cli.api.config_ng.sources import (
    CliArgumentSource,
    EnvironmentSource,
    FileSource,
)


class TestResolverEndToEnd:
    """End-to-end integration tests for complete resolution workflow."""

    def test_production_configuration_setup(self, tmp_path, monkeypatch):
        """Test production-like configuration setup."""
        # Create SnowCLI TOML config
        snowcli_config = tmp_path / "connections.toml"
        snowcli_config.write_text(
            "[default]\n"
            'account = "toml_account"\n'
            'user = "toml_user"\n'
            'database = "toml_db"\n'
        )

        # Set environment variables
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "env_warehouse")

        # CLI arguments
        cli_context = {"account": "cli_account"}

        # Create sources
        cli_source = CliArgumentSource(cli_context=cli_context)
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[snowcli_config],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        # Create resolver
        resolver = ConfigurationResolver(
            sources=[cli_source, env_source, file_source], track_history=True
        )

        # Resolve
        config = resolver.resolve()

        # Verify precedence
        assert config["account"] == "cli_account"  # CLI wins
        assert config["warehouse"] == "env_warehouse"  # From env
        assert config["user"] == "toml_user"  # From file
        assert config["database"] == "toml_db"  # From file

        # Verify history
        account_history = resolver.get_resolution_history("account")
        assert len(account_history.entries) == 2  # TOML and CLI
        assert (
            account_history.selected_entry.config_value.source_name == "cli_arguments"
        )

    def test_snowsql_to_snowcli_migration(self, tmp_path, monkeypatch):
        """Test complete SnowSQL to SnowCLI migration scenario."""
        # SnowSQL config (legacy)
        snowsql_config = tmp_path / "snowsql.toml"
        snowsql_config.write_text(
            "[connections]\n"
            'accountname = "old_account"\n'
            'username = "old_user"\n'
            'databasename = "old_db"\n'
            'warehousename = "old_warehouse"\n'
        )

        # SnowCLI config (new, partial migration)
        snowcli_config = tmp_path / "connections.toml"
        snowcli_config.write_text(
            '[default]\naccount = "new_account"\nuser = "new_user"\n'
        )

        # Environment variables (mixed)
        monkeypatch.setenv("SNOWSQL_PWD", "env_password")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "env_warehouse")

        # Create sources
        env_source = EnvironmentSource(
            handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
        )
        file_source = FileSource(
            file_paths=[snowcli_config, snowsql_config],
            handlers=[
                TomlFileHandler(section_path=["default"]),
                SnowSqlConfigHandler(),
            ],
        )

        resolver = ConfigurationResolver(sources=[env_source, file_source])

        config = resolver.resolve()

        # New values should win
        assert config["account"] == "new_account"  # From SnowCLI TOML
        assert config["user"] == "new_user"  # From SnowCLI TOML
        assert config["warehouse"] == "env_warehouse"  # From SnowCLI env
        assert (
            config["password"] == "env_password"
        )  # From SnowSQL env (mapped from PWD)

        # Legacy values as fallback
        assert config["database"] == "old_db"  # From SnowSQL config

    def test_debugging_complete_workflow(self, tmp_path, monkeypatch):
        """Test complete debugging workflow."""
        # Setup multi-source config
        config_file = tmp_path / "config.toml"
        config_file.write_text('[default]\naccount = "file_account"\n')

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        # Resolve
        resolver.resolve()

        # Format resolution chain
        formatted = resolver.format_resolution_chain("account")

        # Verify chain shows all sources
        assert "file_account" in formatted
        assert "env_account" in formatted
        assert "cli_account" in formatted
        assert "SELECTED" in formatted

    def test_history_export_complete(self, tmp_path, monkeypatch):
        """Test complete history export for debugging."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            "[default]\n"
            'account = "file_account"\n'
            'user = "file_user"\n'
            'database = "file_db"\n'
        )

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")
        monkeypatch.setenv("SNOWFLAKE_USER", "env_user")

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        config = resolver.resolve()

        # Export history
        export_file = tmp_path / "debug.json"
        resolver.export_history(export_file)

        # Verify export contains all keys
        import json

        with open(export_file) as f:
            data = json.load(f)

        assert "account" in data["histories"]
        assert "user" in data["histories"]
        assert "database" in data["histories"]

        # Verify summary (may have more keys than expected from TOML)
        assert data["summary"]["total_keys_resolved"] >= 3
        assert data["summary"]["keys_with_overrides"] >= 2  # account and user

    def test_cli_override_everything(self, tmp_path, monkeypatch):
        """Test CLI arguments override all other sources."""
        # Setup all sources with same key
        config_file = tmp_path / "config.toml"
        config_file.write_text('[default]\naccount = "file_account"\n')

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        account = resolver.resolve_value("account")

        assert account == "cli_account"

        # Verify all sources were consulted
        history = resolver.get_resolution_history("account")
        assert len(history.entries) == 3
        assert len(history.overridden_entries) == 2

    def test_layered_fallback(self, tmp_path, monkeypatch):
        """Test layered fallback across multiple sources."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            "[default]\n"
            'account = "file_account"\n'
            'user = "file_user"\n'
            'database = "file_db"\n'
            'warehouse = "file_warehouse"\n'
            'role = "file_role"\n'
        )

        # Env only provides some values
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")
        monkeypatch.setenv("SNOWFLAKE_USER", "env_user")

        # CLI only provides one value
        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        config = resolver.resolve()

        # Verify layered fallback
        assert config["account"] == "cli_account"  # From CLI
        assert config["user"] == "env_user"  # From Env (CLI didn't have it)
        assert config["database"] == "file_db"  # From File (neither CLI nor Env had it)
        assert config["warehouse"] == "file_warehouse"  # From File
        assert config["role"] == "file_role"  # From File

    def test_summary_statistics_complete(self, tmp_path, monkeypatch):
        """Test summary statistics for complete resolution."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            "[default]\n"
            'account = "file_account"\n'
            'user = "file_user"\n'
            'database = "file_db"\n'
        )

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")
        monkeypatch.setenv("SNOWFLAKE_USER", "env_user")

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        config = resolver.resolve()

        summary = resolver.get_history_summary()

        # At least 3 keys
        assert summary["total_keys_resolved"] >= 3

        # account and user have overrides
        assert summary["keys_with_overrides"] >= 2

        # Source usage: File provided at least 3, Env provided at least 2, CLI provided 1
        assert summary["source_usage"]["toml:default"] >= 3
        assert summary["source_usage"]["snowflake_cli_env"] >= 2
        assert summary["source_usage"]["cli_arguments"] == 1

        # Source wins: CLI won 1 (account), Env won at least 1 (user), File won at least 1 (database)
        assert summary["source_wins"]["cli_arguments"] == 1  # account
        assert (
            summary["source_wins"]["snowflake_cli_env"] >= 1
        )  # user and possibly others
        assert (
            summary["source_wins"]["toml:default"] >= 1
        )  # database and possibly others

    def test_no_sources_with_default(self):
        """Test resolver with no sources returns default."""
        resolver = ConfigurationResolver()

        value = resolver.resolve_value("missing", default="default_value")

        assert value == "default_value"

        # Verify default tracked in history
        history = resolver.get_resolution_history("missing")
        assert history.default_used is True
        assert history.final_value == "default_value"

    def test_real_world_multiple_connections(self, tmp_path):
        """Test real-world scenario with multiple connection configs."""
        # User has both SnowCLI and SnowSQL configs with different connections
        snowcli_config = tmp_path / "connections.toml"
        snowcli_config.write_text(
            "[prod]\n"
            'account = "prod_account"\n'
            'user = "prod_user"\n'
            "[dev]\n"
            'account = "dev_account"\n'
            'user = "dev_user"\n'
        )

        # Test resolving prod connection
        file_source = FileSource(
            file_paths=[snowcli_config],
            handlers=[TomlFileHandler(section_path=["prod"])],
        )

        resolver_prod = ConfigurationResolver(sources=[file_source])
        prod_config = resolver_prod.resolve()

        assert prod_config["account"] == "prod_account"
        assert prod_config["user"] == "prod_user"

        # Test resolving dev connection
        file_source_dev = FileSource(
            file_paths=[snowcli_config],
            handlers=[TomlFileHandler(section_path=["dev"])],
        )

        resolver_dev = ConfigurationResolver(sources=[file_source_dev])
        dev_config = resolver_dev.resolve()

        assert dev_config["account"] == "dev_account"
        assert dev_config["user"] == "dev_user"

    def test_empty_sources_empty_result(self):
        """Test resolver with empty sources returns empty config."""
        cli_source = CliArgumentSource(cli_context={})
        env_source = EnvironmentSource(handlers=[])
        file_source = FileSource(file_paths=[], handlers=[])

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        config = resolver.resolve()

        assert config == {}
