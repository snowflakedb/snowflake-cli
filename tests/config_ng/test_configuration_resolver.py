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
Unit tests for ConfigurationResolver.

Tests verify:
- Source orchestration
- Precedence rules (CLI > Env > Files)
- History tracking integration
- Resolution methods
- Debugging utilities
"""

import json

from snowflake.cli.api.config_ng.resolver import ConfigurationResolver
from snowflake.cli.api.config_ng.sources import (
    CliArgumentSource,
    EnvironmentSource,
    FileSource,
)


class TestConfigurationResolver:
    """Test suite for ConfigurationResolver."""

    def test_create_resolver_empty(self):
        """Should create resolver with no sources."""
        resolver = ConfigurationResolver()

        assert len(resolver.get_sources()) == 0

    def test_create_resolver_with_sources(self):
        """Should create resolver with provided sources."""
        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        sources = resolver.get_sources()
        assert len(sources) == 1

    def test_sources_sorted_by_priority(self):
        """Should sort sources by priority (highest first)."""
        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[])
        file_source = FileSource(file_paths=[], handlers=[])

        # Add in wrong order
        resolver = ConfigurationResolver(sources=[file_source, cli_source, env_source])

        sources = resolver.get_sources()
        # Should be sorted: CLI (1), Env (2), File (3)
        assert sources[0].priority.value == 1  # CLI
        assert sources[1].priority.value == 2  # Env
        assert sources[2].priority.value == 3  # File

    def test_add_source(self):
        """Should add source and re-sort."""
        resolver = ConfigurationResolver()

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        resolver.add_source(cli_source)

        assert len(resolver.get_sources()) == 1

    def test_resolve_from_single_source(self):
        """Should resolve values from single source."""
        cli_source = CliArgumentSource(
            cli_context={"account": "my_account", "user": "my_user"}
        )
        resolver = ConfigurationResolver(sources=[cli_source])

        config = resolver.resolve()

        assert config["account"] == "my_account"
        assert config["user"] == "my_user"

    def test_resolve_specific_key(self):
        """Should resolve specific key only."""
        cli_source = CliArgumentSource(
            cli_context={"account": "my_account", "user": "my_user"}
        )
        resolver = ConfigurationResolver(sources=[cli_source])

        config = resolver.resolve(key="account")

        assert len(config) == 1
        assert config["account"] == "my_account"

    def test_resolve_value_method(self):
        """Should resolve single value."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        account = resolver.resolve_value("account")

        assert account == "my_account"

    def test_resolve_with_default(self):
        """Should return default when key not found."""
        resolver = ConfigurationResolver()

        value = resolver.resolve_value("missing_key", default="default_value")

        assert value == "default_value"

    def test_cli_overrides_env(self, monkeypatch):
        """CLI values should override environment values."""
        from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])

        resolver = ConfigurationResolver(sources=[cli_source, env_source])

        account = resolver.resolve_value("account")

        assert account == "cli_account"

    def test_env_overrides_file(self, tmp_path, monkeypatch):
        """Environment values should override file values."""
        from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler
        from snowflake.cli.api.config_ng.file_handlers import TomlFileHandler

        # Create config file
        config_file = tmp_path / "config.toml"
        config_file.write_text('[default]\naccount = "file_account"\n')

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[env_source, file_source])

        account = resolver.resolve_value("account")

        assert account == "env_account"

    def test_complete_precedence_chain(self, tmp_path, monkeypatch):
        """Test complete precedence: CLI > Env > File."""
        from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler
        from snowflake.cli.api.config_ng.file_handlers import TomlFileHandler

        # Create config file
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

        # CLI should win
        assert account == "cli_account"

    def test_fallback_to_lower_priority(self, tmp_path, monkeypatch):
        """Should use lower priority source when higher doesn't have value."""
        from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler
        from snowflake.cli.api.config_ng.file_handlers import TomlFileHandler

        # Create config file
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[default]\naccount = "file_account"\nuser = "file_user"\n'
        )

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        # CLI doesn't have any values
        cli_source = CliArgumentSource(cli_context={})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])

        config = resolver.resolve()

        # Account from env, user from file
        assert config["account"] == "env_account"
        assert config["user"] == "file_user"

    def test_get_resolution_history(self):
        """Should get resolution history for a key."""
        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()

        history = resolver.get_resolution_history("account")

        assert history is not None
        assert history.key == "account"
        assert history.final_value == "cli_account"

    def test_get_all_histories(self):
        """Should get all resolution histories."""
        cli_source = CliArgumentSource(
            cli_context={"account": "my_account", "user": "my_user"}
        )
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()

        histories = resolver.get_all_histories()

        assert len(histories) == 2
        assert "account" in histories
        assert "user" in histories

    def test_get_value_metadata(self):
        """Should get metadata for resolved value."""
        cli_source = CliArgumentSource(cli_context={"account": "cli_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()

        metadata = resolver.get_value_metadata("account")

        assert metadata is not None
        assert metadata.key == "account"
        assert metadata.value == "cli_account"
        assert metadata.source_name == "cli_arguments"

    def test_get_history_summary(self, tmp_path, monkeypatch):
        """Should get summary statistics."""
        from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler
        from snowflake.cli.api.config_ng.file_handlers import TomlFileHandler

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[default]\naccount = "file_account"\nuser = "file_user"\n'
        )

        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")

        cli_source = CliArgumentSource(cli_context={})
        env_source = EnvironmentSource(handlers=[SnowCliEnvHandler()])
        file_source = FileSource(
            file_paths=[config_file],
            handlers=[TomlFileHandler(section_path=["default"])],
        )

        resolver = ConfigurationResolver(sources=[cli_source, env_source, file_source])
        config = resolver.resolve()

        summary = resolver.get_history_summary()

        # Check that we resolved at least the expected keys
        assert summary["total_keys_resolved"] >= 2
        assert summary["keys_with_overrides"] >= 1  # account overridden
        assert (
            summary["source_wins"]["snowflake_cli_env"] >= 1
        )  # account (and possibly others)
        assert summary["source_wins"]["toml:default"] >= 1  # user and possibly others

    def test_disable_enable_history(self):
        """Should disable and enable history tracking."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source], track_history=False)

        resolver.resolve()

        # No history tracked
        histories = resolver.get_all_histories()
        assert len(histories) == 0

        # Enable and resolve again
        resolver.enable_history()
        resolver.resolve()

        histories = resolver.get_all_histories()
        assert len(histories) == 1

    def test_clear_history(self):
        """Should clear resolution history."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()
        assert len(resolver.get_all_histories()) == 1

        resolver.clear_history()
        assert len(resolver.get_all_histories()) == 0

    def test_format_resolution_chain(self):
        """Should format resolution chain."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()
        formatted = resolver.format_resolution_chain("account")

        assert "account resolution chain" in formatted
        assert "my_account" in formatted
        assert "SELECTED" in formatted

    def test_format_resolution_chain_nonexistent_key(self):
        """Should return message for nonexistent key."""
        resolver = ConfigurationResolver()

        formatted = resolver.format_resolution_chain("nonexistent")

        assert "No resolution history found" in formatted

    def test_format_all_chains(self):
        """Should format all resolution chains."""
        cli_source = CliArgumentSource(
            cli_context={"account": "my_account", "user": "my_user"}
        )
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()
        formatted = resolver.format_all_chains()

        assert "Configuration Resolution History" in formatted
        assert "account resolution chain" in formatted
        assert "user resolution chain" in formatted

    def test_format_all_chains_when_empty(self):
        """Should return message when no history available."""
        resolver = ConfigurationResolver(track_history=False)

        formatted = resolver.format_all_chains()

        assert "No resolution history available" in formatted

    def test_export_history(self, tmp_path):
        """Should export history to JSON file."""
        cli_source = CliArgumentSource(
            cli_context={"account": "my_account", "user": "my_user"}
        )
        resolver = ConfigurationResolver(sources=[cli_source])

        resolver.resolve()

        export_file = tmp_path / "debug_config.json"
        resolver.export_history(export_file)

        assert export_file.exists()

        # Check JSON structure
        with open(export_file) as f:
            data = json.load(f)

        assert "summary" in data
        assert "histories" in data
        assert "account" in data["histories"]
        assert "user" in data["histories"]

    def test_source_error_does_not_break_resolution(self):
        """Should continue resolution if a source fails."""
        from snowflake.cli.api.config_ng.core import SourcePriority, ValueSource

        class FailingSource(ValueSource):
            @property
            def source_name(self) -> str:
                return "failing_source"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.ENVIRONMENT

            def discover(self, key=None):
                raise RuntimeError("Source failed")

            def supports_key(self, key: str) -> bool:
                return True

        failing_source = FailingSource()
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})

        resolver = ConfigurationResolver(sources=[failing_source, cli_source])

        # Should still get value from CLI source
        account = resolver.resolve_value("account")
        assert account == "my_account"

    def test_get_sources_returns_copy(self):
        """get_sources should return a copy."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        sources = resolver.get_sources()
        sources.clear()

        # Original sources should be unchanged
        assert len(resolver.get_sources()) == 1

    def test_resolve_with_no_sources(self):
        """Should return empty dict when no sources configured."""
        resolver = ConfigurationResolver()

        config = resolver.resolve()

        assert config == {}

    def test_resolve_value_returns_default_when_not_found(self):
        """Should return default value when key not found."""
        resolver = ConfigurationResolver()

        value = resolver.resolve_value("missing", default="default_value")

        assert value == "default_value"

    def test_multiple_resolve_calls_consistent(self):
        """Multiple resolve calls should return consistent results."""
        cli_source = CliArgumentSource(cli_context={"account": "my_account"})
        resolver = ConfigurationResolver(sources=[cli_source])

        config1 = resolver.resolve()
        config2 = resolver.resolve()

        assert config1 == config2
