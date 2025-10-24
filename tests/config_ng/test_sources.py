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

"""Tests for configuration sources with string-based testing."""

from snowflake.cli.api.config_ng.sources import (
    CliConfigFile,
    ConnectionsConfigFile,
    SnowSQLConfigFile,
)


class TestSnowSQLConfigFileFromString:
    """Test SnowSQLConfigFile with string-based initialization."""

    def test_from_string_single_connection(self):
        """Test creating source from string with single connection."""
        content = """
[connections.dev]
accountname = test_account
username = test_user
password = test_pass
"""
        source = SnowSQLConfigFile.from_string(content)
        result = source.discover()

        assert "connections" in result
        assert "dev" in result["connections"]
        assert result["connections"]["dev"]["account"] == "test_account"
        assert result["connections"]["dev"]["user"] == "test_user"
        assert result["connections"]["dev"]["password"] == "test_pass"

    def test_from_string_multiple_connections(self):
        """Test creating source from string with multiple connections."""
        content = """
[connections.dev]
accountname = dev_account

[connections.prod]
accountname = prod_account
"""
        source = SnowSQLConfigFile.from_string(content)
        result = source.discover()

        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_from_string_with_variables(self):
        """Test creating source from string with variables section."""
        content = """
[connections.test]
accountname = test_account

[variables]
stage = mystage
table = mytable
"""
        source = SnowSQLConfigFile.from_string(content)
        result = source.discover()

        assert "variables" in result
        assert result["variables"]["stage"] == "mystage"
        assert result["variables"]["table"] == "mytable"

    def test_from_string_key_mapping(self):
        """Test that SnowSQL key mapping works with string source."""
        content = """
[connections.test]
accountname = acc
username = usr
dbname = db
schemaname = sch
warehousename = wh
rolename = rol
pwd = pass
"""
        source = SnowSQLConfigFile.from_string(content)
        result = source.discover()

        conn = result["connections"]["test"]
        assert conn["account"] == "acc"
        assert conn["user"] == "usr"
        assert conn["database"] == "db"
        assert conn["schema"] == "sch"
        assert conn["warehouse"] == "wh"
        assert conn["role"] == "rol"
        assert conn["password"] == "pass"

    def test_from_string_empty_content(self):
        """Test creating source from empty string."""
        source = SnowSQLConfigFile.from_string("")
        result = source.discover()

        assert result == {}

    def test_from_string_default_connection(self):
        """Test creating source with default connection (no name)."""
        content = """
[connections]
accountname = default_account
"""
        source = SnowSQLConfigFile.from_string(content)
        result = source.discover()

        assert "default" in result["connections"]
        assert result["connections"]["default"]["account"] == "default_account"


class TestCliConfigFileFromString:
    """Test CliConfigFile with string-based initialization."""

    def test_from_string_single_connection(self):
        """Test creating CLI config source from string."""
        content = """
[connections.dev]
account = "test_account"
user = "test_user"
"""
        source = CliConfigFile.from_string(content)
        result = source.discover()

        assert "connections" in result
        assert "dev" in result["connections"]
        assert result["connections"]["dev"]["account"] == "test_account"
        assert result["connections"]["dev"]["user"] == "test_user"

    def test_from_string_multiple_connections(self):
        """Test creating CLI config with multiple connections."""
        content = """
[connections.dev]
account = "dev_acc"

[connections.prod]
account = "prod_acc"
"""
        source = CliConfigFile.from_string(content)
        result = source.discover()

        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_acc"
        assert result["connections"]["prod"]["account"] == "prod_acc"

    def test_from_string_with_cli_section(self):
        """Test creating CLI config with cli section."""
        content = """
[cli]
enable_diag = true

[cli.logs]
save_logs = true

[connections.test]
account = "test_account"
"""
        source = CliConfigFile.from_string(content)
        result = source.discover()

        assert "cli" in result
        assert result["cli"]["enable_diag"] is True
        assert result["cli"]["logs"]["save_logs"] is True
        assert result["connections"]["test"]["account"] == "test_account"

    def test_from_string_with_variables(self):
        """Test creating CLI config with variables."""
        content = """
[variables]
stage = "mystage"
env = "dev"

[connections.test]
account = "test_account"
"""
        source = CliConfigFile.from_string(content)
        result = source.discover()

        assert "variables" in result
        assert result["variables"]["stage"] == "mystage"
        assert result["variables"]["env"] == "dev"

    def test_from_string_empty_content(self):
        """Test creating CLI config from empty string."""
        source = CliConfigFile.from_string("")
        result = source.discover()

        assert result == {}

    def test_from_string_nested_structure(self):
        """Test creating CLI config with deeply nested structure."""
        content = """
[cli.features]
feature1 = true
feature2 = false

[cli.logs]
level = "INFO"
path = "/var/log"
"""
        source = CliConfigFile.from_string(content)
        result = source.discover()

        assert result["cli"]["features"]["feature1"] is True
        assert result["cli"]["logs"]["level"] == "INFO"


class TestConnectionsConfigFileFromString:
    """Test ConnectionsConfigFile with string-based initialization."""

    def test_from_string_nested_format(self):
        """Test creating connections file with nested format."""
        content = """
[connections.dev]
account = "dev_account"
user = "dev_user"

[connections.prod]
account = "prod_account"
user = "prod_user"
"""
        source = ConnectionsConfigFile.from_string(content)
        result = source.discover()

        assert "connections" in result
        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_from_string_legacy_format(self):
        """Test creating connections file with legacy format (direct sections)."""
        content = """
[dev]
account = "dev_account"
user = "dev_user"

[prod]
account = "prod_account"
user = "prod_user"
"""
        source = ConnectionsConfigFile.from_string(content)
        result = source.discover()

        # Legacy format should be normalized to nested format
        assert "connections" in result
        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_from_string_mixed_format(self):
        """Test creating connections file with mixed legacy and nested format."""
        content = """
[legacy_conn]
account = "legacy_account"

[connections.new_conn]
account = "new_account"
"""
        source = ConnectionsConfigFile.from_string(content)
        result = source.discover()

        # Both should be normalized to nested format
        assert "connections" in result
        assert len(result["connections"]) == 2
        assert result["connections"]["legacy_conn"]["account"] == "legacy_account"
        assert result["connections"]["new_conn"]["account"] == "new_account"

    def test_from_string_nested_takes_precedence(self):
        """Test that nested format takes precedence over legacy format."""
        content = """
[test]
account = "legacy_account"

[connections.test]
account = "new_account"
"""
        source = ConnectionsConfigFile.from_string(content)
        result = source.discover()

        # Nested format should win
        assert result["connections"]["test"]["account"] == "new_account"

    def test_from_string_empty_content(self):
        """Test creating connections file from empty string."""
        source = ConnectionsConfigFile.from_string("")
        result = source.discover()

        # Empty TOML should return empty dict (no connections)
        assert result == {}

    def test_from_string_single_connection(self):
        """Test creating connections file with single connection."""
        content = """
[connections.default]
account = "test_account"
user = "test_user"
password = "test_pass"
"""
        source = ConnectionsConfigFile.from_string(content)
        result = source.discover()

        assert "default" in result["connections"]
        assert result["connections"]["default"]["account"] == "test_account"

    def test_get_defined_connections(self):
        """Test getting defined connection names."""
        content = """
[connections.dev]
account = "dev_acc"

[connections.prod]
account = "prod_acc"
"""
        source = ConnectionsConfigFile.from_string(content)
        defined_connections = source.get_defined_connections()

        assert defined_connections == {"dev", "prod"}

    def test_get_defined_connections_legacy_format(self):
        """Test getting defined connections with legacy format."""
        content = """
[dev]
account = "dev_acc"

[prod]
account = "prod_acc"
"""
        source = ConnectionsConfigFile.from_string(content)
        defined_connections = source.get_defined_connections()

        assert defined_connections == {"dev", "prod"}


class TestSourceProperties:
    """Test source properties and metadata."""

    def test_snowsql_config_source_name(self):
        """Test SnowSQLConfigFile source name."""
        source = SnowSQLConfigFile.from_string("")
        assert source.source_name == "snowsql_config"

    def test_cli_config_source_name(self):
        """Test CliConfigFile source name."""
        source = CliConfigFile.from_string("")
        assert source.source_name == "cli_config_toml"

    def test_connections_config_source_name(self):
        """Test ConnectionsConfigFile source name."""
        source = ConnectionsConfigFile.from_string("")
        assert source.source_name == "connections_toml"

    def test_connections_file_marker(self):
        """Test that ConnectionsConfigFile is marked as connections file."""
        source = ConnectionsConfigFile.from_string("")
        assert source.is_connections_file is True

    def test_non_connections_file_marker(self):
        """Test that other sources don't have is_connections_file property."""
        cli_source = CliConfigFile.from_string("")
        snowsql_source = SnowSQLConfigFile.from_string("")

        # These should not have the is_connections_file property
        # or it should be False (default)
        assert (
            not hasattr(cli_source, "is_connections_file")
            or not cli_source.is_connections_file
        )
        assert (
            not hasattr(snowsql_source, "is_connections_file")
            or not snowsql_source.is_connections_file
        )
