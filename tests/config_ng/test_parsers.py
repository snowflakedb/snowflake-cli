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

"""Tests for configuration parsers."""

import pytest
from snowflake.cli.api.config_ng.parsers import SnowSQLParser, TOMLParser


class TestSnowSQLParser:
    """Test SnowSQL INI parser."""

    def test_parse_single_connection(self):
        """Test parsing a single connection."""
        content = """
[connections.dev]
accountname = myaccount
username = myuser
password = mypass
"""
        result = SnowSQLParser.parse(content)

        assert "connections" in result
        assert "dev" in result["connections"]
        assert result["connections"]["dev"] == {
            "account": "myaccount",
            "user": "myuser",
            "password": "mypass",
        }

    def test_parse_multiple_connections(self):
        """Test parsing multiple connections."""
        content = """
[connections.dev]
accountname = dev_account
username = dev_user

[connections.prod]
accountname = prod_account
username = prod_user
"""
        result = SnowSQLParser.parse(content)

        assert "connections" in result
        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_parse_default_connection(self):
        """Test parsing default connection (no name suffix)."""
        content = """
[connections]
accountname = default_account
username = default_user
"""
        result = SnowSQLParser.parse(content)

        assert "connections" in result
        assert "default" in result["connections"]
        assert result["connections"]["default"]["account"] == "default_account"

    def test_key_mapping_accountname_to_account(self):
        """Test that accountname is mapped to account."""
        content = """
[connections.test]
accountname = test_account
"""
        result = SnowSQLParser.parse(content)

        assert "account" in result["connections"]["test"]
        assert result["connections"]["test"]["account"] == "test_account"

    def test_key_mapping_username_to_user(self):
        """Test that username is mapped to user."""
        content = """
[connections.test]
username = test_user
"""
        result = SnowSQLParser.parse(content)

        assert "user" in result["connections"]["test"]
        assert result["connections"]["test"]["user"] == "test_user"

    def test_key_mapping_dbname_to_database(self):
        """Test that dbname is mapped to database."""
        content = """
[connections.test]
dbname = test_db
"""
        result = SnowSQLParser.parse(content)

        assert "database" in result["connections"]["test"]
        assert result["connections"]["test"]["database"] == "test_db"

    def test_key_mapping_pwd_to_password(self):
        """Test that pwd is mapped to password."""
        content = """
[connections.test]
pwd = test_pass
"""
        result = SnowSQLParser.parse(content)

        assert "password" in result["connections"]["test"]
        assert result["connections"]["test"]["password"] == "test_pass"

    def test_key_mapping_multiple_keys(self):
        """Test mapping multiple keys at once."""
        content = """
[connections.test]
accountname = acc
username = usr
dbname = db
schemaname = sch
warehousename = wh
rolename = rol
"""
        result = SnowSQLParser.parse(content)

        conn = result["connections"]["test"]
        assert conn["account"] == "acc"
        assert conn["user"] == "usr"
        assert conn["database"] == "db"
        assert conn["schema"] == "sch"
        assert conn["warehouse"] == "wh"
        assert conn["role"] == "rol"

    def test_parse_variables_section(self):
        """Test parsing variables section."""
        content = """
[variables]
stage = mystage
table = mytable
schema = myschema
"""
        result = SnowSQLParser.parse(content)

        assert "variables" in result
        assert result["variables"]["stage"] == "mystage"
        assert result["variables"]["table"] == "mytable"
        assert result["variables"]["schema"] == "myschema"

    def test_parse_connections_and_variables(self):
        """Test parsing both connections and variables."""
        content = """
[connections.dev]
accountname = dev_account

[variables]
env = development
"""
        result = SnowSQLParser.parse(content)

        assert "connections" in result
        assert "variables" in result
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["variables"]["env"] == "development"

    def test_parse_empty_content(self):
        """Test parsing empty content."""
        result = SnowSQLParser.parse("")

        assert result == {}

    def test_parse_no_connections_section(self):
        """Test parsing config without connections section."""
        content = """
[variables]
key = value
"""
        result = SnowSQLParser.parse(content)

        assert "connections" not in result
        assert "variables" in result

    def test_parse_preserves_unmapped_keys(self):
        """Test that unmapped keys are preserved as-is."""
        content = """
[connections.test]
custom_key = custom_value
another_key = another_value
"""
        result = SnowSQLParser.parse(content)

        conn = result["connections"]["test"]
        assert conn["custom_key"] == "custom_value"
        assert conn["another_key"] == "another_value"

    def test_parse_connection_with_special_characters(self):
        """Test parsing connection with special characters in name."""
        content = """
[connections.my-test_conn]
accountname = test
"""
        result = SnowSQLParser.parse(content)

        assert "my-test_conn" in result["connections"]

    def test_parse_values_with_spaces(self):
        """Test parsing values that contain spaces."""
        content = """
[connections.test]
accountname = my account name
"""
        result = SnowSQLParser.parse(content)

        assert result["connections"]["test"]["account"] == "my account name"


class TestTOMLParser:
    """Test TOML parser."""

    def test_parse_simple_toml(self):
        """Test parsing simple TOML."""
        content = """
[connections.test]
account = "test_account"
user = "test_user"
"""
        result = TOMLParser.parse(content)

        assert "connections" in result
        assert "test" in result["connections"]
        assert result["connections"]["test"]["account"] == "test_account"
        assert result["connections"]["test"]["user"] == "test_user"

    def test_parse_nested_toml(self):
        """Test parsing nested TOML structure."""
        content = """
[cli]
enable_diag = true

[cli.logs]
save_logs = true

[connections.prod]
account = "prod_account"
"""
        result = TOMLParser.parse(content)

        assert "cli" in result
        assert result["cli"]["enable_diag"] is True
        assert result["cli"]["logs"]["save_logs"] is True
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_parse_multiple_connections(self):
        """Test parsing multiple connections in TOML."""
        content = """
[connections.dev]
account = "dev_account"

[connections.prod]
account = "prod_account"
"""
        result = TOMLParser.parse(content)

        assert len(result["connections"]) == 2
        assert result["connections"]["dev"]["account"] == "dev_account"
        assert result["connections"]["prod"]["account"] == "prod_account"

    def test_parse_variables(self):
        """Test parsing variables section."""
        content = """
[variables]
stage = "mystage"
table = "mytable"
"""
        result = TOMLParser.parse(content)

        assert "variables" in result
        assert result["variables"]["stage"] == "mystage"
        assert result["variables"]["table"] == "mytable"

    def test_parse_empty_content(self):
        """Test parsing empty TOML."""
        result = TOMLParser.parse("")

        assert result == {}

    def test_parse_toml_with_types(self):
        """Test parsing TOML with different value types."""
        content = """
[test]
string_val = "text"
int_val = 42
float_val = 3.14
bool_val = true
array_val = ["a", "b", "c"]
"""
        result = TOMLParser.parse(content)

        assert result["test"]["string_val"] == "text"
        assert result["test"]["int_val"] == 42
        assert result["test"]["float_val"] == 3.14
        assert result["test"]["bool_val"] is True
        assert result["test"]["array_val"] == ["a", "b", "c"]

    def test_parse_malformed_toml_raises_error(self):
        """Test that malformed TOML raises an error."""
        content = """
[connections.test
account = "broken
"""
        with pytest.raises(Exception):  # tomllib raises TOMLDecodeError
            TOMLParser.parse(content)

    def test_parse_toml_with_inline_table(self):
        """Test parsing TOML with inline tables."""
        content = """
[connections]
dev = { account = "dev_acc", user = "dev_user" }
"""
        result = TOMLParser.parse(content)

        assert result["connections"]["dev"]["account"] == "dev_acc"
        assert result["connections"]["dev"]["user"] == "dev_user"

    def test_parse_legacy_connections_format(self):
        """Test parsing legacy connections.toml format (direct sections)."""
        content = """
[dev]
account = "dev_account"
user = "dev_user"

[prod]
account = "prod_account"
user = "prod_user"
"""
        result = TOMLParser.parse(content)

        # Note: TOMLParser just parses, doesn't normalize
        assert "dev" in result
        assert "prod" in result
        assert result["dev"]["account"] == "dev_account"
        assert result["prod"]["account"] == "prod_account"
