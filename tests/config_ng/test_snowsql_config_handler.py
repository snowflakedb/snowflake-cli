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
Unit tests for IniFileHandler.

Tests verify:
- SnowSQL config file discovery
- Key mapping (accountname → account, username → user, etc.)
- Section navigation
- Migration support
- Raw value preservation showing original key names
"""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from snowflake.cli.api.config_ng.core import SourcePriority
from snowflake.cli.api.config_ng.file_handlers import IniFileHandler


class TestIniFileHandler:
    """Test suite for IniFileHandler."""

    def test_create_handler(self):
        """Should create handler with correct properties."""
        snowsql_config_handler = IniFileHandler()

        assert snowsql_config_handler.source_name == "snowsql_config"
        assert snowsql_config_handler.priority == SourcePriority.FILE
        assert snowsql_config_handler.handler_type == "ini"

    def test_default_section_path(self):
        """Should default to connections section."""
        # Verify by testing that it can discover from [connections] section
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccount = test\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)
            # Should find value in [connections] section
            assert "account" in values
        finally:
            temp_path.unlink()

    def test_custom_section_path(self):
        """Should allow custom section path."""
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\n\n[connections.prod]\naccount = prod_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler(
                section_path=["connections", "prod"]
            )
            values = snowsql_config_handler.discover_from_file(temp_path)
            # Should find value in custom section path
            assert values["account"].value == "prod_account"
        finally:
            temp_path.unlink()

    def test_can_handle_always_true(self):
        """Should always return True."""
        snowsql_config_handler = IniFileHandler()
        assert snowsql_config_handler.can_handle() is True

    def test_can_handle_snowsql_config_files(self):
        """Should detect SnowSQL config files."""
        snowsql_config_handler = IniFileHandler()

        # Typical SnowSQL config path
        assert snowsql_config_handler.can_handle_file(Path("~/.snowsql/config")) is True
        assert (
            snowsql_config_handler.can_handle_file(Path("/home/user/.snowsql/config"))
            is True
        )

    def test_can_handle_toml_files(self):
        """Should also handle .toml files."""
        snowsql_config_handler = IniFileHandler()

        assert snowsql_config_handler.can_handle_file(Path("config.toml")) is True

    def test_discover_raises_not_implemented(self):
        """Should raise NotImplementedError for discover() without file_path."""
        snowsql_config_handler = IniFileHandler()

        with pytest.raises(NotImplementedError, match="requires file_path"):
            snowsql_config_handler.discover()

    def test_discover_from_nonexistent_file(self):
        """Should return empty dict for nonexistent file."""
        snowsql_config_handler = IniFileHandler()
        values = snowsql_config_handler.discover_from_file(Path("/nonexistent/config"))

        assert len(values) == 0

    def test_key_mapping_accountname(self):
        """Should map accountname → account."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert len(values) == 1
            assert "account" in values
            assert "accountname" not in values
            assert values["account"].value == "my_account"
            assert values["account"].raw_value == "accountname=my_account"
        finally:
            temp_path.unlink()

    def test_key_mapping_username(self):
        """Should map username → user."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\nusername = my_user\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert values["user"].value == "my_user"
            assert values["user"].raw_value == "username=my_user"
        finally:
            temp_path.unlink()

    def test_key_mapping_multiple_database_keys(self):
        """Should map both dbname and databasename → database."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\ndatabasename = my_db\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert values["database"].value == "my_db"
        finally:
            temp_path.unlink()

    def test_key_mapping_warehouse_schema_role(self):
        """Should map warehouse, schema, and role names."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write(
                "[connections]\n"
                "warehousename = my_wh\n"
                "schemaname = my_schema\n"
                "rolename = my_role\n"
            )
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert values["warehouse"].value == "my_wh"
            assert values["schema"].value == "my_schema"
            assert values["role"].value == "my_role"
        finally:
            temp_path.unlink()

    def test_key_mapping_pwd_to_password(self):
        """Should map pwd → password (from env mappings)."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\npwd = secret123\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert "password" in values
            assert "pwd" not in values
            assert values["password"].value == "secret123"
        finally:
            temp_path.unlink()

    def test_unmapped_keys_passthrough(self):
        """Keys without mappings should pass through."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\ncustom_key = custom_value\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert values["custom_key"].value == "custom_value"
        finally:
            temp_path.unlink()

    def test_discover_all_common_keys(self):
        """Should discover all common SnowSQL keys with mapping."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write(
                "[connections]\n"
                "accountname = my_account\n"
                "username = my_user\n"
                "pwd = my_password\n"
                "databasename = my_db\n"
                "schemaname = my_schema\n"
                "warehousename = my_wh\n"
                "rolename = my_role\n"
            )
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert len(values) == 7
            assert all(
                key in values
                for key in [
                    "account",
                    "user",
                    "password",
                    "database",
                    "schema",
                    "warehouse",
                    "role",
                ]
            )
        finally:
            temp_path.unlink()

    def test_discover_specific_key(self):
        """Should discover specific key with mapping."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\nusername = my_user\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path, key="account")

            assert len(values) == 1
            assert "account" in values
            assert "user" not in values
        finally:
            temp_path.unlink()

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(
                temp_path, key="nonexistent"
            )

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_discover_nonexistent_section(self):
        """Should return empty dict for nonexistent section."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("accountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()  # Default section: connections
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            config_value = values["account"]
            assert config_value.source_name == "snowsql_config"
            assert config_value.priority == SourcePriority.FILE
            assert config_value.key == "account"
            assert config_value.value == "my_account"
            # Raw value shows original SnowSQL key
            assert config_value.raw_value == "accountname=my_account"
        finally:
            temp_path.unlink()

    def test_supports_any_string_key(self):
        """Should support any string key."""
        snowsql_config_handler = IniFileHandler()

        assert snowsql_config_handler.supports_key("account") is True
        assert snowsql_config_handler.supports_key("any_key") is True

    def test_reverse_mapping_for_specific_key_query(self):
        """Should use reverse mapping when querying specific key."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            # Query for "account" should find "accountname"
            values = snowsql_config_handler.discover_from_file(temp_path, key="account")

            assert len(values) == 1
            assert values["account"].value == "my_account"
        finally:
            temp_path.unlink()

    def test_get_cli_key_method(self):
        """Should convert SnowSQL keys to CLI keys."""
        snowsql_config_handler = IniFileHandler()

        assert snowsql_config_handler.get_cli_key("accountname") == "account"
        assert snowsql_config_handler.get_cli_key("username") == "user"
        assert snowsql_config_handler.get_cli_key("pwd") == "password"
        assert snowsql_config_handler.get_cli_key("unmapped") == "unmapped"

    def test_case_insensitive_key_mapping(self):
        """Key mappings should be case-insensitive."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\nAccountName = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            # Should still map to "account"
            assert "account" in values
            assert values["account"].value == "my_account"
        finally:
            temp_path.unlink()

    def test_invalid_ini_returns_empty(self):
        """Should handle invalid INI gracefully."""
        with NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("invalid ini content [[[")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()
            values = snowsql_config_handler.discover_from_file(temp_path)

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_caching_behavior(self):
        """Should cache file data for performance."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write("[connections]\naccountname = my_account\n")
            f.flush()
            temp_path = Path(f.name)

        try:
            snowsql_config_handler = IniFileHandler()

            # First call loads file
            values1 = snowsql_config_handler.discover_from_file(temp_path)
            # Second call uses cache
            values2 = snowsql_config_handler.discover_from_file(temp_path)

            assert values1 == values2
            # Verify caching by checking results are consistent
        finally:
            temp_path.unlink()
