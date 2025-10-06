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
Unit tests for SnowSqlEnvHandler.

Tests verify:
- SNOWSQL_* environment variable discovery
- Key mapping (PWD → password)
- Value type parsing (string, int, bool)
- Case handling
- Raw value preservation
- Migration support
"""

import os
from unittest.mock import patch

from snowflake.cli.api.config_ng.core import SourcePriority
from snowflake.cli.api.config_ng.env_handlers import SnowSqlEnvHandler


class TestSnowSqlEnvHandler:
    """Test suite for SnowSqlEnvHandler."""

    def test_create_handler(self):
        """Should create handler with correct properties."""
        handler = SnowSqlEnvHandler()

        assert handler.source_name == "snowsql_env"
        assert handler.priority == SourcePriority.ENVIRONMENT
        assert handler.handler_type == "snowsql_env"

    def test_can_handle_with_no_env_vars(self):
        """Should return False when no SNOWSQL_* vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            handler = SnowSqlEnvHandler()
            assert handler.can_handle() is False

    def test_can_handle_with_env_vars(self):
        """Should return True when SNOWSQL_* vars are present."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "test_account"}):
            handler = SnowSqlEnvHandler()
            assert handler.can_handle() is True

    def test_discover_single_string_value(self):
        """Should discover single string value."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "my_account"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "my_account"

    def test_key_mapping_pwd_to_password(self):
        """Should map SNOWSQL_PWD to 'password' key."""
        with patch.dict(os.environ, {"SNOWSQL_PWD": "secret123"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert len(values) == 1
            assert "password" in values  # Mapped key
            assert "pwd" not in values  # Original key should not appear
            assert values["password"].value == "secret123"

    def test_discover_multiple_values_with_mapping(self):
        """Should discover multiple values with key mapping applied."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "my_account",
            "SNOWSQL_USER": "my_user",
            "SNOWSQL_PWD": "my_password",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert len(values) == 3
            assert values["account"].value == "my_account"
            assert values["user"].value == "my_user"
            assert values["password"].value == "my_password"  # Mapped from PWD

    def test_discover_specific_key_direct(self):
        """Should discover specific key that doesn't require mapping."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "my_account",
            "SNOWSQL_USER": "my_user",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover(key="account")

            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "my_account"

    def test_discover_specific_key_with_mapping(self):
        """Should discover specific key using reverse mapping."""
        with patch.dict(os.environ, {"SNOWSQL_PWD": "secret123"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover(key="password")

            assert len(values) == 1
            assert "password" in values
            assert values["password"].value == "secret123"

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "my_account"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover(key="nonexistent")

            assert len(values) == 0

    def test_case_conversion(self):
        """Should convert UPPERCASE env var names to lowercase config keys."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "test"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert "account" in values  # lowercase key
            assert "ACCOUNT" not in values

    def test_parse_value_types_same_as_snowcli(self):
        """Should parse values the same way as SnowCliEnvHandler."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "my_account",  # String
            "SNOWSQL_PORT": "443",  # Integer
            "SNOWSQL_ENABLE_DIAG": "true",  # Boolean
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert values["account"].value == "my_account"
            assert isinstance(values["account"].value, str)

            assert values["port"].value == 443
            assert isinstance(values["port"].value, int)

            assert values["enable_diag"].value is True
            assert isinstance(values["enable_diag"].value, bool)

    def test_parse_boolean_values(self):
        """Should parse various boolean representations."""
        for true_val in ["true", "TRUE", "1", "yes", "on"]:
            with patch.dict(os.environ, {"SNOWSQL_ENABLE_DIAG": true_val}, clear=True):
                handler = SnowSqlEnvHandler()
                values = handler.discover()
                assert values["enable_diag"].value is True

        for false_val in ["false", "FALSE", "0", "no", "off"]:
            with patch.dict(os.environ, {"SNOWSQL_ENABLE_DIAG": false_val}, clear=True):
                handler = SnowSqlEnvHandler()
                values = handler.discover()
                assert values["enable_diag"].value is False

    def test_raw_value_preservation(self):
        """Should preserve raw string value in raw_value field."""
        with patch.dict(os.environ, {"SNOWSQL_PORT": "443"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            config_value = values["port"]
            assert config_value.value == 443  # Parsed as int
            assert config_value.raw_value == "443"  # Original string

    def test_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "my_account"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            config_value = values["account"]
            assert config_value.source_name == "snowsql_env"
            assert config_value.priority == SourcePriority.ENVIRONMENT
            assert config_value.key == "account"

    def test_supports_any_string_key(self):
        """Should support any string key."""
        handler = SnowSqlEnvHandler()

        assert handler.supports_key("account") is True
        assert handler.supports_key("password") is True
        assert handler.supports_key("any_key") is True

    def test_ignores_non_snowsql_env_vars(self):
        """Should ignore environment variables without SNOWSQL_ prefix."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "snowsql_account",
            "SNOWFLAKE_ACCOUNT": "snowflake_account",
            "ACCOUNT": "plain_account",
            "PATH": "/usr/bin",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            # Should only get SNOWSQL_* variables
            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "snowsql_account"

    def test_reverse_mapping_lookup(self):
        """Should correctly perform reverse lookup for mapped keys."""
        handler = SnowSqlEnvHandler()

        # Test reverse mapping: password -> pwd
        snowsql_key = handler.get_snowsql_key("password")
        assert snowsql_key == "pwd"

        # Test non-mapped key returns itself
        snowsql_key = handler.get_snowsql_key("account")
        assert snowsql_key == "account"

    def test_migration_scenario_all_snowsql_vars(self):
        """Simulates user with only SnowSQL environment variables."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "legacy_account",
            "SNOWSQL_USER": "legacy_user",
            "SNOWSQL_PWD": "legacy_password",
            "SNOWSQL_WAREHOUSE": "legacy_warehouse",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert len(values) == 4
            assert values["account"].value == "legacy_account"
            assert values["user"].value == "legacy_user"
            assert values["password"].value == "legacy_password"
            assert values["warehouse"].value == "legacy_warehouse"

    def test_common_snowsql_variables(self):
        """Should handle common SnowSQL environment variables."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "my_account",
            "SNOWSQL_USER": "my_user",
            "SNOWSQL_PWD": "my_password",
            "SNOWSQL_DATABASE": "my_database",
            "SNOWSQL_SCHEMA": "my_schema",
            "SNOWSQL_WAREHOUSE": "my_warehouse",
            "SNOWSQL_ROLE": "my_role",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

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

    def test_empty_string_value(self):
        """Should handle empty string values."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": ""}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert values["account"].value == ""

    def test_special_characters_in_value(self):
        """Should handle special characters in values."""
        with patch.dict(os.environ, {"SNOWSQL_PWD": "p@ss!w0rd#123"}, clear=True):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert values["password"].value == "p@ss!w0rd#123"

    def test_underscore_in_key_preserved(self):
        """Should preserve underscores in environment variable keys."""
        with patch.dict(
            os.environ, {"SNOWSQL_PRIVATE_KEY_PATH": "/path/to/key"}, clear=True
        ):
            handler = SnowSqlEnvHandler()
            values = handler.discover()

            assert "private_key_path" in values
            assert values["private_key_path"].value == "/path/to/key"

    def test_multiple_discover_calls_consistent(self):
        """Multiple discover calls should return consistent results."""
        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "my_account"}, clear=True):
            handler = SnowSqlEnvHandler()

            values1 = handler.discover()
            values2 = handler.discover()

            assert values1 == values2
