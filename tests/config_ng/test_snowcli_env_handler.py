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
Unit tests for SnowCliEnvHandler.

Tests verify:
- SNOWFLAKE_* environment variable discovery
- Value type parsing (string, int, bool)
- Case handling (env vars are uppercase, keys are lowercase)
- Raw value preservation
- Priority and metadata
"""

import os
from unittest.mock import patch

from snowflake.cli.api.config_ng.core import SourcePriority
from snowflake.cli.api.config_ng.env_handlers import SnowCliEnvHandler


class TestSnowCliEnvHandler:
    """Test suite for SnowCliEnvHandler."""

    def test_create_handler(self):
        """Should create handler with correct properties."""
        handler = SnowCliEnvHandler()

        assert handler.source_name == "snowflake_cli_env"
        assert handler.priority == SourcePriority.ENVIRONMENT
        assert handler.handler_type == "snowflake_cli_env"

    def test_can_handle_with_no_env_vars(self):
        """Should return False when no SNOWFLAKE_* vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            handler = SnowCliEnvHandler()
            assert handler.can_handle() is False

    def test_can_handle_with_env_vars(self):
        """Should return True when SNOWFLAKE_* vars are present."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "test_account"}):
            handler = SnowCliEnvHandler()
            assert handler.can_handle() is True

    def test_discover_single_string_value(self):
        """Should discover single string value."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "my_account"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "my_account"
            assert values["account"].key == "account"

    def test_discover_multiple_values(self):
        """Should discover multiple environment variables."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "my_account",
            "SNOWFLAKE_USER": "my_user",
            "SNOWFLAKE_WAREHOUSE": "my_warehouse",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert len(values) == 3
            assert values["account"].value == "my_account"
            assert values["user"].value == "my_user"
            assert values["warehouse"].value == "my_warehouse"

    def test_discover_specific_key(self):
        """Should discover specific key when provided."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "my_account",
            "SNOWFLAKE_USER": "my_user",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover(key="account")

            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "my_account"

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "my_account"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover(key="nonexistent")

            assert len(values) == 0

    def test_case_conversion(self):
        """Should convert UPPERCASE env var names to lowercase config keys."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "test"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert "account" in values  # lowercase key
            assert "ACCOUNT" not in values

    def test_parse_string_value(self):
        """Should parse string values as-is."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "my_account"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert values["account"].value == "my_account"
            assert isinstance(values["account"].value, str)

    def test_parse_integer_value(self):
        """Should parse integer strings as integers."""
        with patch.dict(os.environ, {"SNOWFLAKE_PORT": "443"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert values["port"].value == 443
            assert isinstance(values["port"].value, int)

    def test_parse_boolean_true_values(self):
        """Should parse various true representations as boolean True."""
        true_values = ["true", "True", "TRUE", "1", "yes", "Yes", "on", "On"]

        for true_val in true_values:
            with patch.dict(
                os.environ, {"SNOWFLAKE_ENABLE_DIAG": true_val}, clear=True
            ):
                handler = SnowCliEnvHandler()
                values = handler.discover()

                assert values["enable_diag"].value is True, f"Failed for {true_val}"
                assert isinstance(values["enable_diag"].value, bool)

    def test_parse_boolean_false_values(self):
        """Should parse various false representations as boolean False."""
        false_values = ["false", "False", "FALSE", "0", "no", "No", "off", "Off"]

        for false_val in false_values:
            with patch.dict(
                os.environ, {"SNOWFLAKE_ENABLE_DIAG": false_val}, clear=True
            ):
                handler = SnowCliEnvHandler()
                values = handler.discover()

                assert values["enable_diag"].value is False, f"Failed for {false_val}"
                assert isinstance(values["enable_diag"].value, bool)

    def test_raw_value_preservation(self):
        """Should preserve raw string value in raw_value field."""
        with patch.dict(os.environ, {"SNOWFLAKE_PORT": "443"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            config_value = values["port"]
            assert config_value.value == 443  # Parsed as int
            assert config_value.raw_value == "443"  # Original string

    def test_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "my_account"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            config_value = values["account"]
            assert config_value.source_name == "snowflake_cli_env"
            assert config_value.priority == SourcePriority.ENVIRONMENT
            assert config_value.key == "account"

    def test_supports_any_string_key(self):
        """Should support any string key."""
        handler = SnowCliEnvHandler()

        assert handler.supports_key("account") is True
        assert handler.supports_key("user") is True
        assert handler.supports_key("any_key") is True
        assert handler.supports_key("") is True

    def test_ignores_non_snowflake_env_vars(self):
        """Should ignore environment variables without SNOWFLAKE_ prefix."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "snowflake_account",
            "SNOWSQL_ACCOUNT": "snowsql_account",
            "ACCOUNT": "plain_account",
            "PATH": "/usr/bin",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            # Should only get SNOWFLAKE_* variables
            assert len(values) == 1
            assert "account" in values
            assert values["account"].value == "snowflake_account"

    def test_empty_string_value(self):
        """Should handle empty string values."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": ""}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert values["account"].value == ""
            assert isinstance(values["account"].value, str)

    def test_special_characters_in_value(self):
        """Should handle special characters in values."""
        with patch.dict(
            os.environ,
            {"SNOWFLAKE_PASSWORD": "p@ss!w0rd#123"},
            clear=True,
        ):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert values["password"].value == "p@ss!w0rd#123"

    def test_whitespace_in_value(self):
        """Should preserve whitespace in values."""
        with patch.dict(
            os.environ,
            {"SNOWFLAKE_DESCRIPTION": "  spaced value  "},
            clear=True,
        ):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert values["description"].value == "  spaced value  "

    def test_numeric_string_not_parsed_as_int(self):
        """Should handle strings that look numeric but shouldn't be parsed."""
        # Account identifier that looks like a number
        with patch.dict(os.environ, {"SNOWFLAKE_SESSION_ID": "12345abc"}, clear=True):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            # Should remain string because "abc" makes it non-numeric
            assert values["session_id"].value == "12345abc"
            assert isinstance(values["session_id"].value, str)

    def test_underscore_in_key_preserved(self):
        """Should preserve underscores in environment variable keys."""
        with patch.dict(
            os.environ, {"SNOWFLAKE_PRIVATE_KEY_PATH": "/path/to/key"}, clear=True
        ):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            assert "private_key_path" in values
            assert values["private_key_path"].value == "/path/to/key"

    def test_multiple_discover_calls_consistent(self):
        """Multiple discover calls should return consistent results."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "my_account"}, clear=True):
            handler = SnowCliEnvHandler()

            values1 = handler.discover()
            values2 = handler.discover()

            assert values1 == values2

    def test_discover_with_mixed_case_produces_lowercase_keys(self):
        """All config keys should be lowercase regardless of env var case."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test1",
                "SNOWFLAKE_User": "test2",  # Mixed case shouldn't happen, but test anyway
            },
            clear=True,
        ):
            handler = SnowCliEnvHandler()
            values = handler.discover()

            # All keys should be lowercase
            for key in values.keys():
                assert key == key.lower()
