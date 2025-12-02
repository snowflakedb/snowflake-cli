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
Unit tests for ConfigValue dataclass.

Tests verify:
- Field values and types
- Raw value preservation
- Type conversions
- Representation formatting
"""

from snowflake.cli.api.config_ng.core import ConfigValue, ValueSource


class TestConfigValue:
    """Test suite for ConfigValue dataclass."""

    def test_create_basic_config_value(self):
        """Should create a basic ConfigValue with required fields."""
        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name=ValueSource.SourceName.CLI_ARGUMENTS,
        )

        assert cv.key == "account"
        assert cv.value == "my_account"
        assert cv.source_name == "cli_arguments"
        assert cv.raw_value is None

    def test_create_config_value_with_raw_value(self):
        """Should create ConfigValue with raw value preservation."""
        cv = ConfigValue(
            key="port",
            value=443,
            source_name=ValueSource.SourceName.CLI_ENV,
            raw_value="443",
        )

        assert cv.key == "port"
        assert cv.value == 443
        assert cv.raw_value == "443"
        assert isinstance(cv.value, int)
        assert isinstance(cv.raw_value, str)

    def test_repr_without_conversion(self):
        """__repr__ should show value only when no conversion occurred."""
        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name=ValueSource.SourceName.CLI_ARGUMENTS,
        )

        repr_str = repr(cv)
        assert "account=my_account" in repr_str
        assert "cli_arguments" in repr_str
        assert "→" not in repr_str

    def test_repr_with_conversion(self):
        """__repr__ should show conversion when raw_value differs from value."""
        cv = ConfigValue(
            key="port",
            value=443,
            source_name=ValueSource.SourceName.CLI_ENV,
            raw_value="443",
        )

        repr_str = repr(cv)
        assert "port" in repr_str
        assert "443" in repr_str
        assert "→" in repr_str
        assert "cli_env" in repr_str

    def test_repr_with_same_raw_and_parsed_value(self):
        """__repr__ should not show conversion when values are the same."""
        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name=ValueSource.SourceName.CLI_ARGUMENTS,
            raw_value="my_account",
        )

        repr_str = repr(cv)
        assert "→" not in repr_str

    def test_boolean_conversion_example(self):
        """Should handle boolean conversion from string."""
        cv = ConfigValue(
            key="enable_diag",
            value=True,
            source_name=ValueSource.SourceName.CLI_ENV,
            raw_value="true",
        )

        assert cv.value is True
        assert cv.raw_value == "true"
        assert isinstance(cv.value, bool)
        assert isinstance(cv.raw_value, str)

    def test_integer_conversion_example(self):
        """Should handle integer conversion from string."""
        cv = ConfigValue(
            key="timeout",
            value=30,
            source_name=ValueSource.SourceName.CLI_ENV,
            raw_value="30",
        )

        assert cv.value == 30
        assert cv.raw_value == "30"
        assert isinstance(cv.value, int)
        assert isinstance(cv.raw_value, str)

    def test_snowsql_key_mapping_example(self):
        """Should preserve original SnowSQL key in raw_value."""
        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name=ValueSource.SourceName.SNOWSQL_CONFIG,
            raw_value="accountname=my_account",
        )

        assert cv.key == "account"
        assert cv.value == "my_account"
        assert cv.raw_value == "accountname=my_account"

    def test_none_value(self):
        """Should handle None as a value."""
        cv = ConfigValue(
            key="optional_field",
            value=None,
            source_name=ValueSource.SourceName.CLI_ARGUMENTS,
        )

        assert cv.value is None
        assert cv.key == "optional_field"

    def test_complex_value_types(self):
        """Should handle complex value types like lists and dicts."""
        cv_list = ConfigValue(
            key="tags",
            value=["tag1", "tag2"],
            source_name=ValueSource.SourceName.CONNECTIONS_TOML,
        )

        cv_dict = ConfigValue(
            key="metadata",
            value={"key1": "value1", "key2": "value2"},
            source_name=ValueSource.SourceName.CONNECTIONS_TOML,
        )

        assert cv_list.value == ["tag1", "tag2"]
        assert cv_dict.value == {"key1": "value1", "key2": "value2"}

    def test_different_source_names(self):
        """Should work with different source names."""
        cv_cli = ConfigValue(
            key="account",
            value="cli_account",
            source_name=ValueSource.SourceName.CLI_ARGUMENTS,
        )

        cv_env = ConfigValue(
            key="account",
            value="env_account",
            source_name=ValueSource.SourceName.CLI_ENV,
        )

        cv_file = ConfigValue(
            key="account",
            value="file_account",
            source_name=ValueSource.SourceName.CONNECTIONS_TOML,
        )

        assert cv_cli.source_name is ValueSource.SourceName.CLI_ARGUMENTS
        assert cv_env.source_name is ValueSource.SourceName.CLI_ENV
        assert cv_file.source_name is ValueSource.SourceName.CONNECTIONS_TOML
