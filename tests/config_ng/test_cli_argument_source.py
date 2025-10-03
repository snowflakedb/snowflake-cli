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
Unit tests for CliArgumentSource.

Tests verify:
- Highest priority source (CLI_ARGUMENT)
- Direct value discovery from CLI context
- None value filtering
- No handler support
- Source identification
"""

from snowflake.cli.api.config_ng.core import SourcePriority
from snowflake.cli.api.config_ng.sources import CliArgumentSource


class TestCliArgumentSource:
    """Test suite for CliArgumentSource."""

    def test_create_with_empty_context(self):
        """Should create source with empty context."""
        source = CliArgumentSource()

        assert source.source_name == "cli_arguments"
        assert source.priority == SourcePriority.CLI_ARGUMENT

    def test_create_with_context(self):
        """Should create source with provided context."""
        context = {"account": "my_account", "user": "my_user"}
        source = CliArgumentSource(cli_context=context)

        values = source.discover()
        assert len(values) == 2
        assert values["account"].value == "my_account"
        assert values["user"].value == "my_user"

    def test_has_highest_priority(self):
        """Should have CLI_ARGUMENT priority (highest)."""
        source = CliArgumentSource()
        assert source.priority == SourcePriority.CLI_ARGUMENT
        assert source.priority.value == 1

    def test_discover_all_values(self):
        """Should discover all non-None values when key is None."""
        context = {"account": "my_account", "user": "my_user", "port": 443}
        source = CliArgumentSource(cli_context=context)

        values = source.discover()

        assert len(values) == 3
        assert values["account"].value == "my_account"
        assert values["user"].value == "my_user"
        assert values["port"].value == 443

    def test_discover_specific_key(self):
        """Should discover specific key when provided."""
        context = {"account": "my_account", "user": "my_user"}
        source = CliArgumentSource(cli_context=context)

        values = source.discover(key="account")

        assert len(values) == 1
        assert "account" in values
        assert values["account"].value == "my_account"

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        values = source.discover(key="nonexistent")

        assert len(values) == 0

    def test_filters_none_values(self):
        """Should not include None values in discovery."""
        context = {"account": "my_account", "user": None, "password": None}
        source = CliArgumentSource(cli_context=context)

        values = source.discover()

        assert len(values) == 1
        assert "account" in values
        assert "user" not in values
        assert "password" not in values

    def test_filters_none_for_specific_key(self):
        """Should return empty dict if specific key has None value."""
        context = {"account": None}
        source = CliArgumentSource(cli_context=context)

        values = source.discover(key="account")

        assert len(values) == 0

    def test_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        values = source.discover(key="account")
        config_value = values["account"]

        assert config_value.key == "account"
        assert config_value.value == "my_account"
        assert config_value.source_name == "cli_arguments"
        assert config_value.priority == SourcePriority.CLI_ARGUMENT
        assert config_value.raw_value == "my_account"

    def test_supports_existing_key(self):
        """Should return True for keys present in context."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        assert source.supports_key("account") is True

    def test_supports_nonexistent_key(self):
        """Should return False for keys not in context."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        assert source.supports_key("nonexistent") is False

    def test_supports_key_with_none_value(self):
        """Should still support key even if value is None."""
        context = {"account": None}
        source = CliArgumentSource(cli_context=context)

        assert source.supports_key("account") is True

    def test_no_handlers(self):
        """CLI source should not have any handlers."""
        source = CliArgumentSource()

        handlers = source.get_handlers()
        assert len(handlers) == 0

    def test_discover_direct_returns_same_as_discover(self):
        """discover_direct should return same values as discover."""
        context = {"account": "my_account", "user": "my_user"}
        source = CliArgumentSource(cli_context=context)

        direct_values = source.discover_direct()
        discovered_values = source.discover()

        assert direct_values == discovered_values

    def test_handles_various_value_types(self):
        """Should handle different value types correctly."""
        context = {
            "string_val": "text",
            "int_val": 42,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "dict_val": {"key": "value"},
        }
        source = CliArgumentSource(cli_context=context)

        values = source.discover()

        assert len(values) == 5
        assert values["string_val"].value == "text"
        assert values["int_val"].value == 42
        assert values["bool_val"].value is True
        assert values["list_val"].value == [1, 2, 3]
        assert values["dict_val"].value == {"key": "value"}

    def test_empty_context_returns_empty_dict(self):
        """Empty context should return empty discovery result."""
        source = CliArgumentSource(cli_context={})

        values = source.discover()

        assert len(values) == 0

    def test_raw_value_equals_parsed_value(self):
        """For CLI arguments, raw_value should equal parsed value."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        values = source.discover(key="account")
        config_value = values["account"]

        assert config_value.raw_value == config_value.value

    def test_multiple_discover_calls_consistent(self):
        """Multiple discover calls should return consistent results."""
        context = {"account": "my_account"}
        source = CliArgumentSource(cli_context=context)

        values1 = source.discover()
        values2 = source.discover()

        assert values1 == values2
