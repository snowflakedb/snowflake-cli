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
Unit tests for SourcePriority enum.

Tests verify:
- Enum values are correctly defined
- Priority ordering is correct (lower value = higher priority)
- Enum members have expected attributes
"""

import pytest
from snowflake.cli.api.config_ng.core import SourcePriority


class TestSourcePriority:
    """Test suite for SourcePriority enum."""

    def test_enum_members_exist(self):
        """All required enum members should exist."""
        assert hasattr(SourcePriority, "CLI_ARGUMENT")
        assert hasattr(SourcePriority, "ENVIRONMENT")
        assert hasattr(SourcePriority, "FILE")

    def test_enum_values_are_integers(self):
        """All enum values should be integers."""
        assert isinstance(SourcePriority.CLI_ARGUMENT.value, int)
        assert isinstance(SourcePriority.ENVIRONMENT.value, int)
        assert isinstance(SourcePriority.FILE.value, int)

    def test_cli_argument_has_highest_priority(self):
        """CLI_ARGUMENT should have the lowest numeric value (highest priority)."""
        assert SourcePriority.CLI_ARGUMENT.value == 1

    def test_environment_has_medium_priority(self):
        """ENVIRONMENT should have medium numeric value (medium priority)."""
        assert SourcePriority.ENVIRONMENT.value == 2

    def test_file_has_lowest_priority(self):
        """FILE should have the highest numeric value (lowest priority)."""
        assert SourcePriority.FILE.value == 3

    def test_priority_ordering(self):
        """Lower numeric value should mean higher priority."""
        assert SourcePriority.CLI_ARGUMENT.value < SourcePriority.ENVIRONMENT.value
        assert SourcePriority.ENVIRONMENT.value < SourcePriority.FILE.value

    def test_enum_comparison(self):
        """Enum members should be comparable by value."""
        priorities = [
            SourcePriority.FILE,
            SourcePriority.CLI_ARGUMENT,
            SourcePriority.ENVIRONMENT,
        ]
        sorted_priorities = sorted(priorities, key=lambda p: p.value)

        assert sorted_priorities[0] == SourcePriority.CLI_ARGUMENT
        assert sorted_priorities[1] == SourcePriority.ENVIRONMENT
        assert sorted_priorities[2] == SourcePriority.FILE

    def test_enum_equality(self):
        """Enum members should be equal to themselves."""
        assert SourcePriority.CLI_ARGUMENT == SourcePriority.CLI_ARGUMENT
        assert SourcePriority.ENVIRONMENT == SourcePriority.ENVIRONMENT
        assert SourcePriority.FILE == SourcePriority.FILE

    def test_enum_inequality(self):
        """Different enum members should not be equal."""
        assert SourcePriority.CLI_ARGUMENT != SourcePriority.ENVIRONMENT
        assert SourcePriority.ENVIRONMENT != SourcePriority.FILE
        assert SourcePriority.CLI_ARGUMENT != SourcePriority.FILE

    def test_enum_has_name_attribute(self):
        """Enum members should have a name attribute."""
        assert SourcePriority.CLI_ARGUMENT.name == "CLI_ARGUMENT"
        assert SourcePriority.ENVIRONMENT.name == "ENVIRONMENT"
        assert SourcePriority.FILE.name == "FILE"

    def test_enum_is_iterable(self):
        """Should be able to iterate over enum members."""
        members = list(SourcePriority)
        assert len(members) == 3
        assert SourcePriority.CLI_ARGUMENT in members
        assert SourcePriority.ENVIRONMENT in members
        assert SourcePriority.FILE in members

    def test_enum_can_be_accessed_by_name(self):
        """Should be able to access enum members by name."""
        assert SourcePriority["CLI_ARGUMENT"] == SourcePriority.CLI_ARGUMENT
        assert SourcePriority["ENVIRONMENT"] == SourcePriority.ENVIRONMENT
        assert SourcePriority["FILE"] == SourcePriority.FILE

    def test_enum_can_be_accessed_by_value(self):
        """Should be able to access enum members by value."""
        assert SourcePriority(1) == SourcePriority.CLI_ARGUMENT
        assert SourcePriority(2) == SourcePriority.ENVIRONMENT
        assert SourcePriority(3) == SourcePriority.FILE

    def test_invalid_value_raises_error(self):
        """Accessing enum with invalid value should raise ValueError."""
        with pytest.raises(ValueError):
            SourcePriority(99)

    def test_invalid_name_raises_error(self):
        """Accessing enum with invalid name should raise KeyError."""
        with pytest.raises(KeyError):
            SourcePriority["INVALID"]

    def test_enum_repr(self):
        """Enum members should have a readable representation."""
        assert "SourcePriority.CLI_ARGUMENT" in repr(SourcePriority.CLI_ARGUMENT)
        assert "SourcePriority.ENVIRONMENT" in repr(SourcePriority.ENVIRONMENT)
        assert "SourcePriority.FILE" in repr(SourcePriority.FILE)

    def test_enum_str(self):
        """Enum members should have a readable string representation."""
        assert "SourcePriority.CLI_ARGUMENT" in str(SourcePriority.CLI_ARGUMENT)
        assert "SourcePriority.ENVIRONMENT" in str(SourcePriority.ENVIRONMENT)
        assert "SourcePriority.FILE" in str(SourcePriority.FILE)
