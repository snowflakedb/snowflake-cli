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
"""

from snowflake.cli.api.config_ng.core import SourcePriority


class TestSourcePriority:
    """Test suite for SourcePriority enum."""

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
