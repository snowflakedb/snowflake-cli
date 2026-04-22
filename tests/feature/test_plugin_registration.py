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

"""Tests for plugin registration of the feature command group."""


def test_feature_group_is_registered(runner):
    """The 'snow feature' command group should be discoverable via --help."""
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output


def test_feature_group_help_lists_all_commands(runner):
    """'snow feature --help' should list key sub-commands (drop removed)."""
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "apply" in output
    assert "plan" in output
    assert "list" in output
    assert "describe" in output
    assert "convert" in output
