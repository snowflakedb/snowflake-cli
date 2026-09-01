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

from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags


def _lists_feature_group(output: str) -> bool:
    """True when the root ``--help`` output lists the ``feature`` group.

    Root help renders commands inside a Rich box, so each row is prefixed
    with the ``|`` border; strip it before matching the command name.
    """
    return any(line.lstrip("| ").startswith("feature") for line in output.splitlines())


def test_feature_group_is_registered(runner):
    """The 'snow feature' command group should be invokable via --help.

    The group is hidden from the root help by default (public-preview
    lifecycle), but it stays registered and directly invokable so
    opted-in users and tests can still reach it.
    """
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output


def test_feature_group_hidden_from_root_help_by_default(runner):
    """Without the feature flag, ``snow --help`` must not list ``feature``."""
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert not _lists_feature_group(result.output), result.output


@with_feature_flags({FeatureFlag.ENABLE_FEATURE_STORE: True})
def test_feature_group_visible_in_root_help_when_flag_enabled(runner):
    """With ENABLE_FEATURE_STORE on, ``snow --help`` lists ``feature``."""
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert _lists_feature_group(result.output), result.output


def test_feature_group_help_lists_all_commands(runner):
    """'snow feature --help' should list key sub-commands.

    The standalone ``export`` command is gone — its functionality is
    now part of ``init`` (init-subsumes-export plan).
    """
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "apply" in output
    assert "plan" in output
    assert "list" in output
    assert "describe" in output
    assert "init" in output
    # ``export`` is no longer a standalone subcommand; the export
    # pipeline runs as part of ``init`` instead.
    lines = [line.strip() for line in result.output.splitlines()]
    assert not any(line.startswith("export ") or line == "export" for line in lines)
