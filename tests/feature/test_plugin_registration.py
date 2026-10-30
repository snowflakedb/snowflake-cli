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

# Full registered ``feature`` command surface. ``export`` is intentionally
# absent: it was folded into ``init`` (init-subsumes-export plan).
_EXPECTED_SUBCOMMANDS = {
    "init",
    "sync",
    "apply",
    "plan",
    "list",
    "describe",
    "online-service",
    "ingest",
    "query",
}

_EXPECTED_ONLINE_SERVICE_SUBCOMMANDS = {"status", "create", "drop"}


def _feature_group(get_click_context):
    """Return the registered ``feature`` group off the root Click command.

    ``get_click_context()`` returns ``None`` until a command has run (the
    context is captured during command registration), so callers must invoke
    a command first to populate it.
    """
    return get_click_context().command.commands["feature"]


def test_feature_group_is_registered(runner):
    """The 'snow feature' command group should be invokable via --help."""
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output


def test_feature_group_visible_in_root_help(runner, get_click_context):
    """The ``feature`` group is GA — visible in root help by default."""
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert _feature_group(get_click_context).hidden is False


def test_feature_group_command_surface(runner, get_click_context):
    """'snow feature' registers exactly its expected sub-commands.

    Exact set equality locks the surface: adding or removing a sub-command
    fails here, and the removed standalone ``export`` command cannot reappear.
    """
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output
    feature = _feature_group(get_click_context)
    assert set(feature.commands) == _EXPECTED_SUBCOMMANDS
    assert (
        set(feature.commands["online-service"].commands)
        == _EXPECTED_ONLINE_SERVICE_SUBCOMMANDS
    )
