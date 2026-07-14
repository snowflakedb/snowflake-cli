# Copyright (c) 2026 Snowflake Inc.
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

"""Tests for the ``snow app`` "smart help" feature: ``_detect_app_family`` and
``SmartAppGroup``. Smart help hides the *other* app family's commands from the
``snow app --help`` listing when a project unambiguously belongs to one family,
while keeping every command fully runnable (help-only filtering)."""

import re

import pytest
from snowflake.cli._plugins.nativeapp.commands import (
    COMMON_PANEL,
    NATIVE_APP_PANEL,
    SNOWFLAKE_APP_PANEL,
    _detect_app_family,
)
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import AppFlow

from tests_common import change_directory

# ── minimal but valid snowflake.yml fixtures ─────────────────────────

_SNOWFLAKE_APP_YML = """\
definition_version: '2'
entities:
  my_app:
    type: snowflake-app
    identifier: my_app
    artifacts:
      - src: "*"
        dest: ./
"""

_NATIVE_APP_V2_YML = """\
definition_version: '2'
entities:
  pkg:
    type: "application package"
    identifier: my_pkg
    artifacts:
      - src: "*"
        dest: ./
    manifest: ./manifest.yml
  native_app:
    type: application
    identifier: my_app
    from:
      target: pkg
"""

_NATIVE_APP_V1_YML = """\
definition_version: 1
native_app:
  name: myapp
  artifacts:
    - src: "*"
      dest: ./
"""

_MIXED_YML = """\
definition_version: '2'
entities:
  pkg:
    type: "application package"
    identifier: my_pkg
    artifacts:
      - src: "*"
        dest: ./
    manifest: ./manifest.yml
  my_app:
    type: snowflake-app
    identifier: my_app
    artifacts:
      - src: "*"
        dest: ./
"""

_MALFORMED_YML = """\
definition_version: '2'
entities:
  my_app:
    type: snowflake-app
   identifier: broken-indent
      - not valid: [
"""

# A ``type`` with odd casing / surrounding whitespace: detection normalizes
# with ``.strip().lower()`` so this must still resolve to the snowflake_app family.
_SNOWFLAKE_APP_MESSY_CASE_YML = """\
definition_version: '2'
entities:
  my_app:
    type: "  SNOWFLAKE-App  "
    identifier: my_app
    artifacts:
      - src: "*"
        dest: ./
"""

# Base project (native) plus a snowflake.local.yml overlay that adds a
# snowflake-app entity, making the merged definition ambiguous -> None.
_NATIVE_BASE_FOR_LOCAL_YML = """\
definition_version: '2'
entities:
  pkg:
    type: "application package"
    identifier: my_pkg
    artifacts:
      - src: "*"
        dest: ./
    manifest: ./manifest.yml
"""

_LOCAL_OVERLAY_ADDS_SNOWFLAKE_APP_YML = """\
entities:
  my_app:
    type: snowflake-app
    identifier: my_app
    artifacts:
      - src: "*"
        dest: ./
"""


def _write_yml(tmp_path, content):
    (tmp_path / "snowflake.yml").write_text(content)


# ── _detect_app_family ────────────────────────────────────────────────


class TestDetectAppFamily:
    @pytest.mark.parametrize(
        "yml, expected",
        [
            pytest.param(_NATIVE_APP_V2_YML, AppFlow.NATIVE_APP, id="v2_native"),
            pytest.param(
                _SNOWFLAKE_APP_YML, AppFlow.SNOWFLAKE_APP, id="v2_snowflake_app"
            ),
            pytest.param(_NATIVE_APP_V1_YML, AppFlow.NATIVE_APP, id="v1_native"),
            pytest.param(
                _SNOWFLAKE_APP_MESSY_CASE_YML,
                AppFlow.SNOWFLAKE_APP,
                id="messy_case_and_whitespace",
            ),
            pytest.param(_MIXED_YML, None, id="mixed_is_unknown"),
            pytest.param(_MALFORMED_YML, None, id="malformed_is_unknown"),
        ],
    )
    def test_detects_family(self, tmp_path, yml, expected):
        _write_yml(tmp_path, yml)
        assert _detect_app_family(str(tmp_path)) == expected

    def test_no_project_is_unknown(self, tmp_path):
        # No snowflake.yml anywhere up the tree from tmp_path.
        assert _detect_app_family(str(tmp_path)) is None

    def test_detection_walks_up_to_project_root(self, tmp_path):
        # snowflake.yml at the root, detection called from a nested subdir.
        _write_yml(tmp_path, _SNOWFLAKE_APP_YML)
        nested = tmp_path / "src" / "deep"
        nested.mkdir(parents=True)
        assert _detect_app_family(str(nested)) == AppFlow.SNOWFLAKE_APP

    def test_local_overlay_can_make_family_ambiguous(self, tmp_path):
        # A native-only snowflake.yml plus a snowflake.local.yml that introduces
        # a snowflake-app entity must merge to an ambiguous (None) result --
        # proving detection reads the merged definition, not just the base file.
        _write_yml(tmp_path, _NATIVE_BASE_FOR_LOCAL_YML)
        (tmp_path / "snowflake.local.yml").write_text(
            _LOCAL_OVERLAY_ADDS_SNOWFLAKE_APP_YML
        )
        assert _detect_app_family(str(tmp_path)) is None


# ── SmartAppGroup: help filtering ─────────────────────────────────────

# Command / subgroup names that live on each family's help panel.
_NATIVE_ONLY_COMMANDS = [
    "run",
    "publish",
    "version",
    "release-directive",
    "release-channel",
]
_SNOWFLAKE_ONLY_COMMANDS = ["setup"]


def _lists_command(output: str, name: str) -> bool:
    """True if ``name`` appears as a standalone command token in help output.

    Uses word boundaries so that e.g. ``run`` does not spuriously match inside
    ``Runtime`` (the SNOWFLAKE_APP_PANEL title) or ``release-directive``.
    """
    return re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", output) is not None


class TestSmartAppHelp:
    def test_native_project_hides_snowflake_app_panel(self, runner, tmp_path):
        _write_yml(tmp_path, _NATIVE_APP_V2_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "--help"])

        assert result.exit_code == 0, result.output
        assert COMMON_PANEL in result.output
        assert NATIVE_APP_PANEL in result.output
        assert SNOWFLAKE_APP_PANEL not in result.output
        for name in _SNOWFLAKE_ONLY_COMMANDS:
            assert not _lists_command(result.output, name), f"{name} should be hidden"

    def test_snowflake_app_project_hides_native_app_panel(self, runner, tmp_path):
        _write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "--help"])

        assert result.exit_code == 0, result.output
        assert COMMON_PANEL in result.output
        assert SNOWFLAKE_APP_PANEL in result.output
        assert NATIVE_APP_PANEL not in result.output
        for name in _SNOWFLAKE_ONLY_COMMANDS:
            assert _lists_command(result.output, name), f"{name} should be listed"
        for name in _NATIVE_ONLY_COMMANDS:
            assert not _lists_command(result.output, name), f"{name} should be hidden"

    def test_no_project_shows_all_panels(self, runner, tmp_path):
        # No snowflake.yml -> family unknown -> nothing hidden.
        with change_directory(tmp_path):
            result = runner.invoke(["app", "--help"])

        assert result.exit_code == 0, result.output
        assert COMMON_PANEL in result.output
        assert NATIVE_APP_PANEL in result.output
        assert SNOWFLAKE_APP_PANEL in result.output
        for name in _SNOWFLAKE_ONLY_COMMANDS + _NATIVE_ONLY_COMMANDS:
            assert _lists_command(result.output, name), f"{name} should be listed"

    def test_mixed_project_shows_all_panels(self, runner, tmp_path):
        # Mixed family -> unknown -> nothing hidden.
        _write_yml(tmp_path, _MIXED_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "--help"])

        assert result.exit_code == 0, result.output
        assert COMMON_PANEL in result.output
        assert NATIVE_APP_PANEL in result.output
        assert SNOWFLAKE_APP_PANEL in result.output


# ── runnable-when-hidden invariant ────────────────────────────────────


class TestHiddenCommandsStillRunnable:
    def test_run_help_works_even_when_hidden(self, runner, tmp_path):
        # In a snowflake_app project, ``run`` is hidden from the group help,
        # but Click dispatch uses get_command (not list_commands), so
        # ``snow app run --help`` must still resolve and print usage.
        _write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            help_result = runner.invoke(["app", "--help"])
            run_help = runner.invoke(["app", "run", "--help"])

        # Confirm run really is hidden from the group listing first.
        assert not _lists_command(help_result.output, "run")
        # ...yet still resolvable / runnable.
        assert run_help.exit_code == 0, run_help.output
        assert "Usage" in run_help.output
        assert _lists_command(run_help.output, "run")

    def test_hidden_command_is_dispatched_not_rejected(self, runner, tmp_path):
        # Invoking a hidden command must dispatch to the command itself (which
        # then errors via the native_app_only guard), NOT fail with a Click
        # "No such command" error. This proves get_command still resolves it.
        _write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            help_result = runner.invoke(["app", "--help"])
            run_result = runner.invoke(["app", "run"])

        assert not _lists_command(help_result.output, "run")
        assert result_is_dispatched(run_result)


def result_is_dispatched(run_result) -> bool:
    """The ``run`` command resolved and executed (reaching its native-app-only
    guard) rather than being rejected by Click as an unknown command."""
    assert "No such command" not in run_result.output, run_result.output
    assert "only available for Native App" in run_result.output, run_result.output
    return True
