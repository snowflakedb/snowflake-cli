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

"""Tests for the Native App / Snowflake App Runtime flow-routing decorator and its
flow-detection helper used by the shared ``snow app`` subcommands."""

import ast
from pathlib import Path
from unittest import mock
from unittest.mock import Mock

import pytest
import snowflake.cli
from click import ClickException
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    AppFlow,
    _detect_flow_from_project,
    has_snowflake_app_entities_only,
    set_app_flow,
)
from snowflake.cli.api.cli_global_context import (
    get_cli_context,
    get_cli_context_manager,
)

from tests_common import change_directory


class _ResetAppFlowMixin:
    """Reset ``app_flow`` on the CLI context before/after each test so the
    autouse fixtures don't leak state across tests in the same process."""

    @pytest.fixture(autouse=True)
    def _reset_app_flow(self):
        get_cli_context_manager().app_flow = None
        yield
        get_cli_context_manager().app_flow = None


def _make_project(entity_types_by_id):
    """Build a fake project definition whose ``entities`` dict maps id -> Mock
    with the given ``type`` attribute."""
    project = Mock()
    entities = {}
    for entity_id, entity_type in entity_types_by_id.items():
        entity = Mock()
        entity.type = entity_type
        entities[entity_id] = entity
    project.entities = entities
    return project


# ── _detect_flow_from_project -----------------------------------------


class TestDetectFlowFromProject:
    def test_package_entity_id_routes_native(self):
        project = _make_project(
            {"pkg": "application package", "my_app": "snowflake-app"}
        )
        flow = _detect_flow_from_project(
            project, entity_id="", package_entity_id="pkg", app_entity_id=""
        )
        assert flow == AppFlow.NATIVE_APP

    def test_app_entity_id_routes_native(self):
        project = _make_project(
            {"native_app": "application", "my_app": "snowflake-app"}
        )
        flow = _detect_flow_from_project(
            project, entity_id="", package_entity_id="", app_entity_id="native_app"
        )
        assert flow == AppFlow.NATIVE_APP

    def test_entity_id_pointing_to_native_routes_native(self):
        project = _make_project(
            {"pkg": "application package", "native_app": "application"}
        )
        flow = _detect_flow_from_project(
            project,
            entity_id="native_app",
            package_entity_id="",
            app_entity_id="",
        )
        assert flow == AppFlow.NATIVE_APP

    def test_entity_id_pointing_to_snowflake_app_routes_snowflake(self):
        project = _make_project(
            {"pkg": "application package", "my_app": "snowflake-app"}
        )
        flow = _detect_flow_from_project(
            project,
            entity_id="my_app",
            package_entity_id="",
            app_entity_id="",
        )
        assert flow == AppFlow.SNOWFLAKE_APP

    def test_entity_id_unknown_falls_back_to_scanning_snowflake_only(self):
        # Mistyped --entity-id in a snowflake-app-only project must still
        # route to SNOWFLAKE_APP so the per-flow handler produces the
        # specific "entity X not found" error in the right flow.
        project = _make_project({"my_app": "snowflake-app"})
        flow = _detect_flow_from_project(
            project,
            entity_id="missing",
            package_entity_id="",
            app_entity_id="",
        )
        assert flow == AppFlow.SNOWFLAKE_APP

    def test_entity_id_unknown_falls_back_to_scanning_native_only(self):
        # Symmetric case: mistyped --entity-id in a native-app-only project
        # stays in the NATIVE_APP flow.
        project = _make_project(
            {"pkg": "application package", "native_app": "application"}
        )
        flow = _detect_flow_from_project(
            project,
            entity_id="missing",
            package_entity_id="",
            app_entity_id="",
        )
        assert flow == AppFlow.NATIVE_APP

    def test_entity_id_unknown_in_mixed_project_errors(self):
        # When both flow types exist and the user's --entity-id is wrong,
        # we cannot pick a flow by scan alone -- ask the user to disambiguate.
        project = _make_project(
            {"pkg": "application package", "my_app": "snowflake-app"}
        )
        with pytest.raises(ClickException, match="both Native App"):
            _detect_flow_from_project(
                project,
                entity_id="missing",
                package_entity_id="",
                app_entity_id="",
            )

    def test_only_snowflake_app_entities_routes_snowflake(self):
        project = _make_project({"my_app": "snowflake-app"})
        flow = _detect_flow_from_project(
            project, entity_id="", package_entity_id="", app_entity_id=""
        )
        assert flow == AppFlow.SNOWFLAKE_APP

    def test_only_native_app_entities_routes_native(self):
        project = _make_project(
            {"pkg": "application package", "native_app": "application"}
        )
        flow = _detect_flow_from_project(
            project, entity_id="", package_entity_id="", app_entity_id=""
        )
        assert flow == AppFlow.NATIVE_APP

    def test_mixed_entities_without_id_errors(self):
        project = _make_project(
            {"pkg": "application package", "my_app": "snowflake-app"}
        )
        with pytest.raises(ClickException, match="both Native App"):
            _detect_flow_from_project(
                project, entity_id="", package_entity_id="", app_entity_id=""
            )

    def test_unrelated_entity_type_errors_on_lookup(self):
        project = _make_project({"stream": "streamlit"})
        with pytest.raises(ClickException, match="not supported by 'snow app'"):
            _detect_flow_from_project(
                project,
                entity_id="stream",
                package_entity_id="",
                app_entity_id="",
            )

    def test_empty_project_defaults_to_native(self):
        project = _make_project({})
        flow = _detect_flow_from_project(
            project, entity_id="", package_entity_id="", app_entity_id=""
        )
        assert flow == AppFlow.NATIVE_APP


# ── has_snowflake_app_entities_only -----------------------------------


class TestHasSnowflakeAppEntitiesOnly:
    def test_none_project_returns_false(self):
        assert has_snowflake_app_entities_only(None) is False

    def test_only_snowflake_app_returns_true(self):
        project = _make_project({"my_app": "snowflake-app"})
        assert has_snowflake_app_entities_only(project) is True

    def test_only_native_app_returns_false(self):
        project = _make_project({"pkg": "application package"})
        assert has_snowflake_app_entities_only(project) is False

    def test_mixed_returns_false(self):
        project = _make_project(
            {"pkg": "application package", "my_app": "snowflake-app"}
        )
        assert has_snowflake_app_entities_only(project) is False

    def test_empty_returns_false(self):
        project = _make_project({})
        assert has_snowflake_app_entities_only(project) is False


# ── CLI-level cross-flow option validation ----------------------------


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

_NATIVE_APP_YML = """\
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


class TestCrossFlowOptionValidation:
    """The merged ``snow app`` handlers must raise a clear error when the user
    passes options that don't apply to the detected flow."""

    def _write_yml(self, tmp_path, content):
        (tmp_path / "snowflake.yml").write_text(content)

    def test_snowflake_app_rejects_native_app_deploy_options(self, runner, tmp_path):
        self._write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--prune"])

        assert result.exit_code != 0
        assert "Snowflake App Runtime entity" in result.output
        assert "--prune" in result.output

    def test_native_app_rejects_snowflake_app_deploy_options(self, runner, tmp_path):
        self._write_yml(tmp_path, _NATIVE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--upload-only"])

        assert result.exit_code != 0
        assert "Native App entity" in result.output
        assert "--upload-only" in result.output

    def test_snowflake_app_rejects_native_app_events_options(self, runner, tmp_path):
        self._write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "events", "--follow"])

        assert result.exit_code != 0
        assert "Snowflake App Runtime entity" in result.output
        assert "--follow" in result.output

    def test_snowflake_app_rejects_explicit_native_app_follow_interval(
        self, runner, tmp_path
    ):
        # Regression: ``--follow-interval 10`` matched the old Typer default
        # of 10, so the sentinel check treated it as "not set" and the
        # Native-App-only option was silently accepted. Now that the option
        # defaults to None, an explicit value of any kind is rejected.
        self._write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "events", "--follow-interval", "10"])

        assert result.exit_code != 0
        assert "Snowflake App Runtime entity" in result.output
        assert "--follow-interval" in result.output


class TestSetAppFlow(_ResetAppFlowMixin):
    """``set_app_flow`` records the resolved product flow on the CLI context
    so telemetry can attribute commands without inspecting kwargs."""

    def test_set_native_app(self):
        set_app_flow(AppFlow.NATIVE_APP)
        assert get_cli_context().app_flow == "native_app"

    def test_set_snowflake_app(self):
        set_app_flow(AppFlow.SNOWFLAKE_APP)
        assert get_cli_context().app_flow == "snowflake_app"

    def test_set_app_flow_is_overwritable(self):
        set_app_flow(AppFlow.NATIVE_APP)
        set_app_flow(AppFlow.SNOWFLAKE_APP)
        assert get_cli_context().app_flow == "snowflake_app"


class TestAppFlowRoutingPropagatesToContext(_ResetAppFlowMixin):
    """The routing decorators must record the flow on the CLI context for
    telemetry, even when the command later raises (e.g. cross-flow option
    validation)."""

    def _write_yml(self, tmp_path, content):
        (tmp_path / "snowflake.yml").write_text(content)

    def test_snowflake_app_project_sets_context_flow(self, runner, tmp_path):
        self._write_yml(tmp_path, _SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            # Triggering the routing decorator is enough; we don't need the
            # command to succeed (it will error on cross-flow option).
            runner.invoke(["app", "deploy", "--prune"])

        assert get_cli_context().app_flow == "snowflake_app"

    def test_native_app_project_sets_context_flow(self, runner, tmp_path):
        self._write_yml(tmp_path, _NATIVE_APP_YML)

        with change_directory(tmp_path):
            runner.invoke(["app", "deploy", "--upload-only"])

        assert get_cli_context().app_flow == "native_app"


class TestNativeAppOnlyGuards:
    """Commands like ``run`` / ``publish`` / ``diff`` that only make sense for
    Native App projects must produce a clear error when the project contains
    only ``snowflake-app`` entities."""

    def test_run_on_snowflake_app_project_errors(self, runner, tmp_path):
        (tmp_path / "snowflake.yml").write_text(_SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "run"])

        assert result.exit_code != 0
        assert "only available for Native App" in result.output

    def test_publish_on_snowflake_app_project_errors(self, runner, tmp_path):
        (tmp_path / "snowflake.yml").write_text(_SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "publish"])

        assert result.exit_code != 0
        assert "only available for Native App" in result.output

    def test_diff_on_snowflake_app_project_errors(self, runner, tmp_path):
        (tmp_path / "snowflake.yml").write_text(_SNOWFLAKE_APP_YML)

        with change_directory(tmp_path):
            result = runner.invoke(["app", "diff"])

        assert result.exit_code != 0
        assert "only available for Native App" in result.output


# ── force_project_definition_v2 caller allowlist ─────────────────────


def _find_force_v2_callers(src_root: Path) -> set[tuple[str, str]]:
    """Walk *src_root* recursively and return the set of
    ``(relative_path, function_name)`` tuples for every function decorated
    with ``force_project_definition_v2`` (with or without arguments).

    Uses ``ast`` rather than text search so e.g. commented-out usages or
    string occurrences don't false-positive.
    """
    callers: set[tuple[str, str]] = set()
    for path in src_root.rglob("*.py"):
        # Force UTF-8: Path.read_text() defaults to the platform encoding,
        # which is cp1252 on Windows and trips on UTF-8 source files (e.g.
        # ones containing em-dashes or smart quotes).
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                # Match either @force_project_definition_v2 or
                # @force_project_definition_v2(...).
                target = (
                    decorator.func if isinstance(decorator, ast.Call) else decorator
                )
                if (
                    isinstance(target, ast.Name)
                    and target.id == "force_project_definition_v2"
                ):
                    rel = path.relative_to(src_root).as_posix()
                    callers.add((rel, node.name))
    return callers


class TestForceProjectDefinitionV2Callers:
    """``force_project_definition_v2`` stamps ``app_flow=native_app`` on
    the CLI context (after entity resolution succeeds) on the assumption
    that every caller is a Native-App-only command. Lock that assumption
    in: any new caller MUST be added to ``EXPECTED_CALLERS`` below after
    confirming the command is in fact a Native App command (operates on
    ``application`` / ``application package`` entities).

    If a future command needs ``force_project_definition_v2`` but is NOT
    Native App (e.g. a shared command, or a Snowflake App Runtime
    command), use a different decorator -- this one would silently
    misattribute its telemetry.
    """

    EXPECTED_CALLERS: frozenset[tuple[str, str]] = frozenset(
        {
            ("snowflake/cli/_plugins/nativeapp/commands.py", "app_diff"),
            ("snowflake/cli/_plugins/nativeapp/commands.py", "app_run"),
            ("snowflake/cli/_plugins/nativeapp/commands.py", "app_publish"),
            ("snowflake/cli/_plugins/nativeapp/version/commands.py", "create"),
            ("snowflake/cli/_plugins/nativeapp/version/commands.py", "drop"),
            ("snowflake/cli/_plugins/nativeapp/version/commands.py", "version_list"),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_list",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_add_accounts",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_remove_accounts",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_set_accounts",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_add_version",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_channel/commands.py",
                "release_channel_remove_version",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_directive/commands.py",
                "release_directive_list",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_directive/commands.py",
                "release_directive_set",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_directive/commands.py",
                "release_directive_unset",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_directive/commands.py",
                "release_directive_add_accounts",
            ),
            (
                "snowflake/cli/_plugins/nativeapp/release_directive/commands.py",
                "release_directive_remove_accounts",
            ),
        }
    )

    def test_callers_match_expected_set(self):
        # Resolve src/ from the snowflake.cli package location:
        # <repo>/src/snowflake/cli/__init__.py -> <repo>/src
        src_root = Path(snowflake.cli.__file__).resolve().parents[2]
        actual = _find_force_v2_callers(src_root)

        unexpected = actual - self.EXPECTED_CALLERS
        missing = self.EXPECTED_CALLERS - actual

        assert not unexpected, (
            f"force_project_definition_v2 was applied to a new function: "
            f"{sorted(unexpected)}. If the new caller is a Native App "
            f"command (operates on application / application package "
            f"entities), add it to EXPECTED_CALLERS. If it's a Snowflake "
            f"Apps Deploy or shared command, use a different decorator -- "
            f"this one stamps app_flow=native_app for telemetry."
        )
        assert not missing, (
            f"Expected callers no longer present: {sorted(missing)}. "
            f"Update EXPECTED_CALLERS to remove them."
        )


class TestAppGroupDefinitionEncoding:
    """The ``snow app`` group callback defaults the project-definition encoding
    to UTF-8, but lets an explicit ``cli.encoding.file_io`` setting win."""

    @pytest.fixture(autouse=True)
    def _reset_encoding(self):
        get_cli_context_manager().project_definition_encoding = None
        yield
        get_cli_context_manager().project_definition_encoding = None

    def test_defaults_to_utf8_when_file_io_unset(self):
        from snowflake.cli._plugins.nativeapp.commands import _app_group_callback

        with mock.patch(
            "snowflake.cli._plugins.nativeapp.commands.get_file_io_encoding",
            return_value=None,
        ):
            _app_group_callback()

        assert get_cli_context_manager().project_definition_encoding == "utf-8"

    def test_file_io_setting_takes_precedence(self):
        from snowflake.cli._plugins.nativeapp.commands import _app_group_callback

        with mock.patch(
            "snowflake.cli._plugins.nativeapp.commands.get_file_io_encoding",
            return_value="cp1252",
        ):
            _app_group_callback()

        assert get_cli_context_manager().project_definition_encoding == "cp1252"
