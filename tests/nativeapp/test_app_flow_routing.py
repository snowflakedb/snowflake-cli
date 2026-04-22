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

"""Tests for the Native App / Snowflake Apps Deploy flow-routing decorator and its
flow-detection helper used by the shared ``snow app`` subcommands."""

from unittest.mock import Mock

import pytest
from click import ClickException
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    AppFlow,
    _detect_flow_from_project,
    has_snowflake_app_entities_only,
)

from tests_common import change_directory


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
        assert "Snowflake Apps Deploy entity" in result.output
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
        assert "Snowflake Apps Deploy entity" in result.output
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
        assert "Snowflake Apps Deploy entity" in result.output
        assert "--follow-interval" in result.output


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
