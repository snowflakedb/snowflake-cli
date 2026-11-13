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

"""Tests for the app.yml ``runtime`` / ``streamlit`` keys."""

from textwrap import dedent
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._app.telemetry import CLITelemetryField, _get_app_yml_telemetry
from snowflake.cli._plugins.apps.app_yml import (
    APP_YML_FILENAME,
    AppYmlDefinition,
    load_app_yml,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError

_COMMANDS = "snowflake.cli._plugins.apps.commands"


def _definition(**overrides) -> AppYmlDefinition:
    base = dict(
        version=2, name="MY_APP", database="DB", schema="PUBLIC", query_warehouse="WH"
    )
    base.update(overrides)
    return AppYmlDefinition(**base)


_PY_INSTALL = {"install": {"commands": [["uv", "sync"]]}}


class TestRuntimeAndStreamlitKeys:
    def test_defaults_are_node_and_not_streamlit(self):
        d = _definition()
        assert d.runtime == "node" and d.streamlit is False
        assert d.is_python_runtime is False

    def test_python_commands_do_not_change_the_default(self):
        d = _definition(**_PY_INSTALL)
        assert d.runtime == "node" and d.is_python_runtime is False

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("python", "python"),
            ("PYTHON", "python"),
            (" Node ", "node"),
            ("node", "node"),
        ],
    )
    def test_runtime_normalized_case_insensitively(self, value, expected):
        assert _definition(runtime=value).runtime == expected

    @pytest.mark.parametrize("value", [None, "null"])
    def test_runtime_null_is_the_default(self, value):
        assert _definition(runtime=value).runtime == "node"

    @pytest.mark.parametrize("value", ["java", "", "pythonn", 3, True, ["python"]])
    def test_runtime_invalid(self, value):
        with pytest.raises(ValueError, match="runtime must be"):
            _definition(runtime=value)

    @pytest.mark.parametrize("value", ["yes", "true", 1, 0, "false", []])
    def test_streamlit_rejects_non_bool(self, value):
        with pytest.raises(ValueError, match="streamlit must be a boolean"):
            _definition(streamlit=value)

    @pytest.mark.parametrize("value, expected", [(False, False), (None, False)])
    def test_streamlit_accepts_bool_and_null(self, value, expected):
        assert _definition(streamlit=value).streamlit is expected

    def test_streamlit_true_with_python(self):
        d = _definition(streamlit=True, runtime="python")
        assert d.streamlit is True and d.is_python_runtime is True

    @pytest.mark.parametrize("runtime", ["NODE", None])
    def test_streamlit_requires_python(self, runtime):
        with pytest.raises(ValueError, match="requires 'runtime: python'"):
            _definition(streamlit=True, runtime=runtime)

    def test_streamlit_false_with_node_is_fine(self):
        assert _definition(streamlit=False, runtime="node").is_python_runtime is False

    def test_target_level_keys_are_ignored_like_builder_phases(self):
        d = _definition(
            targets={"dev": {"runtime": "python", "streamlit": True, "install": {}}}
        )
        assert not hasattr(d.targets["dev"], "runtime")
        assert d.is_python_runtime is False


class TestGateWithDeclaredRuntime:
    @pytest.mark.parametrize(
        "kwargs",
        [{"runtime": "python"}, {"runtime": "python", "streamlit": True}],
    )
    def test_error_message_names_the_reason(self, kwargs):
        from snowflake.cli._plugins.apps.commands import _ensure_python_runtime_enabled

        manager = Mock()
        manager.python_runtime_enabled.return_value = False
        with pytest.raises(CliError, match="not enabled") as exc:
            _ensure_python_runtime_enabled(manager, _definition(**kwargs), "SERVERLESS")
        assert "sets `runtime: python`" in str(exc.value)

    @pytest.mark.parametrize("kwargs", [{"runtime": "node"}, {}])
    def test_node_skips_gate_despite_python_commands(self, kwargs):
        from snowflake.cli._plugins.apps.commands import _ensure_python_runtime_enabled

        manager = Mock()
        _ensure_python_runtime_enabled(
            manager, _definition(**kwargs, **_PY_INSTALL), "SERVERLESS"
        )
        manager.python_runtime_enabled.assert_not_called()

    def test_declared_python_hits_gate_without_python_commands(self):
        from snowflake.cli._plugins.apps.commands import _ensure_python_runtime_enabled

        manager = Mock()
        manager.python_runtime_enabled.return_value = True
        _ensure_python_runtime_enabled(
            manager, _definition(runtime="python"), "SERVERLESS"
        )
        manager.python_runtime_enabled.assert_called_once_with("SERVERLESS")


_HEADER = (
    "# created-by: snowflake-apps-streamlit; template=streamlit-template; "
    "skill-version=1.0.0\n"
)
_V2_BODY = "version: 2\nname: A\ndatabase: D\nschema: S\nquery_warehouse: W\n"


class TestLoadAppYml:
    def test_valid_declared_keys_load(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text(
            _HEADER + _V2_BODY + "runtime: Python\nstreamlit: true\n"
        )
        d = load_app_yml(tmp_path)
        assert d.runtime == "python" and d.streamlit is True

    @pytest.mark.parametrize(
        "extra, message",
        [
            ("runtime: ruby\n", "runtime must be"),
            ("streamlit: yes-please\n", "streamlit must be a boolean"),
            ("runtime: node\nstreamlit: true\n", "requires 'runtime: python'"),
            ("streamlit: true\n", "requires 'runtime: python'"),
        ],
    )
    def test_invalid_values_raise_cli_error(self, tmp_path, extra, message):
        (tmp_path / APP_YML_FILENAME).write_text(_V2_BODY + extra)
        with pytest.raises(CliError, match=f"(?s)Invalid app.yml.*{message}"):
            load_app_yml(tmp_path)

    def test_versionless_bad_values_never_raise(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text(
            "runtime: ruby\nstreamlit: maybe\ninstall: 3\n"
        )
        assert load_app_yml(tmp_path) is None


@pytest.fixture
def clean_context():
    manager = get_cli_context_manager()
    manager.app_yml_info = None
    yield manager
    manager.app_yml_info = None


class TestRecordedInfoAndTelemetry:
    def test_v2_streamlit_info(self, tmp_path, clean_context):
        (tmp_path / APP_YML_FILENAME).write_text(
            _HEADER + _V2_BODY + "runtime: python\nstreamlit: true\n"
        )
        load_app_yml(tmp_path)
        assert _get_app_yml_telemetry() == {
            CLITelemetryField.APP_RUNTIME: "python",
            CLITelemetryField.APP_STREAMLIT: True,
        }

    def test_v2_default_node_without_header(self, tmp_path, clean_context):
        (tmp_path / APP_YML_FILENAME).write_text(
            _V2_BODY + "install:\n  commands:\n    - [pip, install, .]\n"
        )
        load_app_yml(tmp_path)
        assert _get_app_yml_telemetry() == {
            CLITelemetryField.APP_RUNTIME: "node",
            CLITelemetryField.APP_STREAMLIT: False,
        }

    def test_versionless_nextjs_flow(self, tmp_path, clean_context):
        (tmp_path / APP_YML_FILENAME).write_text(
            "# created-by: snowflake-apps-create; template=nextjs-template; "
            "skill-version=1.0.0\nruntime: node\n"
        )
        assert load_app_yml(tmp_path) is None
        info = _get_app_yml_telemetry()
        assert info == {
            CLITelemetryField.APP_RUNTIME: "node",
            CLITelemetryField.APP_STREAMLIT: False,
        }

    @pytest.mark.parametrize(
        "body",
        ["runtime: python\nstreamlit: true\n", "runtime: Python\nstreamlit: true\n"],
    )
    def test_versionless_python_streamlit_is_reported(
        self, body, tmp_path, clean_context
    ):
        (tmp_path / APP_YML_FILENAME).write_text(body)
        assert load_app_yml(tmp_path) is None
        assert _get_app_yml_telemetry() == {
            CLITelemetryField.APP_RUNTIME: "python",
            CLITelemetryField.APP_STREAMLIT: True,
        }

    def test_versionless_invalid_values_are_dropped_not_reported(
        self, tmp_path, clean_context
    ):
        (tmp_path / APP_YML_FILENAME).write_text(
            "runtime: ruby\nstreamlit: nope\nrun:\n  command: [python, app.py]\n"
        )
        assert load_app_yml(tmp_path) is None
        info = _get_app_yml_telemetry()
        assert info[CLITelemetryField.APP_RUNTIME] == "node"
        assert info[CLITelemetryField.APP_STREAMLIT] is False

    def test_versionless_streamlit_with_node_keeps_runtime(
        self, tmp_path, clean_context
    ):
        (tmp_path / APP_YML_FILENAME).write_text("runtime: node\nstreamlit: true\n")
        load_app_yml(tmp_path)
        info = _get_app_yml_telemetry()
        assert info[CLITelemetryField.APP_RUNTIME] == "node"
        assert info[CLITelemetryField.APP_STREAMLIT] is False

    def test_no_app_yml_records_nothing(self, tmp_path, clean_context):
        load_app_yml(tmp_path)
        assert clean_context.app_yml_info is None
        assert _get_app_yml_telemetry() == {}

    def test_unexpected_values_are_not_forwarded(self, clean_context):
        clean_context.app_yml_info = {
            "runtime": "ruby",
            "streamlit": "yes",
            "secret": "do-not-send",
        }
        assert _get_app_yml_telemetry() == {}

    def test_never_raises(self, clean_context):
        with patch(
            "snowflake.cli._app.telemetry.get_cli_context", side_effect=RuntimeError
        ):
            assert _get_app_yml_telemetry() == {}

    def test_recording_failure_does_not_fail_load(self, tmp_path, clean_context):
        (tmp_path / APP_YML_FILENAME).write_text(_V2_BODY)
        with patch(
            "snowflake.cli._plugins.apps.app_yml.get_cli_context_manager",
            side_effect=RuntimeError("boom"),
        ):
            assert load_app_yml(tmp_path) is not None
        assert clean_context.app_yml_info is None


class TestServiceSpecificationExcludesRuntimeKeys:
    """GS parses the inline SPECIFICATION with FAIL_ON_UNKNOWN_PROPERTIES, so
    ``runtime`` / ``streamlit`` (and the provenance header) must never leak
    into it."""

    def test_specification_has_only_allowlisted_keys(self, tmp_path):
        import yaml
        from snowflake.cli._plugins.apps.app_yml import resolve_target
        from snowflake.cli._plugins.apps.manager import SnowflakeAppManager

        (tmp_path / APP_YML_FILENAME).write_text(
            _HEADER
            + _V2_BODY
            + dedent(
                """\
                runtime: python
                streamlit: true
                compute_resource: SERVERLESS
                label: My app
                url_prefix: my-app
                health_check: /healthz
                min_instances: 1
                targets:
                  dev:
                    runtime: python
                    streamlit: true
                    description: dev
                """
            )
        )
        app_def = load_app_yml(tmp_path)
        _, target = resolve_target(app_def, "dev")

        rendered = SnowflakeAppManager.build_service_specification(
            target, include_url_prefix=True, include_health_check=True
        )
        spec = yaml.safe_load(rendered)

        assert {"runtime", "streamlit"}.isdisjoint(spec)
        assert set(spec) == {
            "query_warehouse",
            "url_prefix",
            "health_check",
            "label",
            "description",
            "min_instances",
        }
        for leaked in ("runtime", "streamlit", "created-by", "streamlit-template"):
            assert leaked not in rendered
