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

import os
import uuid
from unittest import mock

import pytest
import typer
from click import ClickException
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from snowflake.connector import ProgrammingError
from snowflake.connector.version import VERSION as DRIVER_VERSION


@mock.patch(
    "snowflake.cli._app.telemetry.python_version",
)
@mock.patch("snowflake.cli._app.telemetry.platform.platform")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._app.telemetry.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_FEATURES_FOO": "False"})
def test_executing_command_sends_telemetry_usage_data(
    _, mock_conn, mock_time, mock_uuid4, mock_platform, mock_version, runner
):
    mock_time.return_value = "123"
    mock_platform.return_value = "FancyOS"
    mock_version.return_value = "2.3.4"
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
    )

    del usage_command_event["message"][
        "command_ci_environment"
    ]  # to avoid side effect from CI
    assert usage_command_event == {
        "message": {
            "driver_type": "PythonConnector",
            "driver_version": ".".join(str(s) for s in DRIVER_VERSION[:3]),
            "source": "snowcli",
            "version_cli": "0.0.0-test_patched",
            "version_os": "FancyOS",
            "version_python": "2.3.4",
            "installation_source": "pypi",
            "command": ["connection", "test"],
            "command_group": "connection",
            "command_execution_id": "8a2225b3800c4017a4a9eab941db58fa",
            "command_flags": {"diag_log_path": "DEFAULT", "format": "DEFAULT"},
            "command_output_type": "TABLE",
            "type": "executing_command",
            "project_definition_version": "None",
            "config_feature_flags": {
                "dummy_flag": "True",
                "foo": "False",
                "wrong_type_flag": "UNKNOWN",
            },
            "mode": "cmd",
        },
        "timestamp": "123",
    }


@pytest.mark.parametrize(
    "ci_type, env_var",
    [
        ("GITHUB_ACTIONS", "GITHUB_ACTIONS"),
        ("GITLAB_CI", "GITLAB_CI"),
        ("CIRCLECI", "CIRCLECI"),
        ("JENKINS", "JENKINS_URL"),
        ("JENKINS", "HUDSON_URL"),
        ("AZURE_DEVOPS", "TF_BUILD"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_executing_command_sends_ci_usage_data(_, mock_conn, runner, env_var, ci_type):
    with mock.patch.dict(os.environ, {env_var: "true"}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
    )

    assert usage_command_event["message"]["command_ci_environment"] == ci_type


@mock.patch(
    "snowflake.cli._app.telemetry.python_version",
)
@mock.patch("snowflake.cli._app.telemetry.platform.platform")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.connector.time_util.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_FEATURES_FOO": "False"})
def test_executing_command_sends_telemetry_result_data(
    _, mock_conn, mock_time, mock_uuid4, mock_platform, mock_version, runner
):
    mock_time.return_value = "123"
    mock_platform.return_value = "FancyOS"
    mock_version.return_value = "2.3.4"
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    result_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            1
        ]
        .args[0]
        .to_dict()
    )
    assert (
        result_command_event["message"]["type"] == "result_executing_command"
        and result_command_event["message"]["command_result_status"] == "success"
        and result_command_event["message"]["command_execution_time"]
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitEntity")
def test_executing_command_sends_project_definition_in_telemetry_data(
    mock_entity, mock_conn, project_directory, runner
):
    with project_directory("streamlit_full_definition_v2"):
        result = runner.invoke(["streamlit", "deploy"])
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    actual_call = mock_conn.return_value._telemetry.try_add_log_to_batch.call_args.args[  # noqa: SLF001
        0
    ].to_dict()
    assert actual_call["message"]["project_definition_version"] == "2"


@mock.patch("snowflake.connector.connect")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitManager")
def test_failing_executing_command_sends_telemetry_data(
    _, mock_uuid4, mock_conn, project_directory, runner
):
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    with project_directory("napp_post_deploy_missing_file"):
        runner.invoke(["app", "run"], catch_exceptions=False)

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    result_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            1
        ]
        .args[0]
        .to_dict()
    )
    assert (
        result_command_event["message"]["type"] == "error_executing_command"
        and result_command_event["message"]["error_type"] == "SourceNotFoundError"
        and result_command_event["message"]["is_cli_exception"] == True
        and result_command_event["message"]["command_execution_id"]
        == "8a2225b3800c4017a4a9eab941db58fa"
    )


@pytest.mark.parametrize(
    "error,is_cli",
    [
        (ProgrammingError(), False),
        (ClickException("message"), True),
        (
            CouldNotUseObjectError(object_type=ObjectType.WAREHOUSE, name="warehouse"),
            True,
        ),
        (typer.Abort(), True),
        (typer.Exit(), True),
        (BrokenPipeError(), True),
        (RuntimeError(), False),
    ],
)
def test_cli_exception_classification(error: Exception, is_cli: bool):
    from snowflake.cli._app.telemetry import _is_cli_exception

    assert _is_cli_exception(error) == is_cli
