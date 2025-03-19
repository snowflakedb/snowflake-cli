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
from unittest import mock

from snowflake.connector import ProgrammingError

from snowflake.cli._app.telemetry import CLITelemetryField, TelemetryEvent
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from tests.nativeapp.factories import ProjectV11Factory
from tests.project.fixtures import *
from tests_integration.test_utils import extract_first_telemetry_message_of_type


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_not_programmingerror_does_not_attach_any_info(
    mock_telemetry, runner, nativeapp_project_directory, temporary_directory
):
    ProjectV11Factory(
        pdf__native_app__artifacts=["setup.sql", "manifest.yml"],
        pdf__native_app__name="myapp",
        pdf__native_app__package__post_deploy=[
            {"sql_script": "non_existent.sql"},
        ],
        files={
            "setup.sql": "\n",
            "manifest.yml": "\n",
        },
    )

    with nativeapp_project_directory(temporary_directory):
        result = runner.invoke_with_connection_json(["app", "deploy"])
        assert result.exit_code == 1

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_ERROR.value
        )
        assert CLITelemetryField.ERROR_TYPE.value in message
        assert message[CLITelemetryField.ERROR_TYPE.value] != ProgrammingError.__name__
        assert message[CLITelemetryField.ERROR_CAUSE.value] != ProgrammingError.__name__

        assert CLITelemetryField.ERROR_CODE.value not in message
        assert CLITelemetryField.SQL_STATE.value not in message


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_programmingerror_attaches_errno_and_sqlstate(
    mock_telemetry,
    runner,
):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "use warehouse non_existent_warehouse"]
    )
    assert result.exit_code == 1

    message = extract_first_telemetry_message_of_type(
        mock_telemetry, TelemetryEvent.CMD_EXECUTION_ERROR.value
    )

    assert message[CLITelemetryField.ERROR_TYPE.value] == ProgrammingError.__name__
    assert (
        message[CLITelemetryField.ERROR_CODE.value]
        == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
    )
    assert message[CLITelemetryField.SQL_STATE.value] == "02000"
    assert message.get(CLITelemetryField.ERROR_CAUSE.value) is None


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_programmingerror_cause_attaches_errno_and_sqlstate(
    mock_telemetry, runner, nativeapp_project_directory, temporary_directory
):
    ProjectV11Factory(
        pdf__native_app__artifacts=["setup.sql", "manifest.yml"],
        pdf__native_app__name="myapp",
        pdf__native_app__package__post_deploy=[
            # this file just needs to be present for the error to be triggered
            {"sql_script": "post_deploy1.sql"},
        ],
        pdf__native_app__package__warehouse="non_existent_warehouse",
        files={"post_deploy1.sql": "\n", "setup.sql": "\n", "manifest.yml": "\n"},
    )

    with nativeapp_project_directory(Path(temporary_directory)):
        result = runner.invoke_with_connection_json(["app", "deploy"])
        assert result.exit_code == 1

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_ERROR.value
        )

        assert (
            message[CLITelemetryField.ERROR_TYPE.value]
            == CouldNotUseObjectError.__name__
        )
        assert (
            message[CLITelemetryField.ERROR_CODE.value]
            == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
        )
        assert message[CLITelemetryField.SQL_STATE.value] == "02000"
        assert message[CLITelemetryField.ERROR_CAUSE.value] == ProgrammingError.__name__
