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
from tests.project.fixtures import *
from tests_integration.test_utils import pushd, extract_first_telemetry_message_of_type


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_ProgrammingError_attaches_errno_and_sqlstate(
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


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files,command",
    [("napp_project_with_incorrect_pkg_warehouse", ["app", "deploy"])],
    indirect=["project_definition_files"],
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_ProgrammingError_cause_attaches_errno_and_sqlstate(
    mock_telemetry,
    command: List[str],
    runner,
    project_definition_files: List[Path],
    nativeapp_teardown,
):
    with pushd(project_definition_files[0].parent):
        with nativeapp_teardown():
            result = runner.invoke_with_connection_json(command)
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
            assert (
                message[CLITelemetryField.ERROR_CAUSE.value]
                is ProgrammingError.__name__
            )
