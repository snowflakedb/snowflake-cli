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
from typing import Dict, Any
from unittest import mock
from unittest.mock import MagicMock

from snowflake.connector import ProgrammingError

from snowflake.cli._app.telemetry import CLITelemetryField
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from tests.project.fixtures import *
from tests_integration.test_utils import pushd


def _extract_first_result_executing_command_telemetry_message(
    mock_telemetry: MagicMock,
) -> Dict[str, Any]:
    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    return next(
        args.args[0].to_dict()["message"]
        for args in mock_telemetry.call_args_list
        if args.args[0].to_dict().get("message").get("type")
        == "error_executing_command"
    )


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

    message = _extract_first_result_executing_command_telemetry_message(mock_telemetry)

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

            message = _extract_first_result_executing_command_telemetry_message(
                mock_telemetry
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
