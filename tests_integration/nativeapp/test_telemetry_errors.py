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


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["napp_project_with_pkg_warehouse"], indirect=True
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_ProgrammingError_attaches_errno_and_sqlstate(
    mock_telemetry,
    runner,
    snowflake_session,
    project_definition_files: List[Path],
    default_username,
    nativeapp_teardown,
    resource_suffix,
):
    local_test_env = {
        "role": "nonexistent_role",
    }

    with pushd(project_definition_files[0].parent):
        result = runner.invoke_with_connection_json(
            ["app", "deploy"], env=local_test_env
        )
        assert result.exit_code == 1

        message = _extract_first_result_executing_command_telemetry_message(
            mock_telemetry
        )
        assert message["error_code"] == 3013 and message["sql_state"] == "42501"

    nativeapp_teardown()
