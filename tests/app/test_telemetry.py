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
from unittest import mock

from snowflake.connector.version import VERSION as DRIVER_VERSION


@mock.patch(
    "snowflake.cli.app.telemetry.python_version",
)
@mock.patch("snowflake.cli.app.telemetry.platform.platform")
@mock.patch("snowflake.cli.app.telemetry.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
@mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_FEATURES_FOO": "False"})
def test_executing_command_sends_telemetry_data(
    _, mock_conn, mock_time, mock_platform, mock_version, runner
):
    mock_time.return_value = "123"
    mock_platform.return_value = "FancyOS"
    mock_version.return_value = "2.3.4"

    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    actual_call = mock_conn.return_value._telemetry.try_add_log_to_batch.call_args.args[  # noqa: SLF001
        0
    ].to_dict()
    assert actual_call == {
        "message": {
            "driver_type": "PythonConnector",
            "driver_version": ".".join(str(s) for s in DRIVER_VERSION[:3]),
            "source": "snowcli",
            "version_cli": "0.0.0-test_patched",
            "version_os": "FancyOS",
            "version_python": "2.3.4",
            "command": ["connection", "test"],
            "command_group": "connection",
            "command_flags": {"diag_log_path": "DEFAULT", "format": "DEFAULT"},
            "command_output_type": "TABLE",
            "type": "executing_command",
            "project_definition_version": "None",
            "config_feature_flags": {
                "dummy_flag": "True",
                "foo": "False",
                "wrong_type_flag": "UNKNOWN",
            },
        },
        "timestamp": "123",
    }


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.streamlit.commands.StreamlitManager")
def test_executing_command_sends_project_definition_in_telemetry_data(
    _, mock_conn, project_directory, runner
):

    with project_directory("streamlit_full_definition"):
        result = runner.invoke(["streamlit", "deploy"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    actual_call = mock_conn.return_value._telemetry.try_add_log_to_batch.call_args.args[  # noqa: SLF001
        0
    ].to_dict()
    assert actual_call["message"]["project_definition_version"] == "1"
