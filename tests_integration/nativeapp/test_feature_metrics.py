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
from shlex import split
from typing import Dict
from unittest import mock

from snowflake.cli._app.telemetry import TelemetryEvent
from snowflake.cli.api.metrics import CLICounterField
from tests.project.fixtures import *
from tests_integration.test_utils import extract_first_telemetry_message_of_type


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,expected_counter",
    [
        (
            [
                "sql",
                "-q",
                "select '<% ctx.env.test %>'",
                "--env",
                "test=value_from_cli",
            ],
            1,
        ),
        (["sql", "-q", "select 'string'"], 0),
    ],
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_sql_templating_emits_counter(
    mock_telemetry,
    command: List[str],
    expected_counter,
    runner,
):
    result = runner.invoke_with_connection_json(command)

    assert result.exit_code == 0

    message = extract_first_telemetry_message_of_type(
        mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
    )

    assert message["counters"][CLICounterField.SQL_TEMPLATES] == expected_counter


@pytest.mark.integration
@pytest.mark.parametrize(
    "command," "test_project," "expected_counters",
    [
        # ensure that post deploy scripts are picked up for v1
        (
            "app deploy",
            "napp_application_post_deploy_v1",
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 0,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
                CLICounterField.PACKAGE_SCRIPTS: 0,
            },
        ),
        # post deploy scripts should not be available for bundling since there is no deploy
        (
            "ws bundle --entity-id=pkg",
            "napp_templates_processors_v2",
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 1,
                CLICounterField.PDF_TEMPLATES: 1,
            },
        ),
        # ensure that templates processor is picked up
        (
            "app run",
            "napp_templates_processors_v1",
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 1,
                CLICounterField.PDF_TEMPLATES: 0,
                CLICounterField.POST_DEPLOY_SCRIPTS: 0,
                CLICounterField.PACKAGE_SCRIPTS: 0,
            },
        ),
        # package scripts are auto-converted to post deploy scripts in v1
        (
            "app deploy",
            "integration_external",
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 1,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
                CLICounterField.PACKAGE_SCRIPTS: 1,
            },
        ),
        # ensure post deploy scripts are picked up for v2
        (
            "app deploy",
            "integration_external_v2",
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 1,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
            },
        ),
    ],
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_nativeapp_feature_counter_has_expected_value(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    nativeapp_project_directory,
    command: str,
    test_project: str,
    expected_counters: Dict[str, int],
):
    local_test_env = {
        "APP_DIR": "app",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "value": "test_value",
    }

    with nativeapp_project_directory(test_project):
        runner.invoke_with_connection(split(command), env=local_test_env)

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message["counters"] == expected_counters
