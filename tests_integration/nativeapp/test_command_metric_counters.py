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
from typing import Dict, Callable
from unittest import mock

from snowflake.cli._app.telemetry import TelemetryEvent, CLITelemetryField
from snowflake.cli.api.metrics import CLICounterField
from tests.nativeapp.factories import (
    ProjectV11Factory,
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
    ApplicationEntityModelFactory,
)
from tests.project.fixtures import *
from tests_common import temp_dir
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

    assert (
        message[CLITelemetryField.COUNTERS.value][CLICounterField.SQL_TEMPLATES]
        == expected_counter
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,project_factory,expected_counters",
    [
        (
            "app deploy",
            lambda: ProjectV11Factory(
                pdf__native_app__artifacts=["README.md", "setup.sql", "manifest.yml"],
                pdf__native_app__package__post_deploy=[
                    {"sql_script": "post_deploy1.sql"},
                ],
                files={
                    "README.md": "",
                    "setup.sql": "select 1",
                    "manifest.yml": "\n",
                    "post_deploy1.sql": "\n",
                },
            ),
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 0,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
                CLICounterField.PACKAGE_SCRIPTS: 0,
            },
        ),
        (
            "ws bundle --entity-id=pkg",
            lambda: ProjectV2Factory(
                pdf__entities=dict(
                    pkg=ApplicationPackageEntityModelFactory(
                        identifier="myapp_pkg_<% ctx.env.USER %>",
                        artifacts=[
                            "setup.sql",
                            "README.md",
                            "manifest.yml",
                            # just needs to have the templates processor to nest phases
                            {"src": "app/*", "dest": "./", "processors": ["templates"]},
                        ],
                    ),
                    app=ApplicationEntityModelFactory(
                        identifier="myapp",
                        fromm__target="pkg",
                        meta__post_deploy=[
                            {"sql_script": "app_post_deploy1.sql"},
                        ],
                    ),
                ),
                files={
                    "app_post_deploy1.sql": "\n",
                    "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
                    "README.md": "\n",
                    "manifest.yml": "\n",
                    "app/dummy_file.md": "\n",
                },
            ),
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 1,
                CLICounterField.PDF_TEMPLATES: 1,
            },
        ),
        (
            "app run",
            lambda: ProjectV2Factory(
                pdf__entities=dict(
                    pkg=ApplicationPackageEntityModelFactory(
                        identifier="myapp_pkg",
                        artifacts=[
                            "setup.sql",
                            "README.md",
                            "manifest.yml",
                            {"src": "app/*", "dest": "./", "processors": ["templates"]},
                        ],
                    ),
                    app=ApplicationEntityModelFactory(
                        identifier="myapp",
                        fromm__target="pkg",
                    ),
                ),
                files={
                    "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
                    "README.md": "\n",
                    "manifest.yml": "\n",
                    "app/dummy_file.md": "\n",
                },
            ),
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 1,
                CLICounterField.PDF_TEMPLATES: 0,
                CLICounterField.POST_DEPLOY_SCRIPTS: 0,
            },
        ),
        (
            "app deploy",
            lambda: ProjectV11Factory(
                pdf__native_app__package__scripts=["scripts/package_script1.sql"],
                pdf__native_app__artifacts=["README.md", "setup.sql", "manifest.yml"],
                pdf__native_app__package__warehouse="non_existent_warehouse",
                files={
                    "README.md": "",
                    "setup.sql": "select 1",
                    "manifest.yml": "\n",
                    "scripts/package_script1.sql": "\n",
                },
            ),
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 0,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
                CLICounterField.PACKAGE_SCRIPTS: 1,
            },
        ),
        (
            "app deploy",
            lambda: ProjectV2Factory(
                pdf__entities=dict(
                    pkg=ApplicationPackageEntityModelFactory(
                        identifier="myapp_pkg",
                        meta__post_deploy=[
                            {"sql_script": "scripts/pkg_post_deploy1.sql"},
                        ],
                    ),
                    app=ApplicationEntityModelFactory(
                        identifier="myapp_<% ctx.env.USER %>",
                        fromm__target="pkg",
                    ),
                ),
                files={
                    "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
                    "README.md": "\n",
                    "manifest.yml": "\n",
                    "scripts/pkg_post_deploy1.sql": "-- package post-deploy script\n",
                },
            ),
            {
                CLICounterField.SNOWPARK_PROCESSOR: 0,
                CLICounterField.TEMPLATES_PROCESSOR: 0,
                CLICounterField.PDF_TEMPLATES: 1,
                CLICounterField.POST_DEPLOY_SCRIPTS: 1,
            },
        ),
    ],
    ids=[
        "v1_post_deploy_set_and_package_scripts_available",
        "v2_post_deploy_not_available_in_only_bundle",
        "v2_templates_processor_set",
        "v1_package_scripts_converted_to_post_deploy_both_set",
        "v2_post_deploy_set_and_package_scripts_not_available",
    ],
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_nativeapp_feature_counter_has_expected_value(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temp_dir,
    command: str,
    project_factory: Callable,
    expected_counters: Dict[str, int],
):
    local_test_env = {
        "APP_DIR": "app",
        "schema_name": "test_schema",
        "table_name": "test_table",
        "value": "test_value",
    }

    project_factory()

    with nativeapp_teardown(project_dir=Path(temp_dir)):
        runner.invoke_with_connection(split(command), env=local_test_env)

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == expected_counters
