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
from snowflake.cli.api.metrics import CLICounterField, CLIMetricsSpan, CLIMetrics
from tests.nativeapp.factories import (
    ProjectV11Factory,
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
    ApplicationEntityModelFactory,
)
from tests.project.fixtures import *
from tests_common import temp_dir
from tests_integration.test_utils import extract_first_telemetry_message_of_type

SPAN_KEYS_TO_CHECK = [CLIMetricsSpan.NAME_KEY, CLIMetricsSpan.PARENT_KEY]


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,project_factory,expected_spans",
    [
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
                },
            ),
            [{"name": "ApplicationPackageEntity.action_bundle", "parent": None}],
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
                        meta__post_deploy=[
                            {"sql_script": "scripts/app_post_deploy1.sql"},
                        ],
                    ),
                ),
                files={
                    "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
                    "README.md": "\n",
                    "manifest.yml": "\n",
                    "app/dummy_file.md": "\n",
                    "scripts/app_post_deploy1.sql": "-- package post-deploy script\n",
                },
            ),
            [
                {
                    "name": "ApplicationEntity.action_deploy",
                    "parent": None,
                },
                {
                    "name": "compile_artifacts",
                    "parent": "ApplicationEntity.action_deploy",
                },
                {
                    "name": "sync_deploy_root_with_stage",
                    "parent": "ApplicationEntity.action_deploy",
                },
                {
                    "name": "get_validation_result",
                    "parent": "ApplicationEntity.action_deploy",
                },
                {
                    "name": "create_or_upgrade_app",
                    "parent": "ApplicationEntity.action_deploy",
                },
                {
                    "name": "execute_post_deploy_hooks",
                    "parent": "create_or_upgrade_app",
                },
            ],
        ),
    ],
    ids=["bundle_barebones_no_other_spans", "run_with_all_features_with_spans"],
)
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_nativeapp_command_has_expected_spans(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temp_dir,
    command: str,
    project_factory: Callable,
    expected_spans: Dict,
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

        spans = message[CLITelemetryField.SPANS.value]

        # ensure spans are within our defined limits
        assert spans[CLITelemetryField.NUM_SPANS_PAST_DEPTH_LIMIT.value] == 0
        assert spans[CLITelemetryField.NUM_SPANS_PAST_TOTAL_LIMIT.value] == 0
        assert all(
            span[CLIMetricsSpan.TRIMMED_KEY] == False
            for span in spans[CLITelemetryField.COMPLETED_SPANS.value]
        )

        assert expected_spans == [
            {key: span[key] for key in SPAN_KEYS_TO_CHECK}
            for span in spans[CLITelemetryField.COMPLETED_SPANS.value]
        ]
