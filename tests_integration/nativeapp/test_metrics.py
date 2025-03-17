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
from snowflake.cli.api.metrics import CLICounterField, CLIMetricsSpan
from tests.nativeapp.factories import (
    ProjectV11Factory,
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
    ApplicationEntityModelFactory,
)
from tests.project.fixtures import *
from tests_integration.test_utils import extract_first_telemetry_message_of_type


def assert_spans_within_limits(spans: Dict) -> None:
    assert spans[CLITelemetryField.NUM_SPANS_PAST_DEPTH_LIMIT.value] == 0
    assert spans[CLITelemetryField.NUM_SPANS_PAST_TOTAL_LIMIT.value] == 0
    assert all(
        span[CLIMetricsSpan.TRIMMED_KEY] == False
        for span in spans[CLITelemetryField.COMPLETED_SPANS.value]
    )


def extract_span_keys_to_check(spans: Dict) -> List[Dict]:
    SPAN_KEYS_TO_CHECK = [CLIMetricsSpan.NAME_KEY, CLIMetricsSpan.PARENT_KEY]

    return [
        {key: span[key] for key in SPAN_KEYS_TO_CHECK}
        for span in spans[CLITelemetryField.COMPLETED_SPANS.value]
    ]


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
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_feature_counters_v1_post_deploy_set_and_package_scripts_available(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV11Factory(
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
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "deploy"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == {
            CLICounterField.SNOWPARK_PROCESSOR: 0,
            CLICounterField.TEMPLATES_PROCESSOR: 0,
            CLICounterField.PDF_TEMPLATES: 0,
            CLICounterField.POST_DEPLOY_SCRIPTS: 1,
            CLICounterField.PACKAGE_SCRIPTS: 0,
        }


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_feature_counters_v2_post_deploy_not_available_in_bundle(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
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
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["ws", "bundle", "--entity-id=pkg"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == {
            CLICounterField.SNOWPARK_PROCESSOR: 0,
            CLICounterField.TEMPLATES_PROCESSOR: 0,
            CLICounterField.PDF_TEMPLATES: 1,
        }


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_feature_counter_v2_templates_processor_set(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
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
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "run"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == {
            CLICounterField.SNOWPARK_PROCESSOR: 0,
            CLICounterField.TEMPLATES_PROCESSOR: 1,
            CLICounterField.PDF_TEMPLATES: 0,
            CLICounterField.POST_DEPLOY_SCRIPTS: 0,
            CLICounterField.EVENT_SHARING: 0,
            CLICounterField.EVENT_SHARING_ERROR: 0,
            CLICounterField.EVENT_SHARING_WARNING: 0,
        }


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_feature_counter_v1_package_scripts_converted_to_post_deploy_and_both_set(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV11Factory(
        pdf__native_app__package__scripts=["scripts/package_script1.sql"],
        pdf__native_app__artifacts=["README.md", "setup.sql", "manifest.yml"],
        files={
            "README.md": "",
            "setup.sql": "select 1",
            "manifest.yml": "\n",
            "scripts/package_script1.sql": "\n",
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "deploy"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == {
            CLICounterField.SNOWPARK_PROCESSOR: 0,
            CLICounterField.TEMPLATES_PROCESSOR: 0,
            CLICounterField.PDF_TEMPLATES: 0,
            CLICounterField.POST_DEPLOY_SCRIPTS: 1,
            CLICounterField.PACKAGE_SCRIPTS: 1,
        }


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_feature_counter_v2_post_deploy_set_and_package_scripts_not_available(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
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
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "deploy"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        assert message[CLITelemetryField.COUNTERS.value] == {
            CLICounterField.SNOWPARK_PROCESSOR: 0,
            CLICounterField.TEMPLATES_PROCESSOR: 0,
            CLICounterField.PDF_TEMPLATES: 1,
            CLICounterField.POST_DEPLOY_SCRIPTS: 1,
        }


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_spans_bundle(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                artifacts=[
                    "setup.sql",
                    "README.md",
                    "manifest.yml",
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
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["ws", "bundle", "--entity-id=pkg"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        spans = message[CLITelemetryField.SPANS.value]

        assert_spans_within_limits(spans)

        assert extract_span_keys_to_check(spans) == [
            {"name": "action.app_pkg.bundle", "parent": None},
            {"name": "bundle", "parent": "action.app_pkg.bundle"},
        ]


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_spans_run_with_all_features(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                artifacts=[
                    "setup.sql",
                    "README.md",
                    "manifest.yml",
                    {"src": "app/*", "dest": "./", "processors": ["templates"]},
                ],
                meta__post_deploy=[
                    {"sql_script": "scripts/pkg_post_deploy1.sql"},
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
            "scripts/app_post_deploy1.sql": "select '';",
            "scripts/pkg_post_deploy1.sql": "select '';",
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "run"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        spans = message[CLITelemetryField.SPANS.value]

        assert_spans_within_limits(spans)

        assert extract_span_keys_to_check(spans) == [
            {
                "name": "action.app.deploy",
                "parent": None,
            },
            {
                "name": "get_snowsight_url_for_app",
                "parent": None,
            },
            {
                "name": "action.app_pkg.deploy",
                "parent": "action.app.deploy",
            },
            {
                "name": "update_app_object",
                "parent": "action.app.deploy",
            },
            {
                "name": "bundle",
                "parent": "action.app_pkg.deploy",
            },
            {
                "name": "artifact_processors",
                "parent": "action.app_pkg.deploy",
            },
            {
                "name": "sync_deploy_root_with_stage",
                "parent": "action.app_pkg.deploy",
            },
            {
                "name": "post_deploy_hooks",
                "parent": "action.app_pkg.deploy",
            },
            {
                "name": "validate_setup_script",
                "parent": "action.app_pkg.deploy",
            },
            {
                "name": "post_deploy_hooks",
                "parent": "update_app_object",
            },
            {
                "name": "templates_processor",
                "parent": "artifact_processors",
            },
        ]


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_spans_validate(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                artifacts=[
                    "setup.sql",
                    "README.md",
                    "manifest.yml",
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
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "validate"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        spans = message[CLITelemetryField.SPANS.value]

        assert_spans_within_limits(spans)

        assert extract_span_keys_to_check(spans) == [
            {
                "name": "action.app_pkg.validate",
                "parent": None,
            },
            {
                "name": "validate_setup_script",
                "parent": "action.app_pkg.validate",
            },
            {
                "name": "bundle",
                "parent": "validate_setup_script",
            },
            {
                "name": "sync_deploy_root_with_stage",
                "parent": "validate_setup_script",
            },
        ]


@pytest.mark.integration
@mock.patch("snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch")
def test_spans_teardown(
    mock_telemetry,
    runner,
    nativeapp_teardown,
    temporary_directory,
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                artifacts=[
                    "setup.sql",
                    "README.md",
                    "manifest.yml",
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
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        runner.invoke_with_connection(["app", "teardown"])

        message = extract_first_telemetry_message_of_type(
            mock_telemetry, TelemetryEvent.CMD_EXECUTION_RESULT.value
        )

        spans = message[CLITelemetryField.SPANS.value]

        assert_spans_within_limits(spans)

        assert extract_span_keys_to_check(spans) == [
            {"name": "action.app.drop", "parent": None},
            {"name": "action.app_pkg.drop", "parent": None},
        ]
