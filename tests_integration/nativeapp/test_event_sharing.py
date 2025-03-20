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

import pytest

from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ManifestFactory,
    ProjectV2Factory,
)
from tests_integration.test_utils import row_from_snowflake_session


@pytest.fixture
def events_assertion(snowflake_session, resource_suffix):
    def _events_assertion(events):
        events_in_app = row_from_snowflake_session(
            snowflake_session.execute_string(
                f"show telemetry event definitions in application myapp{resource_suffix}",
            )
        )

        assert sorted(events_in_app, key=lambda x: x["type"]) == sorted(
            events,
            key=lambda x: x["type"],
        )

    return _events_assertion


def assert_events_in_app(snowflake_session, resource_suffix, events):
    events_in_app = row_from_snowflake_session(
        snowflake_session.execute_string(
            f"show telemetry event definitions in application myapp{resource_suffix}",
        )
    )

    assert sorted(events_in_app, key=lambda x: x["type"]) == sorted(
        events,
        key=lambda x: x["type"],
    )


@pytest.mark.integration
def test_given_event_sharing_with_mandatory_events_and_sharing_allowed_then_success(
    temporary_directory, runner, events_assertion, nativeapp_teardown
):
    manifest_yml = ManifestFactory(
        configuration__telemetry_event_definitions=[
            {"type": "ERRORS_AND_WARNINGS", "sharing": "MANDATORY"},
            {"type": "DEBUG_LOGS", "sharing": "OPTIONAL"},
        ]
    )
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="my_pkg",
                artifacts=[{"src": "*", "dest": "./"}],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                telemetry={
                    "share_mandatory_events": True,
                    "optional_shared_events": ["DEBUG_LOGS"],
                },
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": manifest_yml,
        },
    )

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "run"])
        assert result.exit_code == 0

        events_assertion(
            [
                {
                    "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                    "sharing": "MANDATORY",
                    "status": "ENABLED",
                    "type": "ERRORS_AND_WARNINGS",
                },
                {
                    "name": "SNOWFLAKE$DEBUG_LOGS",
                    "sharing": "OPTIONAL",
                    "status": "ENABLED",
                    "type": "DEBUG_LOGS",
                },
            ]
        )


@pytest.mark.integration
def test_given_event_sharing_with_mandatory_events_and_sharing_not_allowed_then_error(
    temporary_directory, runner, nativeapp_teardown
):
    manifest_yml = ManifestFactory(
        configuration__telemetry_event_definitions=[
            {"type": "ERRORS_AND_WARNINGS", "sharing": "MANDATORY"},
            {"type": "DEBUG_LOGS", "sharing": "OPTIONAL"},
        ]
    )
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="my_pkg",
                artifacts=[{"src": "*", "dest": "./"}],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                telemetry={"share_mandatory_events": False},
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": manifest_yml,
        },
    )

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "run"])
        assert result.exit_code == 1
        assert (
            "The application package requires event sharing to be authorized"
            in result.output
        )


@pytest.mark.integration
def test_given_event_sharing_with_no_mandatory_events_and_sharing_not_allowed_then_success(
    temporary_directory, runner, events_assertion, nativeapp_teardown
):
    manifest_yml = ManifestFactory(
        configuration__telemetry_event_definitions=[
            {"type": "ERRORS_AND_WARNINGS", "sharing": "OPTIONAL"},
            {"type": "DEBUG_LOGS", "sharing": "OPTIONAL"},
        ]
    )

    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="my_pkg",
                artifacts=[{"src": "*", "dest": "./"}],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                telemetry={"share_mandatory_events": False},
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": manifest_yml,
        },
    )

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "run"])
        assert result.exit_code == 0

        events_assertion(
            [
                {
                    "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                    "sharing": "OPTIONAL",
                    "status": "DISABLED",
                    "type": "ERRORS_AND_WARNINGS",
                },
                {
                    "name": "SNOWFLAKE$DEBUG_LOGS",
                    "sharing": "OPTIONAL",
                    "status": "DISABLED",
                    "type": "DEBUG_LOGS",
                },
            ]
        )


@pytest.mark.integration
def test_given_event_sharing_with_no_mandatory_events_and_sharing_is_allowed_then_success(
    temporary_directory, runner, events_assertion, nativeapp_teardown
):
    manifest_yml = ManifestFactory(
        configuration__telemetry_event_definitions=[
            {"type": "ERRORS_AND_WARNINGS", "sharing": "OPTIONAL"},
            {"type": "DEBUG_LOGS", "sharing": "OPTIONAL"},
        ]
    )

    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="my_pkg",
                artifacts=[{"src": "*", "dest": "./"}],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                telemetry={
                    "share_mandatory_events": True,
                    "optional_shared_events": ["DEBUG_LOGS", "ERRORS_AND_WARNINGS"],
                },
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": manifest_yml,
        },
    )

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "run"])
        assert result.exit_code == 0

        events_assertion(
            [
                {
                    "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                    "sharing": "OPTIONAL",
                    "status": "ENABLED",
                    "type": "ERRORS_AND_WARNINGS",
                },
                {
                    "name": "SNOWFLAKE$DEBUG_LOGS",
                    "sharing": "OPTIONAL",
                    "status": "ENABLED",
                    "type": "DEBUG_LOGS",
                },
            ]
        )
