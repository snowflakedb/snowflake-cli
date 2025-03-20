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

from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock

import pytest
from click import ClickException
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
    PolicyBase,
)
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import UserInputError
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import (
    APPLICATION_REQUIRES_TELEMETRY_SHARING,
    CANNOT_DISABLE_MANDATORY_TELEMETRY,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import ProgrammingError

from tests.conftest import MockConnectionCtx
from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
)
from tests.nativeapp.patch_utils import (
    mock_connection,
)
from tests.nativeapp.utils import (
    GET_UI_PARAMETERS,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE_CREATE_APPLICATION,
    SQL_FACADE_GET_EXISTING_APP_INFO,
    SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE,
    SQL_FACADE_UPGRADE_APPLICATION,
    mock_execute_helper,
    mock_side_effect_error_with_cause,
)

DEFAULT_APP_ID = "myapp"
DEFAULT_PKG_ID = "app_pkg"
DEFAULT_STAGE_FQN = "app_pkg.app_src.stage"
DEFAULT_SUCCESS_MESSAGE = "Application successfully upgraded."
DEFAULT_USER_INPUT_ERROR_MESSAGE = "User input error message."

allow_always_policy = AllowAlwaysPolicy()
ask_always_policy = AskAlwaysPolicy()
deny_always_policy = DenyAlwaysPolicy()
test_manifest_contents = dedent(
    """\
    manifest_version: 1

    version:
        name: dev
        label: "Dev Version"
        comment: "Default version used for development. Override for actual deployment."

    artifacts:
        setup_script: setup.sql
        readme: README.md

    configuration:
        log_level: INFO
        trace_level: ALWAYS
        telemetry_event_definitions:
          - type: ERRORS_AND_WARNINGS
            sharing: OPTIONAL
          - type: DEBUG_LOGS
            sharing: OPTIONAL
"""
)

test_manifest_with_mandatory_events = dedent(
    """\
    manifest_version: 1

    version:
        name: dev
        label: "Dev Version"
        comment: "Default version used for development. Override for actual deployment."

    artifacts:
        setup_script: setup.sql
        readme: README.md

    configuration:
        telemetry_event_definitions:
          - type: ERRORS_AND_WARNINGS
            sharing: MANDATORY
          - type: DEBUG_LOGS
            sharing: OPTIONAL
"""
)


def _create_or_upgrade_app(
    policy: PolicyBase,
    install_method: SameAccountInstallMethod,
    is_interactive: bool = False,
    package_id: str = "app_pkg",
    app_id: str = "myapp",
    console: AbstractConsole | None = None,
):
    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities[package_id]
    app_model: ApplicationEntityModel = pd.entities[app_id]
    ctx = WorkspaceContext(
        console=console or cc,
        project_root=dm.project_root,
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )
    app = ApplicationEntity(app_model, ctx)
    pkg = ApplicationPackageEntity(pkg_model, ctx)

    pkg.action_bundle(action_ctx=ActionContext(get_entity=lambda *args: None))

    return app.create_or_upgrade_app(
        package=pkg,
        stage_path=pkg.stage_path,
        install_method=install_method,
        policy=policy,
        interactive=is_interactive,
    )


def _setup_project(
    app_pkg_role="package_role",
    app_pkg_warehouse="pkg_warehouse",
    app_role="app_role",
    app_warehouse="app_warehouse",
    setup_sql_contents="CREATE OR ALTER VERSIONED SCHEMA core;",
    readme_contents="\n",
    manifest_contents=test_manifest_contents,
    share_mandatory_events=None,
    optional_shared_events=None,
    stage_subdirectory="",
):
    telemetry = {}
    if share_mandatory_events is not None:
        telemetry["share_mandatory_events"] = share_mandatory_events
    if optional_shared_events is not None:
        telemetry["optional_shared_events"] = optional_shared_events
    ProjectV2Factory(
        pdf__entities=dict(
            app_pkg=ApplicationPackageEntityModelFactory(
                identifier="app_pkg",
                meta={"role": app_pkg_role, "warehouse": app_pkg_warehouse},
                stage_subdirectory=stage_subdirectory,
            ),
            myapp=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="app_pkg",
                meta={"role": app_role, "warehouse": app_warehouse},
                telemetry=(telemetry),
            ),
        ),
        files={
            "setup.sql": setup_sql_contents,
            "README.md": readme_contents,
            "manifest.yml": manifest_contents,
        },
    )


def _setup_mocks_for_app(
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    is_prod=False,
    is_upgrade=False,
    events_definitions_in_app=None,
    error_raised=None,
    stage_path_to_artifacts=DEFAULT_STAGE_FQN,
):
    if is_upgrade:
        return _setup_mocks_for_upgrade_app(
            mock_sql_facade_upgrade_application,
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            expected_authorize_telemetry_flag=expected_authorize_telemetry_flag,
            expected_shared_events=expected_shared_events,
            is_prod=is_prod,
            events_definitions_in_app=events_definitions_in_app,
            error_raised=error_raised,
            stage_path_to_artifacts=stage_path_to_artifacts,
        )
    else:
        return _setup_mocks_for_create_app(
            mock_sql_facade_create_application,
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            expected_authorize_telemetry_flag=expected_authorize_telemetry_flag,
            expected_shared_events=expected_shared_events,
            is_prod=is_prod,
            events_definitions_in_app=events_definitions_in_app,
            error_raised=error_raised,
            stage_path_to_artifacts=stage_path_to_artifacts,
        )


def _setup_mocks_for_create_app(
    mock_sql_facade_create_application,
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    events_definitions_in_app=None,
    is_prod=False,
    error_raised=None,
    stage_path_to_artifacts=DEFAULT_STAGE_FQN,
):
    mock_get_existing_app_info.return_value = None

    calls = [
        (
            mock_cursor([("old_role",)], []),
            mock.call("select current_role()"),
        ),
        (None, mock.call("use role app_role")),
        (
            mock_cursor(
                events_definitions_in_app or [], ["name", "type", "sharing", "status"]
            ),
            mock.call(
                "show telemetry event definitions in application myapp",
                cursor_class=DictCursor,
            ),
        ),
        (None, mock.call("use role old_role")),
    ]

    if expected_shared_events is not None:
        calls.extend(
            [
                (
                    mock_cursor([("old_role",)], []),
                    mock.call("select current_role()"),
                ),
                (None, mock.call("use role app_role")),
                (
                    None,
                    mock.call(
                        f"""alter application myapp set shared telemetry events ({", ".join([f"'SNOWFLAKE${x}'" for x in expected_shared_events])})"""
                    ),
                ),
                (None, mock.call("use role old_role")),
            ]
        )

    side_effects, mock_execute_query_expected = mock_execute_helper(calls)
    mock_execute_query.side_effect = side_effects

    def create_app_side_effect_function(*args, **kwargs):
        if error_raised:
            raise error_raised
        return (mock_cursor([(DEFAULT_SUCCESS_MESSAGE,)], []), [])

    mock_sql_facade_create_application.side_effect = create_app_side_effect_function

    mock_sql_facade_create_application_expected = [
        mock.call(
            name=DEFAULT_APP_ID,
            package_name=DEFAULT_PKG_ID,
            install_method=(
                SameAccountInstallMethod.release_directive()
                if is_prod
                else SameAccountInstallMethod.unversioned_dev()
            ),
            path_to_version_directory=stage_path_to_artifacts,
            debug_mode=None,
            should_authorize_event_sharing=expected_authorize_telemetry_flag,
            role="app_role",
            warehouse="app_warehouse",
            release_channel=None,
        )
    ]

    mock_sql_facade_grant_privileges_to_role_expected = [
        mock.call(
            privileges=["install", "develop"],
            object_type=ObjectType.APPLICATION_PACKAGE,
            object_identifier="app_pkg",
            role_to_grant="app_role",
            role_to_use="package_role",
        ),
        mock.call(
            privileges=["usage"],
            object_type=ObjectType.SCHEMA,
            object_identifier="app_pkg.app_src",
            role_to_grant="app_role",
            role_to_use="package_role",
        ),
        mock.call(
            privileges=["read"],
            object_type=ObjectType.STAGE,
            object_identifier="app_pkg.app_src.stage",
            role_to_grant="app_role",
            role_to_use="package_role",
        ),
    ]

    return [
        *mock_execute_query_expected,
        *mock_sql_facade_create_application_expected,
        *mock_sql_facade_grant_privileges_to_role_expected,
    ]


def _setup_mocks_for_upgrade_app(
    mock_sql_facade_upgrade_application,
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    events_definitions_in_app=None,
    is_prod=False,
    error_raised=None,
    stage_path_to_artifacts=DEFAULT_STAGE_FQN,
):
    mock_get_existing_app_info_result = {
        "comment": "GENERATED_BY_SNOWFLAKECLI",
    }
    mock_get_existing_app_info.return_value = mock_get_existing_app_info_result

    calls = [
        (
            mock_cursor([("old_role",)], []),
            mock.call("select current_role()"),
        ),
        (None, mock.call("use role app_role")),
        (
            mock_cursor(
                events_definitions_in_app or [], ["name", "type", "sharing", "status"]
            ),
            mock.call(
                "show telemetry event definitions in application myapp",
                cursor_class=DictCursor,
            ),
        ),
        (None, mock.call("use role old_role")),
    ]

    if expected_shared_events is not None:
        calls.extend(
            [
                (
                    mock_cursor([("old_role",)], []),
                    mock.call("select current_role()"),
                ),
                (None, mock.call("use role app_role")),
                (
                    None,
                    mock.call(
                        f"""alter application myapp set shared telemetry events ({", ".join([f"'SNOWFLAKE${x}'" for x in expected_shared_events])})"""
                    ),
                ),
                (None, mock.call("use role old_role")),
            ],
        )

    side_effects, mock_execute_query_expected = mock_execute_helper(calls)
    mock_execute_query.side_effect = side_effects

    mock_sql_facade_upgrade_application.side_effect = error_raised or mock_cursor(
        [[(DEFAULT_SUCCESS_MESSAGE,)]], []
    )
    mock_sql_facade_upgrade_application_expected = [
        mock.call(
            name=DEFAULT_APP_ID,
            install_method=(
                SameAccountInstallMethod.release_directive()
                if is_prod
                else SameAccountInstallMethod.unversioned_dev()
            ),
            path_to_version_directory=stage_path_to_artifacts,
            debug_mode=None,
            should_authorize_event_sharing=expected_authorize_telemetry_flag,
            role="app_role",
            warehouse="app_warehouse",
            release_channel=None,
        )
    ]
    return [*mock_execute_query_expected, *mock_sql_facade_upgrade_application_expected]


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: False,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: False,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
        test_manifest_with_mandatory_events,
    ],
)
@pytest.mark.parametrize(
    "share_mandatory_events",
    [
        False,
        None,
    ],
)
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize(
    "is_upgrade",
    [False, True],
)
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_disabled_no_change_to_current_behavior(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        is_upgrade=is_upgrade,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: False,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: False,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
        test_manifest_with_mandatory_events,
    ],
)
@pytest.mark.parametrize("share_mandatory_events", [True])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_disabled_but_we_add_event_sharing_flag_in_project_definition_file(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        is_upgrade=is_upgrade,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )

    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    assert mock_console.warning.mock_calls == [
        mock.call(
            "WARNING: Same-account event sharing is not enabled in your account, therefore, application telemetry section will be ignored."
        ),
        mock.call(DEFAULT_SUCCESS_MESSAGE),
    ]


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: False,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
    ],
)
@pytest.mark.parametrize("share_mandatory_events", [True, False, None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_enabled_not_enforced_no_mandatory_events_then_flag_respected(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
    ],
)
@pytest.mark.parametrize("share_mandatory_events", [True, False])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_enabled_when_upgrade_flag_matches_existing_app_then_do_not_set_it_explicitly(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,  # requested flag from the project definition file
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: False,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [True])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_enabled_with_mandatory_events_and_explicit_authorization_then_flag_respected(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=["ERRORS_AND_WARNINGS"],
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert expected == [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ]

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: False,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [False, None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_event_sharing_enabled_with_mandatory_events_but_no_authorization_then_flag_respected_with_warning(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    assert mock_console.warning.mock_calls == [
        mock.call(DEFAULT_SUCCESS_MESSAGE),
        mock.call(
            "WARNING: Mandatory events are present in the application, but event sharing is not authorized in the application telemetry field. This will soon be required to set in order to deploy this application."
        ),
    ]


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_contents],
)
@pytest.mark.parametrize("share_mandatory_events", [True, False, None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_no_mandatory_events_then_use_value_provided_for_authorization(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [True])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_provided(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    stage_subdir,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [False])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_refused_on_create_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        error_raised=mock_side_effect_error_with_cause(
            UserInputError(
                "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
            ),
            ProgrammingError(errno=APPLICATION_REQUIRES_TELEMETRY_SHARING),
        ),
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )
    mock_console.warning.assert_not_called()


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [False])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_manifest_and_authorization_refused_on_update_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        error_raised=mock_side_effect_error_with_cause(
            UserInputError(
                "Could not disable telemetry event sharing for the application because it contains mandatory events. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
            ),
            ProgrammingError(errno=CANNOT_DISABLE_MANDATORY_TELEMETRY),
        ),
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    with pytest.raises(UserInputError) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "Could not disable telemetry event sharing for the application because it contains mandatory events. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )
    mock_console.warning.assert_not_called()


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_and_dev_mode_then_default_to_true_with_warning(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=True,
        is_upgrade=is_upgrade,
        expected_shared_events=[],
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected
    expected_warning = "WARNING: Mandatory events are present in the manifest file. Automatically authorizing event sharing in dev mode. To suppress this warning, please add 'share_mandatory_events: true' in the application telemetry section."
    assert mock_console.warning.mock_calls == [
        mock.call(expected_warning),
        mock.call(DEFAULT_SUCCESS_MESSAGE),
    ]


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_not_specified_on_create_and_prod_mode_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_upgrade=is_upgrade,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        error_raised=mock_side_effect_error_with_cause(
            UserInputError(
                "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
            ),
            ProgrammingError(errno=APPLICATION_REQUIRES_TELEMETRY_SHARING),
        ),
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )
    mock_console.warning.assert_not_called()


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_not_specified_on_update_and_prod_mode_then_no_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_upgrade=is_upgrade,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        stage_subdirectory=stage_subdir,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("share_mandatory_events", [False, None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_shared_events_with_no_enabled_mandatory_events_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        optional_shared_events=["DEBUG_LOGS"],
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "'telemetry.share_mandatory_events' must be set to 'true' when sharing optional events through 'telemetry.optional_shared_events'."
    )
    mock_console.warning.assert_not_called()


@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_FACADE_CREATE_APPLICATION)
@mock.patch(SQL_FACADE_UPGRADE_APPLICATION)
@mock.patch(SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: True,
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: True,
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events, test_manifest_contents],
)
@pytest.mark.parametrize("share_mandatory_events", [True])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
@pytest.mark.parametrize("stage_subdir", ["", "v1"])
def test_shared_events_with_authorization_then_success(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_sql_facade_grant_privileges_to_role,
    mock_sql_facade_upgrade_application,
    mock_sql_facade_create_application,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    stage_subdir,
    is_upgrade,
    temporary_directory,
    mock_cursor,
):
    shared_events = ["DEBUG_LOGS", "ERRORS_AND_WARNINGS"]
    expected = _setup_mocks_for_app(
        mock_sql_facade_upgrade_application,
        mock_sql_facade_create_application,
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=shared_events,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            },
            {
                "name": "SNOWFLAKE$DEBUG_LOGS",
                "type": "DEBUG_LOGS",
                "sharing": "OPTIONAL",
                "status": "ENABLED",
            },
        ],
        stage_path_to_artifacts=f"{DEFAULT_STAGE_FQN}/{stage_subdir}"
        if stage_subdir
        else DEFAULT_STAGE_FQN,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        optional_shared_events=shared_events,
        stage_subdirectory=stage_subdir,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert [
        *mock_execute_query.mock_calls,
        *mock_sql_facade_upgrade_application.mock_calls,
        *mock_sql_facade_create_application.mock_calls,
        *mock_sql_facade_grant_privileges_to_role.mock_calls,
    ] == expected

    mock_console.warning.assert_called_once_with(DEFAULT_SUCCESS_MESSAGE)
