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
from snowflake.cli._plugins.nativeapp.constants import (
    SPECIAL_COMMENT,
)
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
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.errno import (
    APPLICATION_REQUIRES_TELEMETRY_SHARING,
    CANNOT_DISABLE_MANDATORY_TELEMETRY,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import ProgrammingError

from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
)
from tests.nativeapp.patch_utils import (
    mock_connection,
)
from tests.nativeapp.utils import (
    APP_ENTITY_GET_EXISTING_APP_INFO,
    GET_UI_PARAMETERS,
    SQL_EXECUTOR_EXECUTE,
    mock_execute_helper,
)
from tests.testing_utils.fixtures import MockConnectionCtx

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
    stage_fqn = f"{pkg_model.fqn.name}.{pkg_model.stage}"

    pkg.action_bundle(action_ctx=ActionContext(get_entity=lambda *args: None))

    return app.create_or_upgrade_app(
        package=pkg,
        stage_fqn=stage_fqn,
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
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    is_prod=False,
    is_upgrade=False,
    existing_app_flag=False,
    events_definitions_in_app=None,
    programming_errno=None,
):
    if is_upgrade:
        return _setup_mocks_for_upgrade_app(
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            expected_authorize_telemetry_flag=expected_authorize_telemetry_flag,
            expected_shared_events=expected_shared_events,
            is_prod=is_prod,
            existing_app_flag=existing_app_flag,
            events_definitions_in_app=events_definitions_in_app,
            programming_errno=programming_errno,
        )
    else:
        return _setup_mocks_for_create_app(
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            expected_authorize_telemetry_flag=expected_authorize_telemetry_flag,
            expected_shared_events=expected_shared_events,
            is_prod=is_prod,
            events_definitions_in_app=events_definitions_in_app,
            programming_errno=programming_errno,
        )


def _setup_mocks_for_create_app(
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    events_definitions_in_app=None,
    is_prod=False,
    programming_errno=None,
):
    mock_get_existing_app_info.return_value = None

    authorize_telemetry_clause = ""
    if expected_authorize_telemetry_flag is not None:
        authorize_telemetry_clause = f" AUTHORIZE_TELEMETRY_EVENT_SHARING = {expected_authorize_telemetry_flag}".upper()
    install_clause = "using @app_pkg.app_src.stage debug_mode = True"
    if is_prod:
        install_clause = " "

    calls = [
        (
            mock_cursor([("old_role",)], []),
            mock.call("select current_role()"),
        ),
        (None, mock.call("use role app_role")),
        (
            mock_cursor([("old_wh",)], []),
            mock.call("select current_warehouse()"),
        ),
        (None, mock.call("use warehouse app_warehouse")),
        (
            mock_cursor([("app_role",)], []),
            mock.call("select current_role()"),
        ),
        (None, mock.call("use role package_role")),
        (
            None,
            mock.call(
                "grant install, develop on application package app_pkg to role app_role"
            ),
        ),
        (
            None,
            mock.call("grant usage on schema app_pkg.app_src to role app_role"),
        ),
        (
            None,
            mock.call("grant read on stage app_pkg.app_src.stage to role app_role"),
        ),
        (None, mock.call("use role app_role")),
        (
            (ProgrammingError(errno=programming_errno) if programming_errno else None),
            mock.call(
                dedent(
                    f"""\
                    create application myapp
                        from application package app_pkg {install_clause}{authorize_telemetry_clause}
                        comment = {SPECIAL_COMMENT}
                    """
                )
            ),
        ),
        (
            mock_cursor([("app_role",)], []),
            mock.call("select current_role()"),
        ),
        (
            mock_cursor(
                events_definitions_in_app or [], ["name", "type", "sharing", "status"]
            ),
            mock.call(
                "show telemetry event definitions in application myapp",
                cursor_class=DictCursor,
            ),
        ),
    ]

    if expected_shared_events is not None:
        calls.append(
            (
                None,
                mock.call(
                    f"""alter application myapp set shared telemetry events ({", ".join([f"'SNOWFLAKE${x}'" for x in expected_shared_events])})"""
                ),
            ),
        )

    calls.extend(
        [
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    side_effects, mock_execute_query_expected = mock_execute_helper(calls)
    mock_execute_query.side_effect = side_effects
    return mock_execute_query_expected


def _setup_mocks_for_upgrade_app(
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    expected_authorize_telemetry_flag=None,
    expected_shared_events=None,
    events_definitions_in_app=None,
    is_prod=False,
    existing_app_flag=False,
    programming_errno=None,
):
    mock_get_existing_app_info.return_value = {
        "comment": "GENERATED_BY_SNOWFLAKECLI",
    }
    install_clause = "using @app_pkg.app_src.stage"
    if is_prod:
        install_clause = ""

    calls = [
        (
            mock_cursor([("old_role",)], []),
            mock.call("select current_role()"),
        ),
        (None, mock.call("use role app_role")),
        (
            mock_cursor([("old_wh",)], []),
            mock.call("select current_warehouse()"),
        ),
        (None, mock.call("use warehouse app_warehouse")),
        (None, mock.call(f"alter application myapp upgrade {install_clause}")),
        (
            mock_cursor([("app_role",)], []),
            mock.call("select current_role()"),
        ),
        (
            mock_cursor(
                events_definitions_in_app or [], ["name", "type", "sharing", "status"]
            ),
            mock.call(
                "show telemetry event definitions in application myapp",
                cursor_class=DictCursor,
            ),
        ),
        (
            mock_cursor([("app_role",)], []),
            mock.call("select current_role()"),
        ),
        (
            mock_cursor(
                [
                    {
                        "property": "authorize_telemetry_event_sharing",
                        "value": str(existing_app_flag).lower(),
                    }
                ],
                ["property", "value"],
            ),
            mock.call(
                "desc application myapp",
                cursor_class=DictCursor,
            ),
        ),
    ]

    if expected_authorize_telemetry_flag is not None:
        calls.append(
            (
                (
                    ProgrammingError(errno=programming_errno)
                    if programming_errno
                    else None
                ),
                mock.call(
                    f"alter application myapp set AUTHORIZE_TELEMETRY_EVENT_SHARING = {str(expected_authorize_telemetry_flag).upper()}"
                ),
            ),
        )

    if expected_shared_events is not None:
        calls.append(
            (
                None,
                mock.call(
                    f"""alter application myapp set shared telemetry events ({", ".join([f"'SNOWFLAKE${x}'" for x in expected_shared_events])})"""
                ),
            ),
        )

    calls.extend(
        [
            (None, mock.call("use warehouse old_wh")),
            (None, mock.call("use role old_role")),
        ]
    )
    side_effects, mock_execute_query_expected = mock_execute_helper(calls)
    mock_execute_query.side_effect = side_effects
    return mock_execute_query_expected


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "false",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "false",
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
def test_event_sharing_disabled_no_change_to_current_behavior(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "false",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "false",
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
def test_event_sharing_disabled_but_we_add_event_sharing_flag_in_project_definition_file(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        is_upgrade=is_upgrade,
    )

    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_called_with(
        "WARNING: Same-account event sharing is not enabled in your account, therefore, application telemetry section will be ignored."
    )


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "false",
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
def test_event_sharing_enabled_not_enforced_no_mandatory_events_then_flag_respected(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        existing_app_flag=not share_mandatory_events,  # existing app with opposite flag to test that flag has changed
        expected_shared_events=[] if share_mandatory_events else None,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_event_sharing_enabled_when_upgrade_flag_matches_existing_app_then_do_not_set_it_explicitly(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=None,  # make sure flag is not set again during upgrade
        is_upgrade=is_upgrade,
        existing_app_flag=share_mandatory_events,  # existing app with same flag as target app
        expected_shared_events=[] if share_mandatory_events else None,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,  # requested flag from the project definition file
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "false",
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
def test_event_sharing_enabled_with_mandatory_events_and_explicit_authorization_then_flag_respected(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        existing_app_flag=not share_mandatory_events,  # existing app with opposite flag to test that flag has changed
        expected_shared_events=["ERRORS_AND_WARNINGS"],
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "false",
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
def test_event_sharing_enabled_with_mandatory_events_but_no_authorization_then_flag_respected_with_warning(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=(
            None if is_upgrade else share_mandatory_events
        ),
        is_upgrade=is_upgrade,
        existing_app_flag=False,  # we can't switch from True to False, so we assume False
        expected_shared_events=[] if share_mandatory_events else None,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected

    mock_console.warning.assert_called_with(
        "WARNING: Mandatory events are present in the application, but event sharing is not authorized in the application telemetry field. This will soon be required to set in order to deploy this application."
    )


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_no_mandatory_events_then_use_value_provided_for_authorization(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        existing_app_flag=not share_mandatory_events,  # existing app with opposite flag to test that flag has changed
        expected_shared_events=[] if share_mandatory_events else None,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_and_authorization_provided(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
        expected_shared_events=[] if share_mandatory_events else None,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_and_authorization_refused_on_create_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        existing_app_flag=not share_mandatory_events,  # existing app with opposite flag to test that flag has changed
        is_upgrade=is_upgrade,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        programming_errno=APPLICATION_REQUIRES_TELEMETRY_SHARING,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
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


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_manifest_and_authorization_refused_on_update_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        existing_app_flag=not share_mandatory_events,  # existing app with opposite flag to test that flag has changed
        is_upgrade=is_upgrade,
        events_definitions_in_app=[
            {
                "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                "type": "ERRORS_AND_WARNINGS",
                "sharing": "MANDATORY",
                "status": "ENABLED",
            }
        ],
        programming_errno=CANNOT_DISABLE_MANDATORY_TELEMETRY,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
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
        == "Could not disable telemetry event sharing for the application because it contains mandatory events. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
    )
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_and_dev_mode_then_default_to_true_with_warning(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=True,
        is_upgrade=is_upgrade,
        expected_shared_events=[],
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    expected_warning = "WARNING: Mandatory events are present in the manifest file. Automatically authorizing event sharing in dev mode. To suppress this warning, please add 'share_mandatory_events: true' in the application telemetry section."
    mock_console.warning.assert_called_with(expected_warning)


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_and_authorization_not_specified_on_create_and_prod_mode_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
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
        programming_errno=APPLICATION_REQUIRES_TELEMETRY_SHARING,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
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


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_enforced_events_sharing_with_mandatory_events_and_authorization_not_specified_on_update_and_prod_mode_then_no_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
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
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_shared_events_with_no_enabled_mandatory_events_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        expected_authorize_telemetry_flag=share_mandatory_events,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        optional_shared_events=["DEBUG_LOGS"],
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


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.NA_EVENT_SHARING_V2: "true",
        UIParameter.NA_ENFORCE_MANDATORY_FILTERS: "true",
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
def test_shared_events_with_authorization_then_success(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    share_mandatory_events,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    shared_events = ["DEBUG_LOGS", "ERRORS_AND_WARNINGS"]
    mock_execute_query_expected = _setup_mocks_for_app(
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
    )
    mock_conn.return_value = MockConnectionCtx()
    _setup_project(
        manifest_contents=manifest_contents,
        share_mandatory_events=share_mandatory_events,
        optional_shared_events=shared_events,
    )
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()
