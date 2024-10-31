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
from snowflake.cli._plugins.stage.diff import DiffResult
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.project.definition_manager import DefinitionManager

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

    def drop_application_before_upgrade(cascade: bool = False):
        app.drop_application_before_upgrade(
            console=console or cc,
            app_name=app_model.fqn.identifier,
            app_role=app_model.meta.role,
            policy=policy,
            is_interactive=is_interactive,
            cascade=cascade,
        )

    pkg.action_bundle(action_ctx=ActionContext(get_entity=lambda *args: None))

    return app.create_or_upgrade_app(
        package_model=pkg_model,
        stage_fqn=stage_fqn,
        install_method=install_method,
        drop_application_before_upgrade=drop_application_before_upgrade,
    )


def _setup_project(
    app_pkg_role="package_role",
    app_pkg_warehouse="pkg_warehouse",
    app_role="app_role",
    app_warehouse="app_warehouse",
    setup_sql_contents="CREATE OR ALTER VERSIONED SCHEMA core;",
    readme_contents="\n",
    manifest_contents=test_manifest_contents,
    authorize_event_sharing=None,
    optional_shared_events=None,
):
    telemetry = {}
    if authorize_event_sharing is not None:
        telemetry["authorize_event_sharing"] = authorize_event_sharing
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
    authorize_telemetry_flag=None,
    optional_shared_events=None,
    is_prod=False,
    is_upgrade=False,
):
    if is_upgrade:
        return _setup_mocks_for_upgrade_app(
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            authorize_telemetry_flag=authorize_telemetry_flag,
            optional_shared_events=optional_shared_events,
            is_prod=is_prod,
        )
    else:
        return _setup_mocks_for_create_app(
            mock_execute_query,
            mock_cursor,
            mock_get_existing_app_info,
            authorize_telemetry_flag=authorize_telemetry_flag,
            optional_shared_events=optional_shared_events,
            is_prod=is_prod,
        )


def _setup_mocks_for_create_app(
    mock_execute_query,
    mock_cursor,
    mock_get_existing_app_info,
    authorize_telemetry_flag=None,
    optional_shared_events=None,
    is_prod=False,
):
    mock_get_existing_app_info.return_value = None

    authorize_telemetry_clause = ""
    if authorize_telemetry_flag is not None:
        authorize_telemetry_clause = (
            f" AUTHORIZE_TELEMETRY_EVENT_SHARING = {authorize_telemetry_flag}".upper()
        )
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
            None,
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
    ]

    if optional_shared_events is not None:
        calls.append(
            (
                None,
                mock.call(
                    f"""alter application myapp set shared telemetry events ('{"', '".join(optional_shared_events)}')"""
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
    authorize_telemetry_flag=None,
    optional_shared_events=None,
    is_prod=False,
):
    mock_get_existing_app_info.return_value = {"comment": "GENERATED_BY_SNOWFLAKECLI"}
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
    ]

    if authorize_telemetry_flag is not None:
        calls.append(
            (
                None,
                mock.call(
                    f"alter application myapp set AUTHORIZE_TELEMETRY_EVENT_SHARING = {str(authorize_telemetry_flag).upper()}"
                ),
            ),
        )

    if optional_shared_events is not None:
        calls.append(
            (
                None,
                mock.call(
                    f"""alter application myapp set shared telemetry events ('{"', '".join(optional_shared_events)}')"""
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
        UIParameter.EVENT_SHARING_V2: "false",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "false",
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
    "authorize_event_sharing",
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
    authorize_event_sharing,
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
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
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
        UIParameter.EVENT_SHARING_V2: "false",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "false",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
        test_manifest_with_mandatory_events,
    ],
)
@pytest.mark.parametrize("authorize_event_sharing", [True])
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
    authorize_event_sharing,
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
        authorize_telemetry_flag=None,  # treat it as unset
        is_upgrade=is_upgrade,
    )

    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_has_calls(
        [
            mock.call(
                "WARNING: Same-account event sharing is not enabled in your account, therefore, application telemetry field will be ignored."
            )
        ]
    )


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "false",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [
        test_manifest_contents,
    ],
)
@pytest.mark.parametrize("authorize_event_sharing", [True, False, None])
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
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
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
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "false",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [True, False, None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
def test_event_sharing_enabled_with_mandatory_events_and_explicit_authorization_then_flag_respected_with_potential_warning(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    # Warn if the flag is not set, but there are mandatory events
    if authorize_event_sharing:
        mock_console.warning.assert_not_called()
    else:
        mock_console.warning.assert_has_calls(
            [
                mock.call(
                    "WARNING: Mandatory events are present in the manifest file, but event sharing is not authorized in the application telemetry field. This will soon be required to set in order to deploy applications."
                )
            ]
        )


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_contents],
)
@pytest.mark.parametrize("authorize_event_sharing", [True, False, None])
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
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
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
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [True])
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
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
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
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [False])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_refused_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "Mandatory events are present in the manifest file, but event sharing is not authorized in the application telemetry field. This is required to deploy applications."
    )
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [None])
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
    authorize_event_sharing,
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
        authorize_telemetry_flag=True,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    expected_warning = "WARNING: Mandatory events are present in the manifest file. Automatically authorizing event sharing in dev mode. To suppress this warning, please add authorize_event_sharing field in the application telemetry section."
    mock_console.warning.assert_has_calls([mock.call(expected_warning)])


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [None])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
def test_enforced_events_sharing_with_mandatory_events_and_authorization_not_specified_and_prod_mode_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    authorize_event_sharing,
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
        authorize_telemetry_flag=True,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "Mandatory events are present in the manifest file, but event sharing is not authorized in the application telemetry field. This is required to deploy applications."
    )
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events],
)
@pytest.mark.parametrize("authorize_event_sharing", [None, False])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False])
def test_optional_shared_events_with_no_authorization_then_error(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    authorize_event_sharing,
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
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
        optional_shared_events=["DEBUG_LOGS"],
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    with pytest.raises(ClickException) as e:
        _create_or_upgrade_app(
            policy=MagicMock(),
            install_method=install_method,
            console=mock_console,
        )

    assert (
        e.value.message
        == "telemetry.authorize_event_sharing is required to be true in order to use telemetry.optional_shared_events."
    )
    mock_console.warning.assert_not_called()


@mock.patch(APP_ENTITY_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_connection()
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={
        UIParameter.EVENT_SHARING_V2: "true",
        UIParameter.ENFORCE_MANDATORY_FILTERS: "true",
    },
)
@pytest.mark.parametrize(
    "manifest_contents",
    [test_manifest_with_mandatory_events, test_manifest_contents],
)
@pytest.mark.parametrize("authorize_event_sharing", [True])
@pytest.mark.parametrize(
    "install_method",
    [
        SameAccountInstallMethod.unversioned_dev(),
        SameAccountInstallMethod.release_directive(),
    ],
)
@pytest.mark.parametrize("is_upgrade", [False, True])
def test_optional_shared_events_with_authorization_then_success(
    mock_param,
    mock_conn,
    mock_execute_query,
    mock_get_existing_app_info,
    manifest_contents,
    authorize_event_sharing,
    install_method,
    is_upgrade,
    temp_dir,
    mock_cursor,
):
    optional_shared_events = ["DEBUG_LOGS", "ERRORS_AND_WARNINGS"]
    mock_execute_query_expected = _setup_mocks_for_app(
        mock_execute_query,
        mock_cursor,
        mock_get_existing_app_info,
        is_prod=not install_method.is_dev_mode,
        authorize_telemetry_flag=authorize_event_sharing,
        is_upgrade=is_upgrade,
        optional_shared_events=optional_shared_events,
    )
    mock_conn.return_value = MockConnectionCtx()
    mock_diff_result = DiffResult()
    _setup_project(
        manifest_contents=manifest_contents,
        authorize_event_sharing=authorize_event_sharing,
        optional_shared_events=optional_shared_events,
    )
    assert not mock_diff_result.has_changes()
    mock_console = MagicMock()

    _create_or_upgrade_app(
        policy=MagicMock(),
        install_method=install_method,
        console=mock_console,
    )

    assert mock_execute_query.mock_calls == mock_execute_query_expected
    mock_console.warning.assert_not_called()
