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

import pytest
import typer
from click import Abort
from snowflake.cli._plugins.nativeapp.constants import (
    SPECIAL_COMMENT,
    SPECIAL_COMMENT_OLD,
)
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    CouldNotDropApplicationPackageWithVersions,
)
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.utils import drop_generic_object
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    INSUFFICIENT_PRIVILEGES,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import mock_get_app_pkg_distribution_in_sf
from tests.nativeapp.utils import (
    APP_ENTITY_DROP_GENERIC_OBJECT,
    APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION,
    APP_ENTITY_MODULE,
    APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT,
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE_GET_EXISTING_APP_INFO,
    TYPER_CONFIRM,
    TYPER_PROMPT,
    mock_execute_helper,
    mock_snowflake_yml_file_v2,
    quoted_override_yml_file_v2,
)
from tests.testing_utils.files_and_dirs import create_named_file


def _drop_application(
    auto_yes: bool,
    interactive: bool = False,
    cascade: bool | None = None,
    console: AbstractConsole | None = None,
):
    dm = DefinitionManager()
    pd = dm.project_definition
    app_model: ApplicationEntityModel = pd.entities["myapp"]
    ctx = WorkspaceContext(
        console=console or cc,
        project_root=dm.project_root,
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )
    app = ApplicationEntity(app_model, ctx)
    return app.action_drop(
        action_ctx=mock.Mock(spec=ActionContext),
        interactive=interactive,
        force_drop=auto_yes,
        cascade=cascade,
    )


def _drop_package(auto_yes: bool, console: AbstractConsole | None = None):
    dm = DefinitionManager()
    pd = dm.project_definition
    pkg_model: ApplicationPackageEntityModel = pd.entities["app_pkg"]
    ctx = WorkspaceContext(
        console=console or cc,
        project_root=dm.project_root,
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )
    pkg = ApplicationPackageEntity(pkg_model, ctx)
    return pkg.action_drop(
        action_ctx=mock.Mock(spec=ActionContext),
        force_drop=auto_yes,
    )


def _teardown(
    console: AbstractConsole,
    interactive: bool,
    force_drop: bool = False,
    cascade: bool | None = None,
):
    _drop_application(
        auto_yes=force_drop, interactive=interactive, cascade=cascade, console=console
    )
    _drop_package(console=console, auto_yes=force_drop)


# Test drop_generic_object() with success
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_drop_generic_object_success(mock_execute, temporary_directory, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("drop application myapp")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    drop_generic_object(
        console=mock.MagicMock(),
        object_type="application",
        object_name="myapp",
        role="app_role",
    )
    assert mock_execute.mock_calls == expected


# Test drop_generic_object() with an exception
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_drop_generic_object_failure_w_exception(
    mock_execute,
    temporary_directory,
    mock_cursor,
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
                ),
                mock.call("drop application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(SnowflakeSQLExecutionError):
        drop_generic_object(
            console=mock.MagicMock(),
            object_type="application package",
            object_name="app_pkg",
            role="package_role",
        )
    assert mock_execute.mock_calls == expected


# Test drop_application() when no application exists
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_no_existing_application(
    mock_get_existing_app_info, auto_yes_param, temporary_directory
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    _drop_application(auto_yes_param, console=mock_console)
    mock_get_existing_app_info.assert_called_once()
    mock_console.warning.assert_called_once_with(
        "Role app_role does not own any application object with the name myapp, or the application object does not exist."
    )


# Test drop_application() when the current role is not allowed to drop it
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(
    APP_ENTITY_DROP_GENERIC_OBJECT,
    side_effect=ProgrammingError(
        msg="Insufficient privileges to operate on application",
        errno=INSUFFICIENT_PRIVILEGES,
    ),
)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION, return_value=[])
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_current_role_is_not_owner(
    mock_get_objects_owned_by_application,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    auto_yes_param,
    temporary_directory,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "different_owner",
        "comment": SPECIAL_COMMENT,
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(ProgrammingError):
        _drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()


# Test drop_application() successfully when it has special comment
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(APP_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION, return_value=[])
@pytest.mark.parametrize(
    "auto_yes_param, special_comment",  # auto_yes should have no effect on the test
    [
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
    ],
)
def test_drop_application_has_special_comment(
    mock_get_objects_owned_by_application,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    auto_yes_param,
    special_comment,
    temporary_directory,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": special_comment,
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()
    mock_drop_generic_object.assert_called_once()


# Test drop_application() successfully when it has special comment but is a quoted string
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION, return_value=[])
@pytest.mark.parametrize(
    "auto_yes_param, special_comment",  # auto_yes should have no effect on the test
    [
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
    ],
)
def test_drop_application_has_special_comment_and_quoted_name(
    mock_get_objects_owned_by_application,
    mock_execute,
    auto_yes_param,
    special_comment,
    temporary_directory,
    mock_cursor,
):
    side_effects, expected = mock_execute_helper(
        [
            # Show apps
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "My Application",
                            "comment": special_comment,
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show applications like 'My Application'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
            # Drop app
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call('drop application "My Application"')),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[quoted_override_yml_file_v2],
    )

    _drop_application(auto_yes_param)
    assert mock_execute.mock_calls == expected


# Test drop_application() without special comment AND auto_yes is False AND should_drop is False
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(APP_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=False)
def test_drop_application_user_prohibits_drop(
    mock_confirm,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    temporary_directory,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": "no_special_comment",
        "created_on": "dummy",
        "source": "dummy",
        "version": "dummy",
        "patch": "dummy",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    with pytest.raises(typer.Abort):
        _drop_application(auto_yes=False, console=mock_console)
    mock_get_existing_app_info.assert_called_once()
    mock_drop_generic_object.assert_not_called()
    mock_console.message.assert_called_once_with(
        "Did not drop application object myapp."
    )


# Test drop_application() without special comment AND auto_yes is False AND should_drop is True
# Test drop_application() without special comment AND auto_yes is True
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(APP_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=True)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION, return_value=[])
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],
)
def test_drop_application_user_allows_drop(
    mock_get_objects_owned_by_application,
    mock_confirm,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    auto_yes_param,
    temporary_directory,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": "no_special_comment",
        "created_on": "dummy",
        "source": "dummy",
        "version": "dummy",
        "patch": "dummy",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()
    mock_drop_generic_object.assert_called_once()


# Test idempotent drop_application()
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(APP_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION, return_value=[])
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],  # This should have no effect on the test
)
def test_drop_application_idempotent(
    mock_get_objects_owned_by_application,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    auto_yes_param,
    temporary_directory,
):
    side_effects_for_get_existing_app_info = [
        {"name": "myapp", "owner": "app_role", "comment": SPECIAL_COMMENT},
        None,
        None,
    ]
    mock_get_existing_app_info.side_effect = side_effects_for_get_existing_app_info

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_application(auto_yes_param)
    _drop_application(auto_yes_param)
    _drop_application(auto_yes_param)

    assert mock_get_existing_app_info.call_count == 3
    mock_drop_generic_object.assert_called_once()


# Test drop_package() when no application package exists
@mock.patch(
    APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO,
    return_value=None,
)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_package_no_existing_application(
    mock_get_existing_app_pkg_info, auto_yes_param, temporary_directory
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    _drop_package(auto_yes_param, mock_console)
    mock_get_existing_app_pkg_info.assert_called_once()
    mock_console.warning.assert_called_once_with(
        "Role package_role does not own any application package with the name app_pkg, or the application package does not exist."
    )


# Test drop_package() when the current role is not allowed to drop it
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(
    APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT,
    side_effect=ProgrammingError(
        msg="Insufficient privileges to operate on application_package",
        errno=INSUFFICIENT_PRIVILEGES,
    ),
)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME, return_value=True)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_package_current_role_is_not_owner(
    mock_is_distribution_same,
    mock_get_distribution,
    mock_drop_generic_object,
    mock_execute,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    temporary_directory,
    mock_cursor,
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "different_owner",
        "comment": SPECIAL_COMMENT,
    }
    mock_get_distribution.return_value = "internal"

    side_effects_for_execute, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects_for_execute

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(ProgrammingError):
        _drop_package(auto_yes_param)
    mock_get_existing_app_pkg_info.assert_called_once()


# Test drop_package when the package has more than 0 existing versions
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
def test_show_versions_failure_w_exception(
    mock_execute,
    mock_get_existing_app_pkg_info,
    temporary_directory,
    mock_cursor,
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "different_owner",
        "comment": "some_comment",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([("row1"), ("row2")], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    with pytest.raises(CouldNotDropApplicationPackageWithVersions):
        _drop_package(auto_yes=False)
    mock_get_existing_app_pkg_info.assert_called_once()


# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is False
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME, return_value=True)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=False)
def test_drop_package_no_mismatch_no_drop(
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_get_existing_app_pkg_info,
    temporary_directory,
    mock_cursor,
):
    mock_get_distribution.return_value = "external"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "some_comment",
        "created_on": "dummy",
        "distribution": "external",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_package(auto_yes=False)
    assert mock_execute.mock_calls == expected


# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is True
# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is True
# Test drop_package when there is distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is True
# Test drop_package when there is distribution mismatch AND distribution = external AND auto_yes is True
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=True)
@mock.patch(APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",
    [(False, True), (False, False), (True, True), (True, False)],
)
def test_drop_package_variable_mismatch_allowed_user_allows_drop(
    mock_drop_generic_object,
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temporary_directory,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "external"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "some_comment",
        "created_on": "dummy",
        "distribution": "external",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    _drop_package(auto_yes_param, mock_console)
    assert mock_execute.mock_calls == expected
    if not is_pkg_distribution_same:
        mock_console.warning.assert_any_call(
            "Dropping application package app_pkg with distribution 'external'."
        )
    if not auto_yes_param:
        mock_console.warning.assert_any_call(
            "Application package app_pkg in your Snowflake account has distribution property 'external' and could be associated with one or more of your listings on Snowflake Marketplace."
        )
    mock_drop_generic_object.assert_called_once()


# Test drop_package when there is no distribution mismatch AND distribution = internal AND special comment is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND special comment is True
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock.patch(APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",  # auto_yes_param should have no effect on the test
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_drop_package_variable_mistmatch_w_special_comment_auto_drop(
    mock_drop_generic_object,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temporary_directory,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": SPECIAL_COMMENT,
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    _drop_package(auto_yes_param, mock_console)
    assert mock_execute.mock_calls == expected
    mock_drop_generic_object.assert_called_once()
    if not is_pkg_distribution_same:
        mock_console.warning.assert_any_call(
            "Dropping application package app_pkg with distribution 'internal'."
        )


# Test drop_package when there is no distribution mismatch AND distribution = internal AND special comment is True AND name is quoted
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@pytest.mark.parametrize(
    "auto_yes_param, special_comment",  # auto_yes should have no effect on the test
    [
        (True, SPECIAL_COMMENT),
        (True, SPECIAL_COMMENT_OLD),
        (False, SPECIAL_COMMENT),
        (False, SPECIAL_COMMENT_OLD),
    ],
)
def test_drop_package_variable_mistmatch_w_special_comment_quoted_name_auto_drop(
    mock_get_distribution,
    mock_execute,
    auto_yes_param,
    special_comment,
    temporary_directory,
    mock_cursor,
):
    mock_get_distribution.return_value = "internal"
    side_effects, expected = mock_execute_helper(
        [
            # Show app pkg
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "My Package",
                            "comment": special_comment,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'My Package'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            # Show versions
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    'show versions in application package "My Package"',
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            # Drop app pkg
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (None, mock.call('drop application package "My Package"')),
            (None, mock.call("use role old_role")),
        ]
    )

    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir_name=current_working_directory,
        contents=[quoted_override_yml_file_v2],
    )

    _drop_package(auto_yes_param)
    assert mock_execute.mock_calls == expected


# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is False
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is False
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=False)
@pytest.mark.parametrize("is_pkg_distribution_same", [True, False])
def test_drop_package_variable_mistmatch_no_special_comment_user_prohibits_drop(
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temporary_directory,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "dummy",
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    mock_console = mock.MagicMock()
    _drop_package(auto_yes=False, console=mock_console)
    assert mock_execute.mock_calls == expected
    if not is_pkg_distribution_same:
        mock_console.warning.assert_any_call(
            "Dropping application package app_pkg with distribution 'internal'."
        )


# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is True
# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is True
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME)
@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_CONFIRM}", return_value=True)
@mock.patch(APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",  # auto_yes_param should have no effect on the test
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_drop_package_variable_mistmatch_no_special_comment_user_allows_drop(
    mock_drop_generic_object,
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temporary_directory,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "dummy",
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_package(auto_yes_param)
    assert mock_execute.mock_calls == expected
    mock_drop_generic_object.assert_called_once()


# Test idempotent drop_package()
@mock.patch(APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO)
@mock.patch(APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME, return_value=True)
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],  # This should have no effect on the test
)
def test_drop_package_idempotent(
    mock_is_distribution_same,
    mock_get_app_pkg_distribution,
    mock_execute,
    mock_drop_generic_object,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    temporary_directory,
    mock_cursor,
):
    mock_get_app_pkg_distribution.return_value = "internal"
    side_effects_for_execute, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    "show versions in application package app_pkg",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects_for_execute

    side_effects_for_get_existing_app_pkg_info = [
        {"name": "app_pkg", "owner": "package_role", "comment": SPECIAL_COMMENT},
        None,
        None,
    ]
    mock_get_existing_app_pkg_info.side_effect = (
        side_effects_for_get_existing_app_pkg_info
    )

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )

    _drop_package(auto_yes_param)
    _drop_package(auto_yes_param)
    _drop_package(auto_yes_param)

    assert mock_get_existing_app_pkg_info.call_count == 3
    mock_drop_generic_object.assert_called_once()
    assert mock_execute.mock_calls == expected


@mock.patch(f"{APP_ENTITY_MODULE}.{TYPER_PROMPT}")
@mock.patch(SQL_FACADE_GET_EXISTING_APP_INFO)
@mock.patch(APP_ENTITY_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION)
@pytest.mark.parametrize(
    "cascade,application_objects,interactive_response,expected_cascade",
    [
        # Cascade true
        [True, [], None, True],
        [True, [{"type": "DATABASE", "name": "db"}], None, True],
        [True, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), None, True],
        # Cascade false
        [False, [], None, False],
        [False, [{"type": "DATABASE", "name": "db"}], None, False],
        [False, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), None, False],
        # Cascade unset
        [None, [], None, False],
        [None, [{"type": "DATABASE", "name": "db"}], None, None],
        [None, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), None, None],
        # Interactive
        [None, [{"type": "DATABASE", "name": "db"}], "yes", True],
        [None, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), "yes", True],
        [None, [{"type": "DATABASE", "name": "db"}], "no", False],
        [None, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), "no", False],
        [None, [{"type": "DATABASE", "name": "db"}], "abort", None],
        [None, ProgrammingError(errno=APPLICATION_NO_LONGER_AVAILABLE), "abort", None],
    ],
)
def test_drop_application_cascade(
    mock_get_objects_owned_by_application,
    mock_drop_generic_object,
    mock_get_existing_app_info,
    mock_typer_prompt,
    cascade,
    application_objects,
    interactive_response,
    expected_cascade,
    temporary_directory,
    capsys,
    os_agnostic_snapshot,
):
    if isinstance(application_objects, Exception):
        mock_get_objects_owned_by_application.side_effect = application_objects
    else:
        mock_get_objects_owned_by_application.return_value = application_objects
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": SPECIAL_COMMENT,
    }
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[mock_snowflake_yml_file_v2],
    )
    interactive = interactive_response is not None
    mock_typer_prompt.return_value = interactive_response

    if expected_cascade is None:
        with pytest.raises(Abort):
            _drop_application(False, interactive, cascade)
    else:
        _drop_application(False, interactive, cascade)
        mock_drop_generic_object.assert_called_once_with(
            console=mock.ANY,
            object_type="application",
            object_name="myapp",
            role="app_role",
            cascade=expected_cascade,
        )
        stdout, _ = capsys.readouterr()
        assert stdout == os_agnostic_snapshot
