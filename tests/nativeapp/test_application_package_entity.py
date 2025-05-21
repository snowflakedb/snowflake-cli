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
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
import pytz
import yaml
from click import ClickException, UsageError
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.nativeapp.artifacts import VersionInfo
from snowflake.cli._plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.stage.manager import DefaultStagePathParts
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.console import cli_console
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
    ProjectV2Factory,
)
from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY,
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL,
    SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_DIRECTIVE,
    SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL,
    SQL_FACADE_GET_UI_PARAMETER,
    SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL,
    SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_DIRECTIVE,
    SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL,
    SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL,
    SQL_FACADE_SET_RELEASE_DIRECTIVE,
    SQL_FACADE_SHOW_RELEASE_CHANNELS,
    SQL_FACADE_SHOW_RELEASE_DIRECTIVES,
    SQL_FACADE_SHOW_VERSIONS,
    SQL_FACADE_UNSET_RELEASE_DIRECTIVE,
    mock_execute_helper,
)


def _get_app_pkg_entity(
    project_directory, test_dir="workspaces_simple", package_overrides=None
):
    with project_directory(test_dir) as project_root:
        with Path(project_root / "snowflake.yml").open() as definition_file_path:
            project_definition = yaml.safe_load(definition_file_path)
            project_definition["entities"]["pkg"] = dict(
                project_definition["entities"]["pkg"], **(package_overrides or {})
            )
            model = ApplicationPackageEntityModel(
                **project_definition["entities"]["pkg"]
            )
            mock_console = mock.MagicMock()
            workspace_ctx = WorkspaceContext(
                console=mock_console,
                project_root=project_root,
                get_default_role=lambda: "app_role",
                get_default_warehouse=lambda: "wh",
            )
            action_ctx = ActionContext(
                get_entity=lambda *args: None,
            )
            return (
                ApplicationPackageEntity(model, workspace_ctx),
                action_ctx,
                mock_console,
            )


def package_with_subdir_factory():
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg", stage_subdirectory="v1"
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            "setup.sql": "SELECT 1;",
            "README.md": "Hello!",
            "manifest.yml": "\n",
        },
    )


def test_bundle_with_subdir(project_directory):
    package_with_subdir_factory()
    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(
        project_directory, package_overrides={"stage_subdirectory": "v1"}
    )

    bundle_result = app_pkg.action_bundle(bundle_ctx)

    deploy_root = bundle_result.deploy_root()
    assert (deploy_root / "README.md").exists()
    assert (deploy_root / "manifest.yml").exists()
    assert (deploy_root / "setup_script.sql").exists()


def test_bundle(project_directory):
    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)

    bundle_result = app_pkg.action_bundle(bundle_ctx)

    deploy_root = bundle_result.deploy_root()
    assert (deploy_root / "README.md").exists()
    assert (deploy_root / "manifest.yml").exists()
    assert (deploy_root / "setup_script.sql").exists()


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.execute_post_deploy_hooks")
@mock.patch(f"{APP_PACKAGE_ENTITY}.validate_setup_script")
@mock.patch(f"{APPLICATION_PACKAGE_ENTITY_MODULE}.sync_deploy_root_with_stage")
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
@pytest.mark.parametrize("enable_release_channels", [False, True, None])
def test_deploy(
    mock_get_parameter,
    mock_sync,
    mock_validate,
    mock_execute_post_deploy_hooks,
    mock_execute,
    project_directory,
    mock_cursor,
    enable_release_channels,
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "PKG",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "app_role",
                        }
                    ],
                    [],
                ),
                mock.call(
                    r"show application packages like 'PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        ("name", "pkg"),
                        ["owner", "app_role"],
                        ["distribution", "internal"],
                    ],
                    [],
                ),
                mock.call("describe application package pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
        + (
            []
            if enable_release_channels is None
            else [
                (
                    mock_cursor([("old_role",)], []),
                    mock.call("select current_role()"),
                ),
                (None, mock.call("use role app_role")),
                (
                    None,
                    mock.call(
                        f"alter application package pkg\n    set enable_release_channels = {enable_release_channels}\n".lower()
                    ),
                ),
                (None, mock.call("use role old_role")),
            ]
        )
        + [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)
    app_pkg.model.enable_release_channels = enable_release_channels

    app_pkg.action_deploy(
        bundle_ctx,
        prune=False,
        recursive=False,
        paths=["a/b", "c"],
        validate=True,
        interactive=False,
        force=False,
    )

    mock_sync.assert_called_once_with(
        console=mock_console,
        deploy_root=(
            app_pkg._workspace_ctx.project_root / Path("output/deploy")  # noqa SLF001
        ),
        package_name="pkg",
        bundle_map=mock.ANY,
        role="app_role",
        prune=False,
        recursive=False,
        stage_path=DefaultStagePathParts.from_fqn("pkg.app_src.stage"),
        local_paths_to_sync=["a/b", "c"],
        print_diff=True,
    )
    mock_validate.assert_called_once()
    mock_execute_post_deploy_hooks.assert_called_once_with()
    mock_get_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, "ENABLED"
    )
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_EXECUTOR_EXECUTE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.execute_post_deploy_hooks")
@mock.patch(f"{APP_PACKAGE_ENTITY}.validate_setup_script")
@mock.patch(f"{APPLICATION_PACKAGE_ENTITY_MODULE}.sync_deploy_root_with_stage")
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
def test_deploy_w_stage_subdir(
    mock_get_parameter,
    mock_sync,
    mock_validate,
    mock_execute_post_deploy_hooks,
    mock_execute,
    project_directory,
    mock_cursor,
):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "PKG",
                            "comment": SPECIAL_COMMENT,
                            "version": LOOSE_FILES_MAGIC_VERSION,
                            "owner": "app_role",
                        }
                    ],
                    [],
                ),
                mock.call(
                    r"show application packages like 'PKG'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        ("name", "pkg"),
                        ["owner", "app_role"],
                        ["distribution", "internal"],
                    ],
                    [],
                ),
                mock.call("describe application package pkg"),
            ),
            (None, mock.call("use role old_role")),
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(
        project_directory, package_overrides={"stage_subdirectory": "v1"}
    )

    app_pkg.action_deploy(
        bundle_ctx,
        prune=False,
        recursive=False,
        paths=["a/b", "c"],
        validate=True,
        interactive=False,
        force=False,
    )

    project_root = app_pkg._workspace_ctx.project_root  # noqa SLF001
    mock_sync.assert_called_once_with(
        console=mock_console,
        deploy_root=(project_root / Path("output/deploy") / "v1"),
        package_name="pkg",
        bundle_map=mock.ANY,
        role="app_role",
        prune=False,
        recursive=False,
        stage_path=DefaultStagePathParts.from_fqn("pkg.app_src.stage", "v1"),
        local_paths_to_sync=["a/b", "c"],
        print_diff=True,
    )
    mock_validate.assert_called_once()
    mock_execute_post_deploy_hooks.assert_called_once_with()
    mock_get_parameter.assert_called_once_with(
        UIParameter.NA_FEATURE_RELEASE_CHANNELS, "ENABLED"
    )
    assert mock_execute.mock_calls == expected


@mock.patch(SQL_FACADE_SHOW_VERSIONS)
def test_version_list(show_versions, application_package_entity, action_context):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"
    expected_versions = [
        {"version": "1.0", "patch": 1},
        {"version": "1.1", "patch": 2},
    ]

    show_versions.return_value = expected_versions

    result = application_package_entity.action_version_list(action_context)
    assert result == expected_versions

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)


@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@pytest.mark.parametrize(
    "application_package_entity", [{"stage_subdirectory": "v1"}], indirect=True
)
def test_version_list_w_subdir(
    show_versions, application_package_entity, action_context
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"
    expected_versions = [
        {"version": "1.0", "patch": 1},
        {"version": "1.1", "patch": 2},
    ]

    show_versions.return_value = expected_versions

    result = application_package_entity.action_version_list(action_context)
    assert result == expected_versions

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[])
def test_given_channels_disabled_and_no_directives_when_release_directive_list_then_success(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel=None, like="%%"
    )

    assert result == []

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "MY_DIRECTIVE"}])
def test_given_channels_disabled_and_directives_present_when_release_directive_list_then_success(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel=None, like="%%"
    )

    assert result == [{"name": "MY_DIRECTIVE"}]

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(
    SQL_FACADE_SHOW_RELEASE_DIRECTIVES,
    return_value=[{"name": "abcdef"}, {"name": "ghijkl"}],
)
def test_given_multiple_directives_and_like_pattern_when_release_directive_list_then_filter_results(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel=None, like="abc%"
    )

    assert result == [{"name": "abcdef"}]

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "MY_DIRECTIVE"}])
def test_given_channels_enabled_and_no_channel_specified_when_release_directive_list_then_success(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel=None, like="%%"
    )

    assert result == [{"name": "MY_DIRECTIVE"}]

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "my_directive"}])
def test_given_channels_disabled_and_default_channel_selected_when_release_directive_list_then_ignore_channel(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel="default", like="%%"
    )

    assert result == [{"name": "my_directive"}]

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "my_directive"}])
def test_given_channels_disabled_and_non_default_channel_selected_when_release_directive_list_then_error(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_directive_list(
            action_ctx=action_context, release_channel="non_default", like="%%"
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "MY_DIRECTIVE"}])
def test_given_channels_enabled_and_invalid_channel_selected_when_release_directive_list_then_error(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_directive_list(
            action_ctx=action_context, release_channel="invalid_channel", like="%%"
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (MY_CHANNEL)."
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "MY_DIRECTIVE"}])
def test_given_channels_enabled_and_valid_channel_selected_when_release_directive_list_then_success(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    result = application_package_entity.action_release_directive_list(
        action_ctx=action_context, release_channel="my_channel", like="%%"
    )

    assert result == [{"name": "MY_DIRECTIVE"}]

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="my_channel",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "TEST_CHANNEL"}])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_named_directive_with_accounts_when_release_directive_set_then_success(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_set(
        action_ctx=action_context,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="directive",
        target_accounts=["org1.account1", "org2.account2"],
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="directive",
        target_accounts=["org1.account1", "org2.account2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "TEST_CHANNEL"}])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_default_directive_with_no_accounts_when_release_directive_set_then_success(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_set(
        action_ctx=action_context,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="default",
        target_accounts=None,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="default",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_no_channels_with_default_channel_used_when_release_directive_set_then_success(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_set(
        action_ctx=action_context,
        version="1.0",
        patch=2,
        release_channel="default",
        release_directive="default",
        target_accounts=None,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=2,
        release_channel=None,
        release_directive="default",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_no_channels_with_non_default_channel_used_when_release_directive_set_then_error(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_directive_set(
            action_ctx=action_context,
            version="1.0",
            patch=2,
            release_channel="non_default",
            release_directive="default",
            target_accounts=None,
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "TEST_CHANNEL"}])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_named_directive_with_no_accounts_when_release_directive_set_then_modify_existing_directive(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_set(
        action_ctx=action_context,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="directive",
        target_accounts=None,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "TEST_CHANNEL"}])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_default_directive_with_accounts_when_release_directive_set_then_error(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_set(
            action_ctx=action_context,
            version="1.0",
            patch=2,
            release_channel="test_channel",
            release_directive="default",
            target_accounts=["org1.account1", "org2.account2"],
        )

    assert (
        str(e.value)
        == "Target accounts can only be specified for non-default named release directives."
    )

    set_release_directive.assert_not_called()


# test with target_account not in org.account format:
@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "TEST_CHANNEL"}])
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
@pytest.mark.parametrize(
    "account_name", ["org1", "org1.", ".account1", "org1.acc.ount1"]
)
def test_given_invalid_account_names_when_release_directive_set_then_error(
    set_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
    account_name,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_set(
            action_ctx=action_context,
            version="1.0",
            patch=2,
            release_channel="test_channel",
            release_directive="directive",
            target_accounts=[account_name],
        )

    assert (
        str(e.value)
        == f"Target account {account_name} is not in a valid format. Make sure you provide the target account in the format 'org.account'."
    )

    show_release_channels.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(
    SQL_FACADE_SHOW_RELEASE_CHANNELS,
    return_value=[{"name": "MY_CHANNEL"}, {"name": "DEFAULT"}],
)
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_default_channel_selected_when_release_directive_unset_then_success(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_unset(
        action_ctx=action_context,
        release_channel="default",
        release_directive="directive",
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="default",
        release_directive="directive",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_non_default_channel_selected_when_release_directive_unset_then_success(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_unset(
        action_ctx=action_context,
        release_channel="my_channel",
        release_directive="directive",
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="my_channel",
        release_directive="directive",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_disabled_and_default_channel_selected_when_release_directive_unset_then_success(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_unset(
        action_ctx=action_context,
        release_channel="default",
        release_directive="directive",
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
        release_directive="directive",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_disabled_and_non_default_channel_selected_when_release_directive_unset_then_error(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_directive_unset(
            action_ctx=action_context,
            release_channel="non_default",
            release_directive="directive",
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_non_existing_channel_selected_when_release_directive_unset_then_error(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_directive_unset(
            action_ctx=action_context,
            release_channel="non_existing",
            release_directive="directive",
        )

    assert (
        str(e.value)
        == f"Release channel non_existing is not available in application package {pkg_model.fqn.name}. Available release channels are: (MY_CHANNEL)."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "default"}])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_default_directive_selected_when_release_directive_unset_then_error(
    unset_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_unset(
            action_ctx=action_context,
            release_channel="default",
            release_directive="default",
        )

    assert (
        str(e.value)
        == "Cannot unset default release directive. Please specify a non-default release directive."
    )

    show_release_channels.assert_not_called()

    unset_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_add_accounts_to_release_directive_then_success(
    add_accounts_to_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_add_accounts(
        action_ctx=action_context,
        release_channel="my_channel",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    add_accounts_to_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="my_channel",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_DIRECTIVE)
def test_given_channels_disabled_and_add_accounts_to_release_directive_then_success(
    add_accounts_to_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_add_accounts(
        action_ctx=action_context,
        release_channel="default",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    add_accounts_to_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_DIRECTIVE)
def test_given_add_accounts_to_release_directive_with_no_accounts_then_error(
    add_accounts_to_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_add_accounts(
            action_ctx=action_context,
            release_channel="my_channel",
            release_directive="my_directive",
            target_accounts=[],
        )

    assert str(e.value) == "No target accounts provided."

    show_release_channels.assert_not_called()
    add_accounts_to_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_remove_accounts_from_release_directive_then_success(
    remove_accounts_from_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_remove_accounts(
        action_ctx=action_context,
        release_channel="my_channel",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    remove_accounts_from_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="my_channel",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[])
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_DIRECTIVE)
def test_given_channels_disabled_and_remove_accounts_from_release_directive_then_success(
    remove_accounts_from_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    application_package_entity.action_release_directive_remove_accounts(
        action_ctx=action_context,
        release_channel="default",
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    remove_accounts_from_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
        release_directive="my_directive",
        target_accounts=["org1.account1", "org2.account2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "MY_CHANNEL"}])
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_DIRECTIVE)
def test_given_remove_accounts_from_release_directive_with_no_accounts_then_error(
    remove_accounts_from_release_directive,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_remove_accounts(
            action_ctx=action_context,
            release_channel="my_channel",
            release_directive="my_directive",
            target_accounts=[],
        )

    assert str(e.value) == "No target accounts provided."

    show_release_channels.assert_not_called()
    remove_accounts_from_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
def test_given_release_channels_with_proper_values_when_list_release_channels_then_success(
    show_release_channels,
    application_package_entity,
    action_context,
    capsys,
    os_agnostic_snapshot,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    application_package_entity._workspace_ctx.console = cli_console  # noqa SLF001

    pkg_model.meta.role = "package_role"

    created_on_mock = mock.MagicMock()
    updated_on_mock = mock.MagicMock()
    created_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=3, tzinfo=pytz.utc
    )
    updated_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=5, tzinfo=pytz.utc
    )

    release_channels = [
        {
            "name": "CHANNEL1",
            "description": "desc",
            "created_on": created_on_mock,
            "updated_on": updated_on_mock,
            "versions": ["v1", "v2"],
            "targets": {"accounts": ["org1.acc1", "org2.acc2"]},
        },
        {
            "name": "CHANNEL2",
            "description": "desc2",
            "created_on": created_on_mock,
            "updated_on": updated_on_mock,
            "versions": ["v3"],
            "targets": {"accounts": ["org3.acc3"]},
        },
    ]
    show_release_channels.return_value = release_channels

    result = application_package_entity.action_release_channel_list(
        action_context, release_channel=None
    )
    captured = capsys.readouterr()

    assert result == release_channels
    assert captured.out == os_agnostic_snapshot


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
def test_given_release_channel_with_no_target_account_or_version_then_show_all_accounts_in_snapshot(
    show_release_channels,
    application_package_entity,
    action_context,
    capsys,
    os_agnostic_snapshot,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    application_package_entity._workspace_ctx.console = cli_console  # noqa SLF001

    pkg_model.meta.role = "package_role"

    created_on_mock = mock.MagicMock()
    updated_on_mock = mock.MagicMock()
    created_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=3, tzinfo=pytz.utc
    )
    updated_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=5, tzinfo=pytz.utc
    )

    release_channels = [
        {
            "name": "CHANNEL1",
            "description": "desc",
            "created_on": created_on_mock,
            "updated_on": updated_on_mock,
            "versions": [],
            "targets": {},
        }
    ]

    show_release_channels.return_value = release_channels

    result = application_package_entity.action_release_channel_list(
        action_context, release_channel=None
    )
    captured = capsys.readouterr()

    assert result == release_channels
    assert captured.out == os_agnostic_snapshot


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
def test_given_no_release_channels_when_list_release_channels_then_success(
    show_release_channels,
    application_package_entity,
    action_context,
    capsys,
    os_agnostic_snapshot,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    application_package_entity._workspace_ctx.console = cli_console  # noqa SLF001

    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    result = application_package_entity.action_release_channel_list(
        action_context, release_channel=None
    )
    captured = capsys.readouterr()

    assert result == []
    assert captured.out == os_agnostic_snapshot


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
def test_given_release_channels_with_a_selected_channel_to_filter_when_list_release_channels_then_returned_selected_channel(
    show_release_channels,
    application_package_entity,
    action_context,
    capsys,
    os_agnostic_snapshot,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    application_package_entity._workspace_ctx.console = cli_console  # noqa SLF001

    pkg_model.meta.role = "package_role"

    created_on_mock = mock.MagicMock()
    updated_on_mock = mock.MagicMock()
    created_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=3, tzinfo=pytz.utc
    )
    updated_on_mock.astimezone.return_value = datetime(
        year=2024, month=12, day=5, tzinfo=pytz.utc
    )

    test_channel_1 = {
        "name": "CHANNEL1",
        "description": "desc",
        "created_on": created_on_mock,
        "updated_on": updated_on_mock,
        "versions": ["v1", "v2"],
        "targets": {"accounts": ["org1.acc1", "org2.acc2"]},
    }

    test_channel_2 = {
        "name": "CHANNEL2",
        "description": "desc2",
        "created_on": created_on_mock,
        "updated_on": updated_on_mock,
        "versions": ["v3"],
        "targets": {"accounts": ["org3.acc3"]},
    }
    show_release_channels.return_value = [
        test_channel_1,
        test_channel_2,
    ]

    result = application_package_entity.action_release_channel_list(
        action_context, release_channel="channel1"
    )

    assert result == [test_channel_1]
    assert capsys.readouterr().out == os_agnostic_snapshot


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL)
def test_given_release_channel_and_accounts_when_add_accounts_to_release_channel_then_success(
    add_accounts_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    application_package_entity.action_release_channel_add_accounts(
        action_ctx=action_context,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )

    add_accounts_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL)
def test_given_release_channels_disabled_when_add_accounts_to_release_channel_then_error(
    add_accounts_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_add_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    add_accounts_to_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL)
def test_given_invalid_release_channel_when_add_accounts_to_release_channel_then_error(
    add_accounts_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_add_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (TEST_CHANNEL)."
    )

    add_accounts_to_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL)
@pytest.mark.parametrize(
    "account_name", ["org1", "org1.", ".account1", "org1.acc.ount1"]
)
def test_given_invalid_account_names_when_add_accounts_to_release_channel_then_error(
    add_accounts_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
    account_name,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_channel_add_accounts(
            action_ctx=action_context,
            release_channel="test_channel",
            target_accounts=[account_name],
        )

    assert (
        str(e.value)
        == f"Target account {account_name} is not in a valid format. Make sure you provide the target account in the format 'org.account'."
    )

    add_accounts_to_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL)
def test_given_release_channel_and_accounts_when_remove_accounts_from_release_channel_then_success(
    remove_accounts_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    application_package_entity.action_release_channel_remove_accounts(
        action_ctx=action_context,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )

    remove_accounts_from_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL)
def test_given_release_channel_disabled_when_remove_accounts_from_release_channel_then_error(
    remove_accounts_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_remove_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    remove_accounts_from_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL)
def test_given_invalid_release_channel_when_remove_accounts_from_release_channel_then_error(
    remove_accounts_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_remove_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (TEST_CHANNEL)."
    )

    remove_accounts_from_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL)
@pytest.mark.parametrize(
    "account_name", ["org1", "org1.", ".account1", "org1.acc.ount1"]
)
def test_given_invalid_account_names_when_remove_accounts_from_release_channel_then_error(
    remove_accounts_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
    account_name,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_channel_remove_accounts(
            action_ctx=action_context,
            release_channel="test_channel",
            target_accounts=[account_name],
        )

    assert (
        str(e.value)
        == f"Target account {account_name} is not in a valid format. Make sure you provide the target account in the format 'org.account'."
    )

    remove_accounts_from_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL)
def test_given_release_channel_and_accounts_when_set_accounts_for_release_channel_then_success(
    set_accounts_for_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    application_package_entity.action_release_channel_set_accounts(
        action_ctx=action_context,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )

    set_accounts_for_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        target_accounts=["org1.acc1", "org2.acc2"],
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL)
def test_given_release_channels_disabled_when_set_accounts_for_release_channel_then_error(
    set_accounts_for_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_set_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    set_accounts_for_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL)
def test_given_invalid_release_channel_when_set_accounts_for_release_channel_then_error(
    set_accounts_for_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_set_accounts(
            action_ctx=action_context,
            release_channel="invalid_channel",
            target_accounts=["org1.acc1", "org2.acc2"],
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (TEST_CHANNEL)."
    )

    set_accounts_for_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL)
@pytest.mark.parametrize(
    "account_name", ["org1", "org1.", ".account1", "org1.acc.ount1"]
)
def test_given_invalid_account_names_when_set_accounts_for_release_channel_then_error(
    set_accounts_for_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
    account_name,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_channel_set_accounts(
            action_ctx=action_context,
            release_channel="test_channel",
            target_accounts=[account_name],
        )

    assert (
        str(e.value)
        == f"Target account {account_name} is not in a valid format. Make sure you provide the target account in the format 'org.account'."
    )

    set_accounts_for_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
def test_given_release_channel_and_version_when_release_channel_add_version_then_success(
    add_version_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    application_package_entity.action_release_channel_add_version(
        action_ctx=action_context,
        release_channel="test_channel",
        version="1.0",
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    add_version_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="1.0",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
def test_given_release_channels_disabled_when_release_channel_add_version_then_error(
    add_version_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_add_version(
            action_ctx=action_context,
            release_channel="invalid_channel",
            version="1.0",
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    add_version_to_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
def test_given_invalid_release_channel_when_release_channel_add_version_then_error(
    add_version_to_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_add_version(
            action_ctx=action_context,
            release_channel="invalid_channel",
            version="1.0",
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (TEST_CHANNEL)."
    )

    add_version_to_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
def test_given_release_channel_and_version_when_release_channel_remove_version_then_success(
    remove_version_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    application_package_entity.action_release_channel_remove_version(
        action_ctx=action_context,
        release_channel="test_channel",
        version="1.0",
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    remove_version_from_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="1.0",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
def test_given_release_channels_disabled_when_release_channel_remove_version_then_error(
    remove_version_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_remove_version(
            action_ctx=action_context,
            release_channel="invalid_channel",
            version="1.0",
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    remove_version_from_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
def test_given_invalid_release_channel_when_release_channel_remove_version_then_error(
    remove_version_from_release_channel,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL"}]

    with pytest.raises(UsageError) as e:
        application_package_entity.action_release_channel_remove_version(
            action_ctx=action_context,
            release_channel="invalid_channel",
            version="1.0",
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel is not available in application package {pkg_model.fqn.name}. Available release channels are: (TEST_CHANNEL)."
    )

    remove_version_from_release_channel.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_release_channel_and_version_when_publish_then_success(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": []}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]

    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="test_channel",
        release_directive="test_directive",
        version="1.0",
        patch=1,
        interactive=False,
        force=False,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="1.0",
    )
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=1,
        release_channel="test_channel",
        release_directive="test_directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_release_channel_and_version_already_in_channel_when_publish_then_success(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": ["1.0"]}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]

    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="test_channel",
        release_directive="test_directive",
        version="1.0",
        patch=1,
        interactive=False,
        force=False,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=1,
        release_channel="test_channel",
        release_directive="test_directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_release_channels_disabled_when_publish_to_default_channel_then_success(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]
    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="default",
        release_directive="test_directive",
        version="1.0",
        patch=1,
        interactive=False,
        force=False,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=1,
        release_channel=None,
        release_directive="test_directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_release_channels_disabled_when_publish_to_non_default_channel_then_error(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = []
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]
    with pytest.raises(ClickException) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="non_default",
            release_directive="test_directive",
            version="1.0",
            patch=1,
            interactive=False,
            force=False,
        )

    assert (
        str(e.value)
        == f"Release channels are not enabled for application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_non_existing_version_when_publish_then_error(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": []}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.3", "patch": 1}]
    with pytest.raises(ClickException) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version="1.0",
            patch=1,
            interactive=False,
            force=False,
        )

    assert (
        str(e.value)
        == f"Version 1.0 does not exist in application package {pkg_model.fqn.name}. Use --create-version flag to create a new version."
    )

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_non_existing_patch_when_publish_then_error(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": []}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 2}]

    with pytest.raises(ClickException) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version="1.0",
            patch=1,
            interactive=False,
            force=False,
        )

    assert (
        str(e.value)
        == f"Patch 1 does not exist for version 1.0 in application package {pkg_model.fqn.name}. Use --create-version flag to add a new patch."
    )

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_versions_referenced_by_existing_release_directives_when_publish_then_error(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [
        {"name": "TEST_CHANNEL", "versions": ["1.0", "2.0"]}
    ]
    show_release_directives.return_value = [
        {"name": "TEST_DIRECTIVE", "version": "1.0", "patch": 1},
        {"name": "TEST_DIRECTIVE2", "version": "2.0", "patch": 1},
    ]
    show_versions.return_value = [
        {"version": "1.0", "patch": 1},
        {"version": "2.0", "patch": 1},
        {"version": "3.0", "patch": 2},
    ]
    with pytest.raises(ClickException) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version="3.0",
            patch=2,
            interactive=False,
            force=False,
        )

    assert (
        str(e.value)
        == "Maximum number of versions in release channel test_channel reached. Cannot add more versions."
    )

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
    )
    add_version_to_release_channel.assert_not_called()
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_only_one_version_referenced_by_existing_release_directive_when_publish_then_remove_unused_version(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"
    mock_console = mock.MagicMock()
    application_package_entity._workspace_ctx.console = mock_console  # noqa SLF001

    show_release_channels.return_value = [
        {"name": "TEST_CHANNEL", "versions": ["1.0", "2.0"]}
    ]
    show_release_directives.return_value = [
        {"name": "TEST_DIRECTIVE", "version": "1.0", "patch": 1}
    ]
    show_versions.return_value = [
        {"version": "1.0", "patch": 1, "created_on": datetime(2024, 12, 3)},
        {"version": "2.0", "patch": 1, "created_on": datetime(2024, 12, 5)},
        {"version": "3.0", "patch": 2, "created_on": datetime(2024, 12, 6)},
    ]

    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="test_channel",
        release_directive="test_directive",
        version="3.0",
        patch=2,
        interactive=False,
        force=True,
    )

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    remove_version_from_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="2.0",
    )
    add_version_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="3.0",
    )
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="3.0",
        patch=2,
        release_channel="test_channel",
        release_directive="test_directive",
        target_accounts=None,
    )

    mock_console.warning.assert_called_once_with(
        "Maximum number of versions in release channel reached. Removing version 2.0 from release_channel test_channel to make space for version 3.0."
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
def test_given_only_one_version_referenced_by_existing_release_directive_when_publish_non_interactive_no_force_then_error(
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    show_release_channels.return_value = [
        {"name": "TEST_CHANNEL", "versions": ["1.0", "2.0"]}
    ]
    show_release_directives.return_value = [
        {"name": "TEST_DIRECTIVE", "version": "1.0", "patch": 1}
    ]
    show_versions.return_value = [
        {"version": "1.0", "patch": 1, "created_on": datetime(2024, 12, 3)},
        {"version": "2.0", "patch": 1, "created_on": datetime(2024, 12, 5)},
        {"version": "3.0", "patch": 2, "created_on": datetime(2024, 12, 6)},
    ]

    with pytest.raises(ClickException) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version="3.0",
            patch=2,
            interactive=False,
            force=False,
        )

    assert (
        str(e.value)
        == "Cannot proceed with publishing the new version. Please remove an existing version from the release channel to make space for the new version, or use --force to automatically clean up unused versions."
    )

    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    remove_version_from_release_channel.assert_not_called()
    add_version_to_release_channel.assert_not_called()
    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.action_version_create")
def test_given_release_channel_and_version_when_publish_with_create_version_then_success(
    action_version_create,
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    action_version_create.return_value = VersionInfo(
        version_name="1.0", patch_number=1, label=None
    )

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": []}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]

    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="test_channel",
        release_directive="test_directive",
        version="x",
        patch=33,
        interactive=False,
        force=False,
        create_version=True,
    )

    action_version_create.assert_called_once_with(
        action_ctx=action_context,
        version="x",
        patch=33,
        label=None,
        skip_git_check=True,
        interactive=False,
        force=False,
        from_stage=False,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="1.0",
    )
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=1,
        release_channel="test_channel",
        release_directive="test_directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.action_version_create")
def test_given_release_channel_and_no_version_when_publish_with_create_version_then_success(
    action_version_create,
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    action_version_create.return_value = VersionInfo(
        version_name="1.0", patch_number=1, label=None
    )

    show_release_channels.return_value = [{"name": "TEST_CHANNEL", "versions": []}]
    show_release_directives.return_value = [{"name": "TEST_DIRECTIVE"}]
    show_versions.return_value = [{"version": "1.0", "patch": 1}]
    application_package_entity.action_publish(
        action_ctx=action_context,
        release_channel="test_channel",
        release_directive="test_directive",
        version=None,
        patch=None,
        interactive=False,
        force=False,
        create_version=True,
        from_stage=True,
        label="some_label",
    )

    action_version_create.assert_called_once_with(
        action_ctx=action_context,
        version=None,
        patch=None,
        label="some_label",
        skip_git_check=True,
        interactive=False,
        force=False,
        from_stage=True,
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )
    show_release_directives.assert_not_called()
    show_versions.assert_called_once_with(pkg_model.fqn.name, pkg_model.meta.role)
    add_version_to_release_channel.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="test_channel",
        version="1.0",
    )
    remove_version_from_release_channel.assert_not_called()
    set_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=1,
        release_channel="test_channel",
        release_directive="test_directive",
        target_accounts=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.action_version_create")
def test_given_release_channel_and_from_stage_without_create_version_when_publish_then_error(
    action_version_create,
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version=None,
            patch=None,
            interactive=False,
            force=False,
            create_version=False,
            from_stage=True,
        )

    assert (
        str(e.value) == "--from-stage flag can only be used with --create-version flag."
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS)
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES)
@mock.patch(SQL_FACADE_SHOW_VERSIONS)
@mock.patch(SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL)
@mock.patch(SQL_FACADE_SET_RELEASE_DIRECTIVE)
@mock.patch(f"{APP_PACKAGE_ENTITY}.action_version_create")
def test_given_release_channel_and_label_without_create_version_when_publish_then_error(
    action_version_create,
    set_release_directive,
    add_version_to_release_channel,
    remove_version_from_release_channel,
    show_versions,
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(UsageError) as e:
        application_package_entity.action_publish(
            action_ctx=action_context,
            release_channel="test_channel",
            release_directive="test_directive",
            version=None,
            patch=None,
            interactive=False,
            force=False,
            create_version=False,
            label="label",
        )

    assert str(e.value) == "--label can only be used with --create-version flag."


@pytest.mark.parametrize(
    "feature_flag, enable_snowflake_yml, expected",
    [
        (None, None, None),
        (True, None, True),
        (False, None, False),
        (False, True, True),
        (True, False, False),
    ],
)
@mock.patch(f"{APPLICATION_PACKAGE_ENTITY_MODULE}.sync_deploy_root_with_stage")
@mock.patch(SQL_FACADE_GET_UI_PARAMETER, return_value="ENABLED")
@mock.patch("snowflake.cli.api.config.get_config_value")
def test_get_enable_release_channels(
    mock_get_config_value,
    mock_get_parameter,
    mock_sync,
    project_directory,
    mock_cursor,
    feature_flag,
    enable_snowflake_yml,
    expected,
):
    mock_get_config_value.return_value = feature_flag
    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)
    app_pkg.model.enable_release_channels = enable_snowflake_yml
    assert app_pkg._get_enable_release_channels_flag() == expected  # noqa: SLF001
