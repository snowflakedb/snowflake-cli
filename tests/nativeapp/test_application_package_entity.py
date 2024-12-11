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

from pathlib import Path
from unittest import mock

import pytest
import yaml
from click import ClickException
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.nativeapp.constants import (
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.utils import (
    APP_PACKAGE_ENTITY,
    APPLICATION_PACKAGE_ENTITY_MODULE,
    SQL_EXECUTOR_EXECUTE,
    SQL_FACADE_GET_UI_PARAMETER,
    SQL_FACADE_MODIFY_RELEASE_DIRECTIVE,
    SQL_FACADE_SET_RELEASE_DIRECTIVE,
    SQL_FACADE_SHOW_RELEASE_CHANNELS,
    SQL_FACADE_SHOW_RELEASE_DIRECTIVES,
    SQL_FACADE_UNSET_RELEASE_DIRECTIVE,
    mock_execute_helper,
)


def _get_app_pkg_entity(project_directory):
    with project_directory("workspaces_simple") as project_root:
        with Path(project_root / "snowflake.yml").open() as definition_file_path:
            project_definition = yaml.safe_load(definition_file_path)
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
def test_deploy(
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

    app_pkg, bundle_ctx, mock_console = _get_app_pkg_entity(project_directory)

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
        stage_schema="app_src",
        bundle_map=mock.ANY,
        role="app_role",
        prune=False,
        recursive=False,
        stage_fqn="pkg.app_src.stage",
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
def test_version_list(
    mock_execute, application_package_entity, action_context, mock_cursor
):
    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([("old_role",)], []),
                mock.call("select current_role()"),
            ),
            (None, mock.call(f"use role {pkg_model.meta.role}")),
            (
                mock_cursor([], []),
                mock.call(f"show versions in application package {pkg_model.fqn.name}"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects
    application_package_entity.action_version_list(action_context)
    assert mock_execute.mock_calls == expected


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
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel=None,
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "my_channel"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "my_directive"}])
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

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_list(
            action_ctx=action_context, release_channel="non_default", like="%%"
        )

    assert (
        str(e.value)
        == f"Release channel non_default does not exist in application package {pkg_model.fqn.name}."
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "my_channel"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "my_directive"}])
def test_given_channels_enabled_and_invalid_channel_selected_when_release_directive_list_then_error(
    show_release_directives,
    show_release_channels,
    application_package_entity,
    action_context,
):

    pkg_model = application_package_entity._entity_model  # noqa SLF001
    pkg_model.meta.role = "package_role"

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_list(
            action_ctx=action_context, release_channel="invalid_channel", like="%%"
        )

    assert (
        str(e.value)
        == f"Release channel invalid_channel does not exist in application package {pkg_model.fqn.name}."
    )
    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "my_channel"}])
@mock.patch(SQL_FACADE_SHOW_RELEASE_DIRECTIVES, return_value=[{"name": "my_directive"}])
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

    assert result == [{"name": "my_directive"}]

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    show_release_directives.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        release_channel="my_channel",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "test_channel"}])
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


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "test_channel"}])
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

    with pytest.raises(ClickException) as e:
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
        == f"Release channel non_default does not exist in application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    set_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "test_channel"}])
@mock.patch(SQL_FACADE_MODIFY_RELEASE_DIRECTIVE)
def test_given_named_directive_with_no_accounts_when_release_directive_set_then_modify_existing_directive(
    modify_release_directive,
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

    modify_release_directive.assert_called_once_with(
        package_name=pkg_model.fqn.name,
        role=pkg_model.meta.role,
        version="1.0",
        patch=2,
        release_channel="test_channel",
        release_directive="directive",
    )


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "test_channel"}])
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
@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "test_channel"}])
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
    return_value=[{"name": "my_channel"}, {"name": "default"}],
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


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "my_channel"}])
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

    with pytest.raises(ClickException) as e:
        application_package_entity.action_release_directive_unset(
            action_ctx=action_context,
            release_channel="non_default",
            release_directive="directive",
        )

    assert (
        str(e.value)
        == f"Release channel non_default does not exist in application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "my_channel"}])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_channels_enabled_and_non_existing_channel_selected_when_release_directive_unset_then_error(
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
            release_channel="non_existing",
            release_directive="directive",
        )

    assert (
        str(e.value)
        == f"Release channel non_existing does not exist in application package {pkg_model.fqn.name}."
    )

    show_release_channels.assert_called_once_with(
        pkg_model.fqn.name, pkg_model.meta.role
    )

    unset_release_directive.assert_not_called()


@mock.patch(SQL_FACADE_SHOW_RELEASE_CHANNELS, return_value=[{"name": "default"}])
@mock.patch(SQL_FACADE_UNSET_RELEASE_DIRECTIVE)
def test_given_default_directive_selected_when_directive_unset_then_error(
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
