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

import os
from pathlib import Path
from textwrap import dedent
from typing import List
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.schemas.entities.common import SqlScriptHookType
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)
from snowflake.cli.api.project.util import TEST_RESOURCE_SUFFIX_VAR

CURRENT_ROLE = "current_role"


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@mock.patch.dict(os.environ, {"USER": "test_user"}, clear=True)
def test_project_model_all_defaults(
    mock_connect, project_definition_files: List[Path], mock_ctx
):
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    project_defn = load_project(project_definition_files).project_definition

    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    assert project.project_root.is_absolute()
    assert project.definition == project_defn.native_app
    assert project.artifacts == [
        PathMapping(src="setup.sql"),
        PathMapping(src="README.md"),
    ]
    assert project.bundle_root == project_dir / "output/bundle"
    assert project.deploy_root == project_dir / "output/deploy"
    assert project.generated_root == project_dir / "output/deploy/__generated"
    assert project.package_scripts == []
    assert project.project_identifier == "minimal"
    assert project.package_name == "minimal_pkg_test_user"
    assert project.stage_fqn == "minimal_pkg_test_user.app_src.stage"
    assert (
        project.scratch_stage_fqn
        == "minimal_pkg_test_user.app_src.stage_snowflake_cli_scratch"
    )
    assert project.stage_schema == "app_src"
    assert project.package_warehouse == "MockWarehouse"
    assert project.application_warehouse == "MockWarehouse"
    assert project.package_role == "MockRole"
    assert project.package_distribution == "internal"
    assert project.app_name == "minimal_test_user"
    assert project.app_role == "MockRole"
    assert project.app_post_deploy_hooks is None
    assert project.debug_mode is None


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@mock.patch.dict(
    os.environ,
    {"USER": "test_user", TEST_RESOURCE_SUFFIX_VAR: "_suffix!"},
    clear=True,
)
def test_project_model_default_package_app_name_with_suffix(
    mock_connect, project_definition_files: List[Path], mock_ctx
):
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    project_defn = load_project(project_definition_files).project_definition

    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    assert project.package_name == '"minimal_pkg_test_user_suffix!"'
    assert project.app_name == '"minimal_test_user_suffix!"'


@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@mock.patch.dict(os.environ, {"USER": "test_user"}, clear=True)
def test_project_model_all_explicit(mock_connect, mock_ctx):
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    project_defition_file_yml = dedent(
        f"""
        definition_version: 1.1
        native_app:
          name: minimal
          
          artifacts:
            - setup.sql
            - README.md
          
          package:
            name: minimal_test_pkg
            role: PkgRole
            distribution: external
            warehouse: PkgWarehouse
            scripts:
              - scripts/package_setup.sql
          
          application:
            name: minimal_test_app
            warehouse: AppWarehouse
            role: AppRole
            debug: false
            post_deploy:
                - sql_script: scripts/app_setup.sql
    
    """
    )

    project_defn = build_project_definition(
        **yaml.load(project_defition_file_yml, Loader=yaml.BaseLoader)
    )
    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    assert project.project_root.is_absolute()
    assert project.definition == project_defn.native_app
    assert project.artifacts == [
        PathMapping(src="setup.sql"),
        PathMapping(src="README.md"),
    ]
    assert project.bundle_root == project_dir / "output/bundle"
    assert project.deploy_root == project_dir / "output/deploy"
    assert project.generated_root == project_dir / "output/deploy/__generated"
    assert project.package_scripts == ["scripts/package_setup.sql"]
    assert project.project_identifier == "minimal"
    assert project.package_name == "minimal_test_pkg"
    assert project.stage_fqn == "minimal_test_pkg.app_src.stage"
    assert (
        project.scratch_stage_fqn
        == "minimal_test_pkg.app_src.stage_snowflake_cli_scratch"
    )
    assert project.stage_schema == "app_src"
    assert project.package_warehouse == "PkgWarehouse"
    assert project.application_warehouse == "AppWarehouse"
    assert project.package_role == "PkgRole"
    assert project.package_distribution == "external"
    assert project.app_name == "minimal_test_app"
    assert project.app_role == "AppRole"
    assert project.app_post_deploy_hooks == [
        SqlScriptHookType(sql_script="scripts/app_setup.sql"),
    ]
    assert project.debug_mode is False


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@mock.patch.dict(
    os.environ,
    {"USER": "test_user", TEST_RESOURCE_SUFFIX_VAR: "_suffix!"},
    clear=True,
)
def test_project_model_explicit_package_app_name_with_suffix(
    mock_connect, project_definition_files: List[Path], mock_ctx
):
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    project_defition_file_yml = dedent(
        f"""
        definition_version: 1.1
        native_app:
          name: minimal
          
          artifacts:
            - setup.sql
            - README.md
          
          package:
            name: minimal_test_pkg
            role: PkgRole
            distribution: external
            warehouse: PkgWarehouse
            scripts:
              - scripts/package_setup.sql
          
          application:
            name: minimal_test_app
            warehouse: AppWarehouse
            role: AppRole
            debug: false
            post_deploy:
                - sql_script: scripts/app_setup.sql
    
    """
    )

    project_defn = build_project_definition(
        **yaml.load(project_defition_file_yml, Loader=yaml.BaseLoader)
    )
    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    assert project.package_name == '"minimal_test_pkg_suffix!"'
    assert project.app_name == '"minimal_test_app_suffix!"'


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
@mock.patch.dict(os.environ, {"USER": "test_user"}, clear=True)
def test_project_model_falls_back_to_current_role(
    mock_connect, project_definition_files: List[Path], mock_ctx, mock_cursor
):
    ctx = mock_ctx(
        cursor=mock_cursor([{"CURRENT_ROLE()": CURRENT_ROLE}], []), role=None
    )
    mock_connect.return_value = ctx

    project_defn = load_project(project_definition_files).project_definition

    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    assert project.app_role == CURRENT_ROLE
    assert project.package_role == CURRENT_ROLE


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
def test_bundle_context_from_project_model(project_definition_files: List[Path]):
    project_defn = load_project(project_definition_files).project_definition
    project_dir = Path().resolve()
    project = NativeAppProjectModel(
        project_definition=project_defn.native_app,
        project_root=project_dir,
    )

    actual_bundle_ctx = project.get_bundle_context()

    expected_bundle_ctx = BundleContext(
        package_name=project.package_name,
        artifacts=[
            PathMapping(src="setup.sql", dest=None),
            PathMapping(src="README.md", dest=None),
        ],
        project_root=project_dir,
        bundle_root=Path(project_dir / "output" / "bundle"),
        deploy_root=Path(project_dir / "output" / "deploy"),
        generated_root=Path(project_dir / "output" / "deploy" / "__generated"),
    )

    assert actual_bundle_ctx == expected_bundle_ctx
