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
from typing import List, Optional, Set

from snowflake.cli._plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp

NATIVEAPP_MODULE = "snowflake.cli._plugins.nativeapp.manager"
TEARDOWN_MODULE = "snowflake.cli._plugins.nativeapp.teardown_processor"
TYPER_CONFIRM = "typer.confirm"
TYPER_PROMPT = "typer.prompt"
RUN_MODULE = "snowflake.cli._plugins.nativeapp.run_processor"
VERSION_MODULE = "snowflake.cli._plugins.nativeapp.version.version_processor"
ENTITIES_COMMON_MODULE = "snowflake.cli.api.entities.common"
ENTITIES_UTILS_MODULE = "snowflake.cli.api.entities.utils"
APPLICATION_PACKAGE_ENTITY_MODULE = (
    "snowflake.cli._plugins.nativeapp.application_package_entity"
)

CLI_GLOBAL_TEMPLATE_CONTEXT = (
    "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.template_context"
)

TEARDOWN_PROCESSOR = f"{TEARDOWN_MODULE}.NativeAppTeardownProcessor"
NATIVEAPP_MANAGER = f"{NATIVEAPP_MODULE}.NativeAppManager"
RUN_PROCESSOR = f"{RUN_MODULE}.NativeAppRunProcessor"

NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MANAGER}._execute_query"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = f"{NATIVEAPP_MANAGER}._execute_queries"
NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF = (
    f"{NATIVEAPP_MANAGER}.get_app_pkg_distribution_in_snowflake"
)
NATIVEAPP_MANAGER_ACCOUNT_EVENT_TABLE = f"{NATIVEAPP_MANAGER}.account_event_table"
NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME = (
    f"{NATIVEAPP_MANAGER}.verify_project_distribution"
)
NATIVEAPP_MANAGER_GET_EXISTING_APP_PKG_INFO = (
    f"{NATIVEAPP_MANAGER}.get_existing_app_pkg_info"
)
NATIVEAPP_MANAGER_GET_OBJECTS_OWNED_BY_APPLICATION = (
    f"{NATIVEAPP_MANAGER}.get_objects_owned_by_application"
)
NATIVEAPP_MANAGER_BUILD_BUNDLE = f"{NATIVEAPP_MANAGER}.build_bundle"
NATIVEAPP_MANAGER_DEPLOY = f"{NATIVEAPP_MANAGER}.deploy"
NATIVEAPP_MANAGER_VALIDATE = f"{NATIVEAPP_MANAGER}.validate"

TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO = f"{TEARDOWN_PROCESSOR}.get_existing_app_info"
TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO = (
    f"{TEARDOWN_PROCESSOR}.get_existing_app_pkg_info"
)

RUN_PROCESSOR_GET_EXISTING_APP_INFO = f"{RUN_PROCESSOR}.get_existing_app_info"
RUN_PROCESSOR_APP_POST_DEPLOY_HOOKS = f"{RUN_PROCESSOR}.app_post_deploy_hooks"

FIND_VERSION_FROM_MANIFEST = f"{VERSION_MODULE}.find_version_info_in_manifest_file"

APP_ENTITY_MODULE = "snowflake.cli._plugins.nativeapp.application_entity"
APP_ENTITY = f"{APP_ENTITY_MODULE}.ApplicationEntity"
APP_ENTITY_GET_EXISTING_APP_INFO = f"{APP_ENTITY}.get_existing_app_info"
APP_ENTITY_GET_EXISTING_VERSION_INFO = f"{APP_ENTITY}.get_existing_version_info"
APP_ENTITY_DROP_GENERIC_OBJECT = f"{APP_ENTITY_MODULE}.drop_generic_object"
APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION = (
    f"{APP_ENTITY}.get_objects_owned_by_application"
)

APP_PACKAGE_ENTITY = "snowflake.cli._plugins.nativeapp.application_package_entity.ApplicationPackageEntity"
APP_PACKAGE_ENTITY_DISTRIBUTION_IN_SF = (
    f"{APP_PACKAGE_ENTITY}.get_app_pkg_distribution_in_snowflake"
)
APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT = (
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.drop_generic_object"
)
APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO = (
    f"{APP_PACKAGE_ENTITY}.get_existing_app_pkg_info"
)
APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME = (
    f"{APP_PACKAGE_ENTITY}.verify_project_distribution"
)

SQL_EXECUTOR_EXECUTE = f"{ENTITIES_COMMON_MODULE}.SqlExecutor._execute_query"
SQL_EXECUTOR_EXECUTE_QUERIES = f"{ENTITIES_COMMON_MODULE}.SqlExecutor._execute_queries"

mock_snowflake_yml_file = dedent(
    """\
        definition_version: 1
        native_app:
            name: myapp

            source_stage:
                app_src.stage

            artifacts:
                - setup.sql
                - app/README.md
                - src: app/streamlit/*.py
                  dest: ui/

            application:
                name: myapp
                role: app_role
                warehouse: app_warehouse
                debug: true

            package:
                name: app_pkg
                role: package_role
                scripts:
                    - shared_content.sql
    """
)

quoted_override_yml_file = dedent(
    """\
        native_app:
            application:
                name: >-
                    "My Application"
            package:
                name: >-
                    "My Package"
    """
)


def mock_execute_helper(mock_input: list):
    side_effects, expected = map(list, zip(*mock_input))
    return side_effects, expected


# TODO: move to shared utils between integration tests and unit tests once available
def touch(path: str):
    file = Path(path)
    file.parent.mkdir(exist_ok=True, parents=True)
    file.write_text("")


# Helper method, currently only used within assert_dir_snapshot
def _stringify_path(p: Path):
    if p.is_dir():
        return f"d {p}"
    else:
        return f"f {p}"


# Helper method, currently only used within assert_dir_snapshot.
# For all other directory walks in source code, please use available source utils.
def _all_paths_under_dir(root: Path) -> List[Path]:
    check = os.getcwd()
    assert root.is_dir()

    paths: Set[Path] = set()
    for subdir, dirs, files in os.walk(root):
        subdir_path = Path(subdir)
        paths.add(subdir_path)
        for d in dirs:
            paths.add(subdir_path / d)
        for f in files:
            paths.add(subdir_path / f)

    return sorted(paths)


# TODO: move to shared utils between integration tests and unit tests once available
def assert_dir_snapshot(root: Path, os_agnostic_snapshot) -> None:
    all_paths = _all_paths_under_dir(root)

    # Verify the contents of the directory matches expectations
    assert "\n".join([_stringify_path(p) for p in all_paths]) == os_agnostic_snapshot

    # Verify that each file under the directory matches expectations
    for path in all_paths:
        if path.is_file():
            snapshot_contents = f"===== Contents of: {path.as_posix()} =====\n"
            snapshot_contents += path.read_text(encoding="utf-8")
            assert (
                snapshot_contents == os_agnostic_snapshot
            ), f"\nExpected:\n{os_agnostic_snapshot}\nGot:\n{snapshot_contents}"


def create_native_app_project_model(
    project_definition: NativeApp, project_root: Optional[Path] = None
) -> NativeAppProjectModel:
    if project_root is None:
        project_root = Path().resolve()
    return NativeAppProjectModel(
        project_definition=project_definition,
        project_root=project_root,
    )
