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
from typing import List, Set

import pytest
from snowflake.connector import ProgrammingError

from tests.nativeapp.factories import ProjectV10Factory

TYPER_CONFIRM = "typer.confirm"
TYPER_PROMPT = "typer.prompt"
API_MODULE = "snowflake.cli.api"
ENTITIES_UTILS_MODULE = "snowflake.cli.api.entities.utils"
PLUGIN_UTIL_MODULE = "snowflake.cli._plugins.connection.util"
APPLICATION_PACKAGE_ENTITY_MODULE = (
    "snowflake.cli._plugins.nativeapp.entities.application_package"
)

CLI_GLOBAL_TEMPLATE_CONTEXT = (
    "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.template_context"
)

APP_ENTITY_MODULE = "snowflake.cli._plugins.nativeapp.entities.application"
APP_ENTITY = f"{APP_ENTITY_MODULE}.ApplicationEntity"
APP_ENTITY_DROP_GENERIC_OBJECT = f"{APP_ENTITY_MODULE}.drop_generic_object"
APP_ENTITY_GET_OBJECTS_OWNED_BY_APPLICATION = (
    f"{APP_ENTITY}.get_objects_owned_by_application"
)

APP_PACKAGE_ENTITY = "snowflake.cli._plugins.nativeapp.entities.application_package.ApplicationPackageEntity"
APP_PACKAGE_ENTITY_DEPLOY = f"{APP_PACKAGE_ENTITY}._deploy"
APP_PACKAGE_ENTITY_DISTRIBUTION_IN_SF = (
    f"{APP_PACKAGE_ENTITY}.get_app_pkg_distribution_in_snowflake"
)
APP_PACKAGE_ENTITY_DROP_GENERIC_OBJECT = (
    f"{APPLICATION_PACKAGE_ENTITY_MODULE}.drop_generic_object"
)
APP_PACKAGE_ENTITY_GET_EXISTING_APP_PKG_INFO = (
    f"{APP_PACKAGE_ENTITY}.get_existing_app_pkg_info"
)
APP_PACKAGE_ENTITY_GET_EXISTING_VERSION_INFO = (
    f"{APP_PACKAGE_ENTITY}.get_existing_version_info"
)
APP_PACKAGE_ENTITY_IS_DISTRIBUTION_SAME = (
    f"{APP_PACKAGE_ENTITY}.verify_project_distribution"
)

CODE_GEN = "snowflake.cli._plugins.nativeapp.codegen"
TEMPLATE_PROCESSOR = f"{CODE_GEN}.templates.templates_processor"
ARTIFACT_PROCESSOR = f"{CODE_GEN}.artifact_processor"

SQL_EXECUTOR_EXECUTE = f"{API_MODULE}.sql_execution.BaseSqlExecutor.execute_query"
SQL_EXECUTOR_EXECUTE_QUERIES = (
    f"{API_MODULE}.sql_execution.BaseSqlExecutor.execute_queries"
)
GET_UI_PARAMETERS = f"{PLUGIN_UTIL_MODULE}.get_ui_parameters"

SQL_FACADE_MODULE = "snowflake.cli._plugins.nativeapp.sf_facade"
SQL_FACADE = f"{SQL_FACADE_MODULE}.SnowflakeSQLFacade"
SQL_FACADE_GET_ACCOUNT_EVENT_TABLE = f"{SQL_FACADE}.get_account_event_table"
SQL_FACADE_EXECUTE_USER_SCRIPT = f"{SQL_FACADE}.execute_user_script"
SQL_FACADE_STAGE_EXISTS = f"{SQL_FACADE}.stage_exists"
SQL_FACADE_CREATE_SCHEMA = f"{SQL_FACADE}.create_schema"
SQL_FACADE_CREATE_STAGE = f"{SQL_FACADE}.create_stage"
SQL_FACADE_CREATE_APPLICATION = f"{SQL_FACADE}.create_application"
SQL_FACADE_UPGRADE_APPLICATION = f"{SQL_FACADE}.upgrade_application"
SQL_FACADE_GET_EVENT_DEFINITIONS = f"{SQL_FACADE}.get_event_definitions"
SQL_FACADE_GET_EXISTING_APP_INFO = f"{SQL_FACADE}.get_existing_app_info"
SQL_FACADE_GRANT_PRIVILEGES_TO_ROLE = f"{SQL_FACADE}.grant_privileges_to_role"
SQL_FACADE_GET_UI_PARAMETER = f"{SQL_FACADE}.get_ui_parameter"
SQL_FACADE_ALTER_APP_PKG_PROPERTIES = (
    f"{SQL_FACADE}.alter_application_package_properties"
)
SQL_FACADE_CREATE_APP_PKG = f"{SQL_FACADE}.create_application_package"
SQL_FACADE_SHOW_RELEASE_DIRECTIVES = f"{SQL_FACADE}.show_release_directives"
SQL_FACADE_SET_RELEASE_DIRECTIVE = f"{SQL_FACADE}.set_release_directive"
SQL_FACADE_UNSET_RELEASE_DIRECTIVE = f"{SQL_FACADE}.unset_release_directive"
SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_DIRECTIVE = (
    f"{SQL_FACADE}.add_accounts_to_release_directive"
)
SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_DIRECTIVE = (
    f"{SQL_FACADE}.remove_accounts_from_release_directive"
)
SQL_FACADE_SHOW_RELEASE_CHANNELS = f"{SQL_FACADE}.show_release_channels"
SQL_FACADE_DROP_VERSION = f"{SQL_FACADE}.drop_version_from_package"
SQL_FACADE_CREATE_VERSION = f"{SQL_FACADE}.create_version_in_package"
SQL_FACADE_SHOW_VERSIONS = f"{SQL_FACADE}.show_versions"
SQL_FACADE_ADD_ACCOUNTS_TO_RELEASE_CHANNEL = (
    f"{SQL_FACADE}.add_accounts_to_release_channel"
)
SQL_FACADE_REMOVE_ACCOUNTS_FROM_RELEASE_CHANNEL = (
    f"{SQL_FACADE}.remove_accounts_from_release_channel"
)
SQL_FACADE_SET_ACCOUNTS_FOR_RELEASE_CHANNEL = (
    f"{SQL_FACADE}.set_accounts_for_release_channel"
)
SQL_FACADE_ADD_VERSION_TO_RELEASE_CHANNEL = (
    f"{SQL_FACADE}.add_version_to_release_channel"
)
SQL_FACADE_REMOVE_VERSION_FROM_RELEASE_CHANNEL = (
    f"{SQL_FACADE}.remove_version_from_release_channel"
)

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
                warehouse: pkg_warehouse
                scripts:
                    - shared_content.sql
    """
)

mock_snowflake_yml_file_v2 = dedent(
    """\
        definition_version: 2
        entities:
            app_pkg:
                type: application package
                stage: app_src.stage
                manifest: app/manifest.yml
                artifacts:
                    - setup.sql
                    - app/README.md
                    - src: app/streamlit/*.py
                      dest: ui/
                meta:
                    role: package_role
                    warehouse: pkg_warehouse
                    post_deploy:
                        - sql_script: shared_content.sql
            myapp:
                type: application
                debug: true
                from:
                    target: app_pkg
                meta:
                    role: app_role
                    warehouse: app_warehouse
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

quoted_override_yml_file_v2 = dedent(
    """\
        entities:
            myapp:
                identifier: >-
                    "My Application"
            app_pkg:
                identifier: >-
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


# POC to replicate tests/test_data/projects/integration sample project
def use_integration_project():
    package_script_1 = dedent(
        """\
        -- package script (1/2)

        create schema if not exists {{ package_name }}.my_shared_content;
        grant usage on schema {{ package_name }}.my_shared_content
        to share in application package {{ package_name }};
        """
    )
    package_script_2 = dedent(
        """\
        -- package script (2/2)

        create or replace table {{ package_name }}.my_shared_content.shared_table (
        col1 number,
        col2 varchar
        );

        insert into {{ package_name }}.my_shared_content.shared_table (col1, col2)
        values (1, 'hello');

        grant select on table {{ package_name }}.my_shared_content.shared_table
        to share in application package {{ package_name }};
        """
    )
    setup_script = dedent(
        """\
        create application role if not exists app_public;
        create or alter versioned schema core;

            create or replace procedure core.echo(inp varchar)
            returns varchar
            language sql
            immutable
            as
            $$
            begin
                return inp;
            end;
            $$;

            grant usage on procedure core.echo(varchar) to application role app_public;

            create or replace view core.shared_view as select * from my_shared_content.shared_table;

            grant select on view core.shared_view to application role app_public;
    """
    )
    readme_contents = dedent(
        """\
        # README

        This directory contains an extremely simple application that is used for
        integration testing SnowCLI.
    """
    )

    # TODO: create a factory for manifest
    manifest_contents = dedent(
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
    ProjectV10Factory(
        pdf__native_app__name="integration",
        pdf__native_app__artifacts=[
            {"src": "app/*", "dest": "./"},
        ],
        pdf__native_app__package__scripts=[
            "package/001-shared.sql",
            "package/002-shared.sql",
        ],
        files={
            "package/001-shared.sql": package_script_1,
            "package/002-shared.sql": package_script_2,
            "app/setup.sql": setup_script,
            "app/README.md": readme_contents,
            "app/manifest.yml": manifest_contents,
        },
    )


def mock_side_effect_error_with_cause(err: Exception, cause: Exception):
    with pytest.raises(type(err)) as side_effect:
        raise err from cause

    return side_effect.value


def assert_programmingerror_cause_with_errno(err: pytest.ExceptionInfo, errno: int):
    assert isinstance(err.value.__cause__, ProgrammingError)
    assert err.value.__cause__.errno == errno
