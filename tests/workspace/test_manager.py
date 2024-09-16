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

import logging
import os
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.nativeapp.patch_utils import mock_connection
from tests.testing_utils.files_and_dirs import create_named_file
from tests.workspace.utils import (
    APP_PACKAGE_ENTITY,
    MOCK_SNOWFLAKE_YML_FILE,
    MOCK_SNOWFLAKE_YML_V1_FILE,
)


@mock_connection()
def _get_ws_manager(mock_connection, pdf_content=MOCK_SNOWFLAKE_YML_FILE):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir_name=current_working_directory,
        contents=[pdf_content],
    )
    dm = DefinitionManager()
    return WorkspaceManager(
        project_definition=dm.project_definition,
        project_root=dm.project_root,
    )


def test_pdf_not_v2(temp_dir):
    with pytest.raises(InvalidProjectDefinitionVersionError):
        _get_ws_manager(pdf_content=MOCK_SNOWFLAKE_YML_V1_FILE)


# Test that the same entity instance is returned for the same id
def test_get_entity_is_cached(temp_dir):
    ws_manager = _get_ws_manager()
    pkg1 = ws_manager.get_entity("pkg")
    pkg2 = ws_manager.get_entity("pkg")
    app = ws_manager.get_entity("app")
    assert pkg1 is pkg2
    assert app is not pkg1


def test_get_entity_invalid_id(temp_dir):
    ws_manager = _get_ws_manager()
    with pytest.raises(ValueError, match="No such entity ID"):
        ws_manager.get_entity("non_existing_id")


def test_bundle(temp_dir):
    ws_manager = _get_ws_manager()
    with mock.patch(f"{APP_PACKAGE_ENTITY}.action_bundle") as app_pkg_bundle_mock:
        ws_manager.perform_action("pkg", EntityActions.BUNDLE)
        app_pkg_bundle_mock.assert_called_once()


def test_bundle_of_invalid_entity_type(temp_dir):
    ws_manager = _get_ws_manager()
    with pytest.raises(
        ValueError, match='This entity type does not support "action_bundle"'
    ):
        ws_manager.perform_action("app", EntityActions.BUNDLE)


def test_migration_already_v2(
    runner,
    project_directory,
):
    with project_directory("migration_already_v2"):
        result = runner.invoke(["ws", "migrate"])

    assert result.exit_code == 0
    assert "Project definition is already at version 2." in result.output


def test_migrations_with_multiple_entities(
    runner, project_directory, os_agnostic_snapshot
):
    with project_directory("migration_multiple_entities"):
        result = runner.invoke(["ws", "migrate"])
    assert result.exit_code == 0
    assert Path("snowflake.yml").read_text() == os_agnostic_snapshot
    assert Path("snowflake_V1.yml").read_text() == os_agnostic_snapshot


def test_migration_native_app_missing_manifest(runner, project_directory):
    with project_directory("migration_multiple_entities") as project_dir:
        (project_dir / "app" / "manifest.yml").unlink()
        result = runner.invoke(["ws", "migrate"])
    assert result.exit_code == 1
    assert "manifest.yml file not found" in result.output


def test_migration_native_app_no_artifacts(runner, project_directory):
    with project_directory("migration_multiple_entities") as project_dir:
        with (project_dir / "snowflake.yml").open("r+") as snowflake_yml:
            pdf = yaml.safe_load(snowflake_yml)
            pdf["native_app"]["artifacts"] = []
            snowflake_yml.seek(0)
            yaml.safe_dump(pdf, snowflake_yml)
            snowflake_yml.truncate()
        result = runner.invoke(["ws", "migrate"])
    assert result.exit_code == 1
    assert "No artifacts mapping found in project definition" in result.output
    assert "Could not bundle Native App artifacts" in result.output


def test_migration_native_app_package_scripts(runner, project_directory):
    with project_directory("migration_package_scripts") as project_dir:
        result = runner.invoke(["ws", "migrate"])
        assert result.exit_code == 0
        package_scripts_dir = project_dir / "package_scripts"
        for file in package_scripts_dir.iterdir():
            assert file.read_text() == dedent(
                """\
                -- Just a demo package script, won't actually be executed in tests
                select * from <% ctx.entities.pkg.identifier %>.my_schema.my_table
                """
            )


@pytest.mark.parametrize(
    "project_directory_name", ["snowpark_templated_v1", "streamlit_templated_v1"]
)
def test_if_template_is_not_rendered_during_migration_with_option_checked(
    runner, project_directory, project_directory_name, os_agnostic_snapshot, caplog
):
    with project_directory(project_directory_name):
        with caplog.at_level(logging.WARNING):
            result = runner.invoke(["ws", "migrate", "--accept-templates"])

    assert result.exit_code == 0
    assert Path("snowflake.yml").read_text() == os_agnostic_snapshot
    assert Path("snowflake_V1.yml").read_text() == os_agnostic_snapshot
    assert (
        "Your V1 definition contains templates. We cannot guarantee the correctness of the migration."
        in caplog.text
    )


@pytest.mark.parametrize(
    "project_directory_name", ["snowpark_templated_v1", "streamlit_templated_v1"]
)
def test_if_template_raises_error_during_migrations(
    runner, project_directory, project_directory_name, os_agnostic_snapshot
):
    with project_directory(project_directory_name):
        result = runner.invoke(["ws", "migrate"])
        assert result.exit_code == 1
        assert "Project definition contains templates" in result.output


def test_migration_with_only_envs(project_directory, runner):
    with project_directory("sql_templating"):
        result = runner.invoke(["ws", "migrate"])

    assert result.exit_code == 0


@pytest.mark.parametrize(
    "duplicated_entity",
    [
        """
    - name: test
      handler: "test"
      signature: ""
      returns: string
      runtime: "3.10"
    """,
        """
streamlit:
  name: test
  stage: streamlit
  query_warehouse: test_warehouse
  main_file: "streamlit_app.py"
  title: "My Fancy Streamlit"
    """,
        """
    - name: test
      handler: "test"
      signature: ""
      returns: string
      handler: test
      runtime: "3.10"
    """,
    ],
)
def test_migrating_a_file_with_duplicated_keys_raises_an_error(
    runner, project_directory, os_agnostic_snapshot, duplicated_entity
):
    with project_directory("snowpark_procedures") as pd:
        definition_path = pd / "snowflake.yml"

        with open(definition_path, "a") as definition_file:
            definition_file.write(duplicated_entity)

        result = runner.invoke(["ws", "migrate"])
    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize("definition_version", [1, "1.1"])
def test_migrate_nativeapp_fields_with_username(
    runner, project_directory, definition_version
):
    with project_directory("integration") as pd:
        definition_path = pd / "snowflake.yml"
        with definition_path.open("r+") as f:
            old_definition = yaml.safe_load(f)
            old_definition["definition_version"] = definition_version
            f.seek(0)
            yaml.safe_dump(old_definition, f)
            f.truncate()

        result = runner.invoke(["ws", "migrate", "--accept-templates"])
        assert result.exit_code == 0, result.output

        with definition_path.open("r") as f:
            new_definition = yaml.safe_load(f)
        assert (
            new_definition["entities"]["app"]["identifier"]
            == "<% fn.concat_ids('integration', '_', fn.sanitize_id(fn.get_username('unknown_user')) | lower) %>"
        )
        assert (
            new_definition["entities"]["pkg"]["identifier"]
            == "<% fn.concat_ids('integration', '_pkg_', fn.sanitize_id(fn.get_username('unknown_user')) | lower) %>"
        )
