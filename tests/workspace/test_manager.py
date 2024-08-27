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
from unittest import mock

import pytest
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.testing_utils.files_and_dirs import create_named_file
from tests.workspace.utils import (
    APP_PACKAGE_ENTITY,
    MOCK_SNOWFLAKE_YML_FILE,
    MOCK_SNOWFLAKE_YML_V1_FILE,
)


def _get_ws_manager(pdf_content=MOCK_SNOWFLAKE_YML_FILE):
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


@pytest.mark.parametrize(
    "project_directory_name",
    ["migration_streamlit_V1_to_V2", "migration_snowpark_V1_to_V2"],
)
def test_migration_v1_to_v2(
    runner, project_directory, snapshot, project_directory_name
):
    with project_directory(project_directory_name):
        result = runner.invoke(["ws", "migrate"])

    assert result.exit_code == 0
    assert "Project definition migrated to version 2." in result.output
    assert Path("snowflake.yml").read_text() == snapshot
    assert Path("snowflake_V1.yml").read_text() == snapshot


@pytest.mark.parametrize(
    "project_directory_name", ["migration_streamlit_V2", "migration_snowpark_V2"]
)
def test_migration_already_v2(runner, project_directory, project_directory_name):
    with project_directory(project_directory_name):
        result = runner.invoke(["ws", "migrate"])

    assert result.exit_code == 0
    assert "Project definition is already at version 2." in result.output


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
