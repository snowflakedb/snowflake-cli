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
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.nativeapp.factories import PdfV10Factory
from tests.nativeapp.patch_utils import mock_connection
from tests.testing_utils.files_and_dirs import create_named_file
from tests.workspace.utils import (
    APP_PACKAGE_ENTITY,
    MOCK_SNOWFLAKE_YML_FILE,
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


def test_pdf_not_v2(temporary_directory):
    pdfv1 = PdfV10Factory.build(
        native_app__source_stage="app_src.stage",
        native_app__artifacts=[{"src": "app/*", "dest": "./"}],
    )
    with pytest.raises(InvalidProjectDefinitionVersionError):
        _get_ws_manager(pdf_content=pdfv1.as_json_str())


# Test that the same entity instance is returned for the same id
def test_get_entity_is_cached(temporary_directory):
    ws_manager = _get_ws_manager()
    pkg1 = ws_manager.get_entity("pkg")
    pkg2 = ws_manager.get_entity("pkg")
    app = ws_manager.get_entity("app")
    assert pkg1 is pkg2
    assert app is not pkg1


def test_get_entity_invalid_id(temporary_directory):
    ws_manager = _get_ws_manager()
    with pytest.raises(ValueError, match="No such entity ID"):
        ws_manager.get_entity("non_existing_id")


def test_bundle(temporary_directory):
    ws_manager = _get_ws_manager()
    with mock.patch(f"{APP_PACKAGE_ENTITY}.action_bundle") as app_pkg_bundle_mock:
        ws_manager.perform_action("pkg", EntityActions.BUNDLE)
        app_pkg_bundle_mock.assert_called_once()


def test_bundle_of_invalid_entity_type(temporary_directory):
    ws_manager = _get_ws_manager()
    with pytest.raises(
        ValueError, match='This entity type does not support "action_bundle"'
    ):
        ws_manager.perform_action("app", EntityActions.BUNDLE)


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

        result = runner.invoke(["helpers", "v1-to-v2", "--accept-templates"])
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
