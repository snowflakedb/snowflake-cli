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
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.workspace.manager import WorkspaceManager

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


# Test that the same entity instance is returned for the same key
def test_get_entity_is_cached(temp_dir):
    ws_manager = _get_ws_manager()
    pkg1 = ws_manager.get_entity("pkg")
    pkg2 = ws_manager.get_entity("pkg")
    app = ws_manager.get_entity("app")
    assert pkg1 is pkg2
    assert app is not pkg1


def test_get_entity_invalid_key(temp_dir):
    ws_manager = _get_ws_manager()
    with pytest.raises(ValueError, match="No such entity key"):
        ws_manager.get_entity("non_existing_key")


def test_bundle(temp_dir):
    ws_manager = _get_ws_manager()
    with mock.patch(f"{APP_PACKAGE_ENTITY}.bundle") as app_pkg_bundle_mock:
        ws_manager.bundle("pkg")
        app_pkg_bundle_mock.assert_called_once()


def test_bundle_of_invalid_entity_type(temp_dir):
    ws_manager = _get_ws_manager()
    with pytest.raises(ValueError, match="This entity type does not support bundling"):
        ws_manager.bundle("app")
