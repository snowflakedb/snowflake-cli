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

import pytest
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.feature_flags import FeatureFlag
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
)

from tests.testing_utils.mock_config import mock_config_key


def test_children_feature_flag_is_disabled():
    assert FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled() == False
    with pytest.raises(AttributeError) as err:
        ApplicationPackageEntityModel(
            **{"type": "application package", "children": [{"target": "some_child"}]}
        )
    assert str(err.value) == "Application package children are not supported yet"


def test_invalid_children_type():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [
                        {
                            # packages cannot contain other packages as children
                            "target": "pkg2"
                        }
                    ],
                },
                "pkg2": {
                    "type": "application package",
                    "artifacts": [],
                },
            },
        }
        with pytest.raises(SchemaValidationError) as err:
            DefinitionV20(**definition_input)
        assert "Target type mismatch" in str(err.value)


def test_invalid_children_target():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [
                        {
                            # no such entity
                            "target": "sl"
                        }
                    ],
                },
            },
        }
        with pytest.raises(SchemaValidationError) as err:
            DefinitionV20(**definition_input)
        assert "No such target: sl" in str(err.value)


def test_valid_children():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [{"target": "sl"}],
                },
                "sl": {"type": "streamlit", "identifier": "my_streamlit"},
            },
        }
        project_definition = DefinitionV20(**definition_input)
        wm = WorkspaceManager(
            project_definition=project_definition,
            project_root="",
        )
        child_entity_id = project_definition.entities["pkg"].children[0]
        child_entity = wm.get_entity(child_entity_id.target)
        assert child_entity.__class__ == StreamlitEntity
