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


import pytest
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.entities import (
    ALL_ENTITIES,
    ALL_ENTITY_MODELS,
    v2_entity_model_to_entity_map,
    v2_entity_model_types_map,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
)

from tests.testing_utils.mock_config import mock_config_key


@pytest.mark.parametrize(
    "definition_input,expected_error",
    [
        [{}, "Your project definition is missing the following field: 'entities'"],
        [{"entities": {}}, None],
        [{"entities": {}, "defaults": {}, "env": {}}, None],
        [
            {"entities": {}, "extra": "field"},
            "You provided field 'extra' with value 'field' that is not supported in given version",
        ],
        [
            {"entities": {"entity": {"type": "invalid_type"}}},
            "Input tag 'invalid_type' found using 'type' does not match any of the expected tags",
        ],
        # Application package tests
        [
            {"entities": {"pkg": {"type": "application package"}}},
            [
                "missing the following field: 'entities.pkg.application package.artifacts'",
                "missing the following field: 'entities.pkg.application package.manifest'",
            ],
        ],
        [
            {
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "",
                        "artifacts": [],
                        "manifest": "",
                    }
                }
            },
            None,
        ],
        [
            {
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "",
                        "artifacts": [],
                        "manifest": "",
                        "bundle_root": "",
                        "deploy_root": "",
                        "generated_root": "",
                        "stage": "stage",
                        "scratch_stage": "scratch_stage",
                        "distribution": "internal",
                    }
                }
            },
            None,
        ],
        [
            {
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "",
                        "artifacts": [],
                        "manifest": "",
                        "distribution": "invalid",
                    }
                }
            },
            "Input should be 'internal', 'external', 'INTERNAL' or 'EXTERNAL'",
        ],
        # Application tests
        [
            {"entities": {"app": {"type": "application"}}},
            [
                "Your project definition is missing the following field: 'entities.app.application.from'",
            ],
        ],
        [
            {
                "entities": {
                    "app": {
                        "type": "application",
                        "identifier": "",
                        "from": {"target": "non_existing"},
                    }
                }
            },
            "No such target: non_existing",
        ],
        [
            {
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "",
                        "artifacts": [],
                        "manifest": "",
                    },
                    "app": {
                        "type": "application",
                        "identifier": "",
                        "from": {"target": "pkg"},
                    },
                }
            },
            None,
        ],
        # Meta fields
        [
            {
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "",
                        "artifacts": [],
                        "manifest": "",
                        "meta": {
                            "warehouse": "warehouse",
                            "role": "role",
                            "post_deploy": [{"sql_script": "script.sql"}],
                        },
                    },
                    "app": {
                        "type": "application",
                        "identifier": "",
                        "from": {"target": "pkg"},
                        "meta": {
                            "warehouse": "warehouse",
                            "role": "role",
                            "post_deploy": [{"sql_script": "script.sql"}],
                        },
                    },
                }
            },
            None,
        ],
    ],
)
def test_project_definition_v2_schema(definition_input, expected_error):
    definition_input["definition_version"] = "2"
    with mock_config_key("enable_project_definition_v2", True):
        try:
            DefinitionV20(**definition_input)
        except SchemaValidationError as err:
            if expected_error:
                if type(expected_error) == str:
                    assert expected_error in str(err)
                else:
                    for err_msg in expected_error:
                        assert err_msg in str(err)
            else:
                raise err


def test_identifiers():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "A": {
                "type": "application package",
                "artifacts": [],
                "manifest": "",
            },
            "B": {"type": "streamlit", "identifier": "foo_streamlit"},
            "C": {
                "type": "application",
                "from": {"target": "A"},
                "identifier": {"name": "foo_app", "schema": "schema_value"},
            },
            "D": {
                "type": "application",
                "from": {"target": "A"},
                "identifier": {
                    "name": "foo_app_2",
                    "schema": "schema_value",
                    "database": "db_value",
                },
            },
        },
    }
    project = DefinitionV20(**definition_input)
    entities = project.entities

    assert entities["A"].fqn.identifier == "A"
    assert entities["A"].entity_id == "A"

    assert entities["B"].fqn.identifier == "foo_streamlit"
    assert entities["B"].entity_id == "B"

    assert entities["C"].fqn.identifier == "schema_value.foo_app"
    assert entities["C"].entity_id == "C"

    assert entities["D"].fqn.identifier == "db_value.schema_value.foo_app_2"
    assert entities["D"].entity_id == "D"


def test_defaults_are_applied():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "pkg": {
                "type": "application package",
                "identifier": "",
                "artifacts": [],
                "manifest": "",
            }
        },
        "defaults": {"stage": "default_stage"},
    }
    with mock_config_key("enable_project_definition_v2", True):
        project = DefinitionV20(**definition_input)
        assert project.entities["pkg"].stage == "default_stage"


def test_defaults_do_not_override_values():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "pkg": {
                "type": "application package",
                "identifier": "",
                "artifacts": [],
                "manifest": "",
                "stage": "pkg_stage",
            }
        },
        "defaults": {"stage": "default_stage"},
    }
    with mock_config_key("enable_project_definition_v2", True):
        project = DefinitionV20(**definition_input)
        assert project.entities["pkg"].stage == "pkg_stage"


# Verify that each entity model type has the correct "type" field
def test_entity_types():
    for entity_type, entity_class in v2_entity_model_types_map.items():
        model_entity_type = entity_class.get_type()
        assert model_entity_type == entity_type


# Verify that each entity class has a corresponding entity model class, and that all entities are covered
def test_entity_model_to_entity_map():
    entities = set(ALL_ENTITIES)
    entity_models = set(ALL_ENTITY_MODELS)
    assert len(entities) == len(entity_models)
    for entity_model_class, entity_class in v2_entity_model_to_entity_map.items():
        entities.remove(entity_class)
        entity_models.remove(entity_model_class)
    assert len(entities) == 0
    assert len(entity_models) == 0
