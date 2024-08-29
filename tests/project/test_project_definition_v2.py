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
from pathlib import Path

import pytest
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.entities import (
    ALL_ENTITIES,
    ALL_ENTITY_MODELS,
    v2_entity_model_to_entity_map,
    v2_entity_model_types_map,
)
from snowflake.cli.api.project.schemas.entities.snowpark_entity import (
    PathMapping,
    SnowparkEntityModel,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
)
from snowflake.cli.api.project.schemas.snowpark.callable import _CallableBase


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
        # Snowpark fields
        [
            {
                "defaults": {"stage": "dev"},
                "entities": {
                    "function1": {
                        "type": "function",
                        "identifier": "name",
                        "handler": "app.hello",
                        "returns": "string",
                        "signature": [{"name": "name", "type": "string"}],
                        "runtime": "3.10",
                        "artifacts": ["src"],
                    }
                },
            },
            None,
        ],
        [
            {
                "mixins": {
                    "snowpark_shared": {
                        "stage": "dev",
                        "artifacts": [{"src": "src", "dest": "my_project"}],
                    }
                },
                "entities": {
                    "procedure1": {
                        "type": "procedure",
                        "identifier": "name",
                        "handler": "app.hello",
                        "returns": "string",
                        "signature": [{"name": "name", "type": "string"}],
                        "runtime": "3.10",
                        "artifacts": ["src"],
                        "execute_as_caller": True,
                        "meta": {"use_mixins": ["snowpark_shared"]},
                    }
                },
            },
            None,
        ],
        [
            {
                "mixins": {
                    "snowpark_shared": {
                        "stage": "dev",
                        "artifacts": [{"src": "src", "dest": "my_project"}],
                    }
                },
                "entities": {
                    "procedure1": {
                        "type": "procedure",
                        "handler": "app.hello",
                        "returns": "string",
                        "signature": [{"name": "name", "type": "string"}],
                        "runtime": "3.10",
                        "execute_as_caller": True,
                        "meta": {"use_mixins": ["snowpark_shared"]},
                    }
                },
            },
            [
                "Your project definition is missing the following field: 'entities.procedure1.procedure.name'",
            ],
        ],
        [
            {"entities": {"function1": {"type": "function", "handler": "app.hello"}}},
            [
                "Your project definition is missing the following field: 'entities.function1.function.returns'",
                "Your project definition is missing the following field: 'entities.function1.function.signature'",
            ],
        ],
    ],
)
def test_project_definition_v2_schema(definition_input, expected_error):
    definition_input["definition_version"] = "2"
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


@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_functions",
        "snowpark_procedures",
        "snowpark_function_fully_qualified_name",
    ],
)
def test_v1_to_v2_conversion(
    project_directory, project_name: str
):  # project_name: str, expected_values: Dict[str, Any]):

    with project_directory(project_name) as project_dir:
        definition_v1 = DefinitionManager(project_dir).project_definition
        definition_v2 = convert_project_definition_to_v2(definition_v1)
        assert definition_v2.definition_version == "2"
        assert (
            definition_v1.snowpark.project_name
            == definition_v2.mixins["snowpark_shared"]["artifacts"][0]["dest"]
        )
        assert len(definition_v1.snowpark.procedures) == len(
            definition_v2.get_entities_by_type("procedure")
        )
        assert len(definition_v1.snowpark.functions) == len(
            definition_v2.get_entities_by_type("function")
        )

        artifact = PathMapping(
            src=Path(definition_v1.snowpark.src),
            dest=definition_v1.snowpark.project_name,
        )
        for v1_procedure in definition_v1.snowpark.procedures:
            v2_procedure = definition_v2.entities.get(v1_procedure.name)
            assert v2_procedure
            assert v2_procedure.artifacts == [artifact]
            assert "snowpark_shared" in v2_procedure.meta.use_mixins
            _assert_entities_are_equal(v1_procedure, v2_procedure)

        for v1_function in definition_v1.snowpark.functions:
            v2_function = definition_v2.entities.get(v1_function.name)
            assert v2_function
            assert v2_function.artifacts == [artifact]
            assert "snowpark_shared" in v2_function.meta.use_mixins
            _assert_entities_are_equal(v1_function, v2_function)


# TODO:
# 1. rewrite projects to have one big definition covering all complex positive cases
# 2. Add negative case - entity uses non-existent mixin
@pytest.mark.parametrize(
    "project_name,stage1,stage2",
    [("mixins_basic", "foo", "bar"), ("mixins_defaults_hierarchy", "foo", "baz")],
)
def test_mixins(project_directory, project_name, stage1, stage2):
    with project_directory(project_name) as project_dir:
        definition = DefinitionManager(project_dir).project_definition

    assert definition.entities["function1"].stage == stage1
    assert definition.entities["function1"].handler == "app.hello"
    assert definition.entities["function2"].stage == stage2
    assert definition.entities["function1"].handler == "app.hello"


def test_mixins_for_different_entities(project_directory):
    with project_directory("mixins_different_entities") as project_dir:
        definition = DefinitionManager(project_dir).project_definition

    assert definition.entities["function1"].stage == "foo"
    assert definition.entities["streamlit1"].main_file == "streamlit_app.py"


def test_list_of_mixins_in_correct_order(project_directory):
    with project_directory("mixins_list_applied_in_order") as project_dir:
        definition = DefinitionManager(project_dir).project_definition

    assert definition.entities["function1"].stage == "foo"
    assert definition.entities["function2"].stage == "baz"
    assert definition.entities["streamlit1"].stage == "bar"


def _assert_entities_are_equal(
    v1_entity: _CallableBase, v2_entity: SnowparkEntityModel
):
    assert v1_entity.name == v2_entity.identifier.name
    assert v1_entity.schema_name == v2_entity.identifier.schema_
    assert v1_entity.database == v2_entity.identifier.database
    assert v1_entity.handler == v2_entity.handler
    assert v1_entity.returns == v2_entity.returns
    assert v1_entity.signature == v2_entity.signature
    assert v1_entity.runtime == v2_entity.runtime
