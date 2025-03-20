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
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    SnowparkEntityModel,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.schemas.entities.entities import (
    ALL_ENTITIES,
    ALL_ENTITY_MODELS,
    v2_entity_model_to_entity_map,
    v2_entity_model_types_map,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
)
from snowflake.cli.api.project.schemas.v1.snowpark.callable import _CallableBase
from snowflake.cli.api.utils.definition_rendering import render_definition_template

from tests.nativeapp.factories import ProjectV11Factory


@pytest.mark.parametrize(
    "definition_input,expected_error",
    [
        [{}, "Your project definition is missing the following field: 'entities'"],
        [{"entities": {}}, None],
        [{"entities": {}, "mixins": {}, "env": {}}, None],
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
                        "stage": "schema.stage",
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
                        "bundle_root": "",
                        "deploy_root": "",
                        "generated_root": "",
                        "stage": "just_stage",
                        "scratch_stage": "scratch_stage",
                        "distribution": "internal",
                    }
                }
            },
            "Incorrect value for stage of native_app. Expected format for this field is {schema_name}.{stage_name}",
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
                "entities": {
                    "function1": {
                        "type": "function",
                        "identifier": "name",
                        "handler": "app.hello",
                        "returns": "string",
                        "stage": "dev",
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
                "env": {"string": "string", "int": 42, "bool": True},
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
        _ = render_definition_template(definition_input, {})
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
        definition_v2 = convert_project_definition_to_v2(project_dir, definition_v1)
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
            src=definition_v1.snowpark.src,
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


def test_v1_to_v2_conversion_in_memory_package_scripts(temporary_directory):
    package_script = "select '{{ package_name }}';"
    package_script_filename = "scripts/package-script.sql"
    ProjectV11Factory(
        pdf__native_app__package__scripts=[package_script_filename],
        pdf__native_app__artifacts=["app/manifest.yml"],
        files={
            package_script_filename: package_script,
            "app/manifest.yml": "",  # It just needs to exist for the definition conversion
        },
    )

    definition_v1 = DefinitionManager(temporary_directory).project_definition
    definition_v2 = convert_project_definition_to_v2(
        Path(temporary_directory), definition_v1, in_memory=True
    )

    # Actual contents of package script in project was not changed
    assert Path(package_script_filename).read_text() == package_script

    # But the converted definition has a reference to a tempfile
    # that contains the literal package name
    assert (
        Path(definition_v2.entities["pkg"].meta.post_deploy[0].sql_script).read_text()
        == f"select '{definition_v2.entities['pkg'].fqn.name}';"
    )


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


def test_using_mixing_with_unknown_entity_property_fails(project_directory):
    with project_directory("mixins_different_entities") as project_dir:
        with pytest.raises(SchemaValidationError) as err:
            _ = DefinitionManager(project_dir).project_definition

    assert "Unsupported key 'main_file' for entity function1 of type function" in str(
        err
    )


def test_list_of_mixins_in_correct_order(project_directory):
    with project_directory("mixins_list_applied_in_order") as project_dir:
        definition = DefinitionManager(project_dir).project_definition

    assert definition.entities["function1"].stage == "foo"
    assert definition.entities["function2"].stage == "baz"
    assert definition.entities["streamlit1"].stage == "streamlit"


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


def test_mixin_with_unknown_entity_key_raises_error():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "func": {
                "type": "function",
                "identifier": "my_func",
                "handler": "foo",
                "returns": "string",
                "signature": "",
                "artifacts": [],
                "stage": "bar",
                "meta": {"use_mixins": ["schema_mixin"]},
            }
        },
        "mixins": {"schema_mixin": {"unknown_key": "NA"}},
    }
    with pytest.raises(SchemaValidationError) as err:
        _ = render_definition_template(definition_input, {})

    assert "Unsupported key 'unknown_key' for entity func of type function" in str(err)


_PARTIAL_FUNCTION = {
    "type": "function",
    "handler": "foo",
    "returns": "string",
    "signature": "",
    "artifacts": [],
    "stage": "bar",
    "secrets": {"secret_a": "secret_a_value"},
    "external_access_integrations": ["integration_1"],
}


def test_mixin_values_are_properly_applied_to_entity():
    definition_input = {
        "definition_version": "2",
        "mixins": {
            "schema_mixin": {
                "identifier": {"schema": "MIXIN"},
                "secrets": {"secret_b": "secret_b_value"},
                "external_access_integrations": ["integration_2"],
            }
        },
        "env": {"FOO": "foo_name"},
        "entities": {
            "func": {
                "identifier": {"name": "my_func"},
                **_PARTIAL_FUNCTION,
                "meta": {"use_mixins": ["schema_mixin"]},
            },
            "func_a": {
                "identifier": {"name": "<% ctx.env.FOO %>"},
                **_PARTIAL_FUNCTION,
                "meta": {"use_mixins": ["schema_mixin"]},
            },
        },
    }
    project = render_definition_template(definition_input, {}).project_definition
    func_entity = project.entities["func"]
    assert func_entity.fqn.schema == "MIXIN"
    assert func_entity.secrets == {
        "secret_a": "secret_a_value",
        "secret_b": "secret_b_value",
    }
    assert func_entity.external_access_integrations == [
        "integration_2",
        "integration_1",
    ]
    assert project.entities["func_a"].fqn.schema == "MIXIN"


def test_mixin_order_scalar():
    _function = {
        "type": "function",
        "handler": "foo",
        "returns": "string",
        "signature": "",
        "artifacts": [],
    }
    stage_from_entity = "stage_from_entity"
    definition_input = {
        "definition_version": "2",
        "mixins": {
            "mix1": {"stage": "mix1"},
            "mix2": {"stage": "mix2"},
        },
        "entities": {
            "no_mixin": {**_function, "stage": stage_from_entity},
            "mix1_only": {
                **_function,
                "meta": {"use_mixins": ["mix1"]},
            },
            "mix1_and_mix2": {
                **_function,
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
            "mix2_and_mix1": {
                **_function,
                "meta": {"use_mixins": ["mix2", "mix1"]},
            },
            "mix1_and_entity": {
                **_function,
                "stage": stage_from_entity,
                "meta": {"use_mixins": ["mix1"]},
            },
            "mixins_and_entity": {
                **_function,
                "stage": stage_from_entity,
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
        },
    }
    pd = render_definition_template(definition_input, {}).project_definition
    entities = pd.entities
    assert entities["no_mixin"].stage == stage_from_entity
    assert entities["mix1_only"].stage == "mix1"
    assert entities["mix1_and_mix2"].stage == "mix2"
    assert entities["mix2_and_mix1"].stage == "mix1"
    assert entities["mix1_and_entity"].stage == stage_from_entity
    assert entities["mixins_and_entity"].stage == stage_from_entity


def test_mixin_order_sequence_merge_order():
    _function = {
        "type": "function",
        "handler": "foo",
        "returns": "string",
        "signature": "",
        "stage": "foo",
        "artifacts": [],
    }

    definition_input = {
        "definition_version": "2",
        "mixins": {
            "mix1": {"external_access_integrations": ["mix1_int"]},
            "mix2": {"external_access_integrations": ["mix2_int"]},
        },
        "entities": {
            "no_mixin": {
                **_function,
                "external_access_integrations": ["entity_int"],
            },
            "mix1_only": {
                **_function,
                "meta": {"use_mixins": ["mix1"]},
            },
            "mix1_and_mix2": {
                **_function,
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
            "mix2_and_mix1": {
                **_function,
                "meta": {"use_mixins": ["mix2", "mix1"]},
            },
            "mix1_and_entity": {
                **_function,
                "external_access_integrations": ["entity_int"],
                "meta": {"use_mixins": ["mix1"]},
            },
            "mixins_and_entity": {
                **_function,
                "external_access_integrations": ["entity_int"],
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
        },
    }
    pd = render_definition_template(definition_input, {}).project_definition
    entities = pd.entities
    assert entities["no_mixin"].external_access_integrations == ["entity_int"]
    assert entities["mix1_only"].external_access_integrations == ["mix1_int"]
    assert entities["mix1_and_mix2"].external_access_integrations == [
        "mix1_int",
        "mix2_int",
    ]
    assert entities["mix2_and_mix1"].external_access_integrations == [
        "mix2_int",
        "mix1_int",
    ]
    assert entities["mix1_and_entity"].external_access_integrations == [
        "mix1_int",
        "entity_int",
    ]
    assert entities["mixins_and_entity"].external_access_integrations == [
        "mix1_int",
        "mix2_int",
        "entity_int",
    ]


def test_mixin_order_mapping_merge_order():
    _function = {
        "type": "function",
        "handler": "foo",
        "returns": "string",
        "signature": "",
        "stage": "foo",
        "artifacts": [],
    }

    definition_input = {
        "definition_version": "2",
        "mixins": {
            "mix1": {"secrets": {"mix1_key": "mix1_value", "common": "mix1"}},
            "mix2": {"secrets": {"mix2_key": "mix2_value", "common": "mix2"}},
        },
        "entities": {
            "no_mixin": {
                **_function,
                "secrets": {"entity_key": "entity_value", "common": "entity"},
            },
            "mix1_only": {
                **_function,
                "meta": {"use_mixins": ["mix1"]},
            },
            "mix1_and_mix2": {
                **_function,
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
            "mix2_and_mix1": {
                **_function,
                "meta": {"use_mixins": ["mix2", "mix1"]},
            },
            "mix1_and_entity": {
                **_function,
                "secrets": {"entity_key": "entity_value", "common": "entity"},
                "meta": {"use_mixins": ["mix1"]},
            },
            "mixins_and_entity": {
                **_function,
                "secrets": {"entity_key": "entity_value", "common": "entity"},
                "meta": {"use_mixins": ["mix1", "mix2"]},
            },
        },
    }
    pd = render_definition_template(definition_input, {}).project_definition
    entities = pd.entities
    assert entities["no_mixin"].secrets == {
        "entity_key": "entity_value",
        "common": "entity",
    }
    assert entities["mix1_only"].secrets == {"mix1_key": "mix1_value", "common": "mix1"}
    assert entities["mix1_and_mix2"].secrets == {
        "mix1_key": "mix1_value",
        "mix2_key": "mix2_value",
        "common": "mix2",
    }
    assert entities["mix2_and_mix1"].secrets == {
        "mix1_key": "mix1_value",
        "mix2_key": "mix2_value",
        "common": "mix1",
    }
    assert entities["mix1_and_entity"].secrets == {
        "mix1_key": "mix1_value",
        "entity_key": "entity_value",
        "common": "entity",
    }
    assert entities["mixins_and_entity"].secrets == {
        "mix1_key": "mix1_value",
        "mix2_key": "mix2_value",
        "entity_key": "entity_value",
        "common": "entity",
    }


def test_mixins_values_have_to_be_type_compatible_with_entities():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "func": {
                "identifier": "my_func",
                "type": "function",
                "handler": "foo",
                "returns": "string",
                "signature": "",
                "artifacts": [],
                "stage": "bar",
                "meta": {"use_mixins": ["schema_mixin"]},
            },
        },
        "mixins": {"schema_mixin": {"identifier": {"schema": "MIXIN"}}},
    }
    with pytest.raises(SchemaValidationError) as err:
        _ = render_definition_template(definition_input, {}).project_definition

    assert (
        "Value from mixins for property identifier is of type 'dict' while entity func expects value of type 'str'"
        in str(err)
    )


def test_if_list_in_mixin_is_applied_correctly():
    definition_input = {
        "definition_version": "2",
        "entities": {
            "func": {
                "identifier": "my_func",
                "type": "function",
                "handler": "foo",
                "returns": "string",
                "signature": "",
                "stage": "bar",
                "meta": {"use_mixins": ["artifact_mixin"]},
            },
        },
        "mixins": {
            "artifact_mixin": {
                "external_access_integrations": ["integration_1", "integration_2"],
                "artifacts": [{"src": "src", "dest": "my_project"}],
            },
        },
    }
    project = render_definition_template(definition_input, {}).project_definition
    assert len(project.entities["func"].artifacts) == 1
