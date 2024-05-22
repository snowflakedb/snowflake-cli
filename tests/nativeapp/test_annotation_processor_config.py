import pytest
from snowflake.cli.api.project.definition import (
    load_project_definition,
)
from snowflake.cli.api.project.schemas.native_app.path_mapping import ProcessorMapping


@pytest.mark.parametrize(
    "project_definition_files", ["napp_with_annotation_processor"], indirect=True
)
def test_napp_project_with_annotation_processor(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert len(project.native_app.artifacts) == 3

    result = project.native_app.artifacts[2]
    assert len(result.processors) == 3

    assert isinstance(result.processors[0], ProcessorMapping)
    assert result.processors[0].name == "simple_processor_str"

    assert isinstance(result.processors[1], ProcessorMapping)
    assert result.processors[1].name == "processor_without_properties"
    assert result.processors[1].properties is None

    assert isinstance(result.processors[2], ProcessorMapping)
    assert result.processors[2].name == "processor_with_properties"
    properties = result.processors[2].properties
    assert len(properties.keys()) == 2
    assert properties["key_1"] == "value_1"
    assert properties["key_2"]["key_3"] == "value_3"
    assert len(properties["key_2"]["key_4"]) == 3
    assert properties["key_2"]["key_4"][0] == "value_a"
    assert properties["key_2"]["key_4"][1] == "value_b"
    assert properties["key_2"]["key_4"][2] == "1"
