import pytest
from snowflake.cli.api.project.definition import (
    load_project_definition,
)


@pytest.mark.parametrize(
    "project_definition_files", ["napp_with_annotation_processor"], indirect=True
)
def test_napp_project_with_annotation_processor(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert len(project.native_app.artifacts) == 4

    result = project.native_app.artifacts[2]
    assert len(result.processors) == 3
    assert result.processors[0] == "snowpark_scala"
    assert result.processors[1].name == "snowpark_java"
    assert result.processors[1].properties is None
    assert result.processors[2].name == "snowpark"
    assert result.processors[2].properties["venv_path"] == "~/Users/jdoe/snowpark_venv"
    assert result.processors[2].properties["env_type"] == "venv"

    result = project.native_app.artifacts[3]
    assert len(result.processors) == 1
    assert result.processors[0].name == "snowpark"
    assert result.processors[0].properties["name"] == "snowpark_conda"
    assert result.processors[0].properties["env_type"] == "conda"
