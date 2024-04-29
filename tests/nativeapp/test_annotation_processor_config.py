import pytest
from snowflake.cli.api.project.definition import (
    load_project_definition,
)


@pytest.mark.parametrize(
    "project_definition_files", ["napp_with_annotation_processor"], indirect=True
)
def test_napp_project_with_annotation_processor(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert len(project.native_app.artifacts) == 3
    result = project.native_app.artifacts[2]
    assert len(result.processors) == 3
    assert result.processors[0] == "snowpark"
    assert result.processors[1].name == "snowpark"
    assert result.processors[1].virtual_env is None
    assert result.processors[2].name == "snowpark"
    assert result.processors[2].virtual_env.name == "snowpark_venv"
    assert result.processors[2].virtual_env.env_type == "venv"
