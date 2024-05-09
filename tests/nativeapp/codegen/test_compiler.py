from copy import deepcopy
from pathlib import Path

import pytest
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    UnsupportedArtifactProcessorError,
)
from snowflake.cli.plugins.nativeapp.codegen.compiler import (
    _find_and_execute_processors,
    _try_create_processor,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)

proj_def = ProjectDefinition(
    **{
        "definition_version": "1",
        "native_app": {
            "artifacts": [
                {"dest": "./", "src": "app/*"},
                "app/setup.sql",
                {"dest": "./", "src": "app/*", "processors": ["DUMMY"]},
                {"dest": "./", "src": "app/*", "processors": ["SNOWPARK"]},
                {
                    "dest": "./",
                    "src": "app/*",
                    "processors": [{"name": "SNOWPARK"}],
                },
            ],
            "name": "napp_test",
            "package": {
                "scripts": [
                    "package/001.sql",
                ]
            },
        },
    }
)


def test_try_create_processor_returns_none():
    artifact_to_process = proj_def.native_app.artifacts[2]
    result = _try_create_processor(
        processor_mapping=artifact_to_process.processors[0],
        project_definition=proj_def.native_app,
        project_root=Path("some/dummy/path"),
        deploy_root=Path("some/dummy/path"),
        artifact_to_process=artifact_to_process,
    )
    assert result is None


@pytest.mark.parametrize(
    "artifact_to_process",
    [proj_def.native_app.artifacts[3], proj_def.native_app.artifacts[4]],
)
def test_try_create_processor_returns_processor(artifact_to_process):
    result = _try_create_processor(
        processor_mapping=artifact_to_process.processors[0],
        project_definition=proj_def.native_app,
        project_root=Path("some/dummy/path"),
        deploy_root=Path("some/dummy/path"),
        artifact_to_process=artifact_to_process,
    )
    assert isinstance(result, SnowparkAnnotationProcessor)


def test_find_and_execute_processors_exception():
    test_proj_def = deepcopy(proj_def)
    test_proj_def.native_app.artifacts = [
        {"dest": "./", "src": "app/*", "processors": ["DUMMY"]}
    ]

    with pytest.raises(UnsupportedArtifactProcessorError):
        _find_and_execute_processors(
            project_definition=test_proj_def.native_app,
            project_root=Path("some/dummy/path"),
            deploy_root=Path("some/dummy/path"),
        )
