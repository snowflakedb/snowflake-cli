from pathlib import Path

import pytest
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    UnsupportedArtifactProcessorError,
)
from snowflake.cli.plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)


@pytest.fixture()
def test_proj_def():
    return ProjectDefinition(
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


@pytest.fixture()
def test_compiler(test_proj_def):
    return NativeAppCompiler(
        project_definition=test_proj_def.native_app,
        project_root=Path("some/dummy/path"),
        deploy_root=Path("some/dummy/path"),
        generated_root=Path("some/dummy/path"),
    )


def test_try_create_processor_returns_none(test_proj_def, test_compiler):
    artifact_to_process = test_proj_def.native_app.artifacts[2]
    result = test_compiler._try_create_processor(  # noqa: SLF001
        processor_mapping=artifact_to_process.processors[0],
    )
    assert result is None


@pytest.mark.parametrize(
    "artifact_index",
    [3, 4],
)
def test_try_create_processor_returns_processor(
    artifact_index, test_proj_def, test_compiler
):
    mapping = test_proj_def.native_app.artifacts[artifact_index]
    result = test_compiler._try_create_processor(  # noqa: SLF001
        processor_mapping=mapping.processors[0],
    )
    assert isinstance(result, SnowparkAnnotationProcessor)


def test_find_and_execute_processors_exception(test_proj_def, test_compiler):
    test_proj_def.native_app.artifacts = [
        {"dest": "./", "src": "app/*", "processors": ["DUMMY"]}
    ]
    test_compiler = NativeAppCompiler(
        project_definition=test_proj_def.native_app,
        project_root=Path("some/dummy/path"),
        deploy_root=Path("some/dummy/path"),
        generated_root=Path("some/dummy/path"),
    )

    with pytest.raises(UnsupportedArtifactProcessorError):
        test_compiler.compile_artifacts()
