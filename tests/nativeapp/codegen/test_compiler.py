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
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    UnsupportedArtifactProcessorError,
)
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)

from tests.nativeapp.utils import create_native_app_project_model


@pytest.fixture()
def test_proj_def():
    return build_project_definition(
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
    na_project = create_native_app_project_model(test_proj_def.native_app)
    return NativeAppCompiler(na_project.get_bundle_context())


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
    app_pkg = create_native_app_project_model(
        project_definition=test_proj_def.native_app
    )
    test_compiler = NativeAppCompiler(app_pkg.get_bundle_context())

    with pytest.raises(UnsupportedArtifactProcessorError):
        test_compiler.compile_artifacts()
