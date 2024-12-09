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
import re
from pathlib import Path
from typing import Optional

import pytest
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    UnsupportedArtifactProcessorError,
)
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)


@pytest.fixture()
def test_proj_def():
    return build_project_definition(
        **dict(
            definition_version="2",
            entities=dict(
                pkg=dict(
                    type="application package",
                    artifacts=[
                        dict(dest="./", src="app/*"),
                        "app/setup.sql",
                        dict(dest="./", src="app/*", processors=["DUMMY"]),
                        dict(dest="./", src="app/*", processors=["SNOWPARK"]),
                        dict(dest="./", src="app/*", processors=[{"name": "SNOWPARK"}]),
                    ],
                    manifest="app/manifest.yml",
                )
            ),
        )
    )


def _get_bundle_context(pkg_model: ApplicationPackageEntityModel):
    project_root = Path().resolve()
    return BundleContext(
        package_name=pkg_model.fqn.name,
        artifacts=pkg_model.artifacts,
        project_root=project_root,
        bundle_root=project_root / pkg_model.bundle_root,
        deploy_root=project_root / pkg_model.deploy_root,
        generated_root=(
            project_root / pkg_model.deploy_root / pkg_model.generated_root
        ),
    )


@pytest.fixture()
def test_compiler(test_proj_def):
    return NativeAppCompiler(_get_bundle_context(test_proj_def.entities["pkg"]))


@pytest.mark.parametrize("name", ["Project", "Deploy", "Bundle", "Generated"])
def test_compiler_requires_absolute_paths(test_proj_def, name):
    bundle_context = _get_bundle_context(test_proj_def.entities["pkg"])

    path = Path()
    setattr(bundle_context, f"{name.lower()}_root", path)
    with pytest.raises(
        AssertionError,
        match=re.escape(rf"{name} root {path} must be an absolute path."),
    ):
        NativeAppCompiler(bundle_context)


def test_try_create_processor_returns_none(test_proj_def, test_compiler):
    artifact_to_process = test_proj_def.entities["pkg"].artifacts[2]
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
    mapping = test_proj_def.entities["pkg"].artifacts[artifact_index]
    result = test_compiler._try_create_processor(  # noqa: SLF001
        processor_mapping=mapping.processors[0],
    )
    assert isinstance(result, SnowparkAnnotationProcessor)


def test_find_and_execute_processors_exception(test_proj_def, test_compiler):
    pkg_model = test_proj_def.entities["pkg"]
    pkg_model.artifacts = [{"dest": "./", "src": "app/*", "processors": ["DUMMY"]}]
    test_compiler = NativeAppCompiler(_get_bundle_context(pkg_model))

    with pytest.raises(UnsupportedArtifactProcessorError):
        test_compiler.compile_artifacts()


class TestProcessor(ArtifactProcessor):
    NAME = "test_processor"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert False  # never invoked

    @staticmethod
    def is_enabled():
        return False

    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        assert False  # never invoked


def test_skips_disabled_processors(test_proj_def, test_compiler):
    pkg_model = test_proj_def.entities["pkg"]
    pkg_model.artifacts = [
        {"dest": "./", "src": "app/*", "processors": ["test_processor"]}
    ]
    test_compiler = NativeAppCompiler(_get_bundle_context(pkg_model))
    test_compiler.register(TestProcessor)

    # TestProcessor is never invoked, otherwise calling its methods will make the test fail
    test_compiler.compile_artifacts()
