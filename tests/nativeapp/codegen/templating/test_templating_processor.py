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

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.templating.templating_processor import (
    TemplatingProcessor,
)
from snowflake.cli._plugins.nativeapp.exceptions import InvalidTemplateInFileError
from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping

from tests.nativeapp.utils import CLI_GLOBAL_TEMPLATE_CONTEXT


class FilesSetup(TemporaryDirectory):
    @dataclass
    class SetupResult:
        artifact_to_process: PathMapping
        bundle_ctx: BundleContext
        output_files: list[Path]

    def __init__(self, file_names: list[str], file_contents: list[str]):
        super().__init__()
        assert len(file_names) == len(file_contents)
        self.file_names = file_names
        self.file_contents = file_contents

    def __enter__(self) -> SetupResult:
        tmp_dir = super().__enter__()
        project_root = Path(tmp_dir)

        deploy_root = Path(tmp_dir) / "output" / "deploy"
        deploy_root.mkdir(parents=True, exist_ok=True)

        src_root = project_root / "src"
        src_root.mkdir(parents=True, exist_ok=True)

        for index, file_name in enumerate(self.file_names):
            test_file = src_root / file_name
            test_file.write_text(self.file_contents[index])

        # create a symlink to the test file from the deploy directory:
        output_files = []
        for file_name in self.file_names:
            deploy_file = deploy_root / file_name
            output_files.append(deploy_file)
            deploy_file.symlink_to(src_root / file_name)

        artifact_to_process = PathMapping(
            src="src/*", dest="./", processors=["templating"]
        )

        bundle_context = BundleContext(
            package_name="test_package_name",
            project_root=project_root,
            artifacts=[artifact_to_process],
            bundle_root=deploy_root / "bundle",
            generated_root=deploy_root / "generated",
            deploy_root=deploy_root,
        )

        return FilesSetup.SetupResult(artifact_to_process, bundle_context, output_files)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return super().__exit__(exc_type, exc_val, exc_tb)


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_templating_processor_valid_files_no_templates():
    file_names = ["test_file.txt"]
    file_contents = ["This is a test file\n with some content"]
    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)
        templating_processor.process(setup_result.artifact_to_process, None)

        assert setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"TEST_VAR": "test_value"}}})
def test_one_file_with_template_and_one_without():
    file_names = ["test_file.txt", "test_file_with_template.txt"]
    file_contents = [
        "This is a test file\n with some content",
        "This is a test file\n with some <% ctx.env.TEST_VAR %>",
    ]
    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)
        templating_processor.process(setup_result.artifact_to_process, None)

        assert setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0]

        assert not setup_result.output_files[1].is_symlink()
        assert setup_result.output_files[1].read_text() == file_contents[1].replace(
            "<% ctx.env.TEST_VAR %>", "test_value"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"native_app": {"name": "test_app"}}})
def test_templating_with_sql_and_non_sql_files_and_mix_syntax():
    file_names = ["test_sql.sql", "test_non_sql.txt"]
    file_contents = [
        "This is a sql file with &{ ctx.native_app.name }",
        "This is a non sql file with <% ctx.native_app.name %>",
    ]
    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)
        templating_processor.process(setup_result.artifact_to_process, None)

        assert not setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0].replace(
            "&{ ctx.native_app.name }", "test_app"
        )
        assert not setup_result.output_files[1].is_symlink()
        assert setup_result.output_files[1].read_text() == file_contents[1].replace(
            "<% ctx.native_app.name %>", "test_app"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templating_with_sql_new_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with <% ctx.env.name %>"]

    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)
        templating_processor.process(setup_result.artifact_to_process, None)

        assert not setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0].replace(
            "<% ctx.env.name %>", "test_name"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templating_with_sql_old_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with &{ ctx.env.name }"]
    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)
        templating_processor.process(setup_result.artifact_to_process, None)

        assert not setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0].replace(
            "&{ ctx.env.name }", "test_name"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templating_with_sql_both_old_and_new_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with &{ ctx.env.name } and <% ctx.env.name %>"]
    with FilesSetup(file_names, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)

        with pytest.raises(InvalidTemplate) as e:
            templating_processor.process(setup_result.artifact_to_process, None)

        assert (
            "The SQL query in src/test_sql.sql mixes &{ ... } syntax and <% ... %> syntax."
            in str(e.value)
        )
        assert setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_file_with_syntax_error():
    file_name = ["test_file.txt"]
    file_contents = ["This is a test file with invalid <% ctx.env.TEST_VAR"]
    with FilesSetup(file_name, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)

        with pytest.raises(InvalidTemplateInFileError) as e:
            templating_processor.process(setup_result.artifact_to_process, None)

        assert "does not contain a valid template" in str(e.value)
        assert setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_file_with_undefined_variable():
    file_name = ["test_file.txt"]
    file_contents = ["This is a test file with invalid <% ctx.env.TEST_VAR %>"]
    with FilesSetup(file_name, file_contents) as setup_result:
        templating_processor = TemplatingProcessor(bundle_ctx=setup_result.bundle_ctx)

        with pytest.raises(InvalidTemplateInFileError) as e:
            templating_processor.process(setup_result.artifact_to_process, None)

        assert "'ctx' is undefined" in str(e.value)
        assert "does not contain a valid template" in str(e.value)
        assert setup_result.output_files[0].is_symlink()
        assert setup_result.output_files[0].read_text() == file_contents[0]
