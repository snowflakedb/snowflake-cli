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
from snowflake.cli._plugins.nativeapp.codegen.templates.templates_processor import (
    TemplatesProcessor,
)
from snowflake.cli._plugins.nativeapp.exceptions import InvalidTemplateInFileError
from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping

from tests.nativeapp.utils import CLI_GLOBAL_TEMPLATE_CONTEXT


@dataclass
class BundleResult:
    """
    Dataclass to hold the test setup result
    """

    artifact_to_process: PathMapping
    bundle_ctx: BundleContext
    output_files: list[Path]


def bundle_files(
    tmp_dir: str, file_names: list[str], file_contents: list[str]
) -> BundleResult:
    project_root = Path(tmp_dir)

    deploy_root = Path(tmp_dir) / "output" / "deploy"
    deploy_root.mkdir(parents=True, exist_ok=True)

    src_root = project_root / "src"
    src_root.mkdir(parents=True, exist_ok=True)

    for index, file_name in enumerate(file_names):
        test_file = src_root / file_name
        test_file.write_text(file_contents[index])

    # create a symlink to the test file from the deploy directory:
    output_files = []
    for file_name in file_names:
        deploy_file = deploy_root / file_name
        output_files.append(deploy_file)
        deploy_file.symlink_to(src_root / file_name)

    artifact_to_process = PathMapping(src="src/*", dest="./", processors=["templates"])

    bundle_context = BundleContext(
        package_name="test_package_name",
        project_root=project_root,
        artifacts=[artifact_to_process],
        bundle_root=deploy_root / "bundle",
        generated_root=deploy_root / "generated",
        deploy_root=deploy_root,
    )

    return BundleResult(artifact_to_process, bundle_context, output_files)


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_templates_processor_valid_files_no_templates():
    file_names = ["test_file.txt"]
    file_contents = ["This is a test file\n with some content"]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)
        templates_processor.process(bundle_result.artifact_to_process, None)

        assert bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"TEST_VAR": "test_value"}}})
def test_one_file_with_template_and_one_without():
    file_names = ["test_file.txt", "test_file_with_template.txt"]
    file_contents = [
        "This is a test file\n with some content",
        "This is a test file\n with some <% ctx.env.TEST_VAR %>",
    ]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)
        templates_processor.process(bundle_result.artifact_to_process, None)

        assert bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0]

        assert not bundle_result.output_files[1].is_symlink()
        assert bundle_result.output_files[1].read_text() == file_contents[1].replace(
            "<% ctx.env.TEST_VAR %>", "test_value"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"native_app": {"name": "test_app"}}})
def test_templates_with_sql_and_non_sql_files_and_mix_syntax():
    file_names = ["test_sql.sql", "test_non_sql.txt"]
    file_contents = [
        "This is a sql file with &{ ctx.native_app.name }",
        "This is a non sql file with <% ctx.native_app.name %>",
    ]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)
        templates_processor.process(bundle_result.artifact_to_process, None)

        assert not bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0].replace(
            "&{ ctx.native_app.name }", "test_app"
        )

        assert not bundle_result.output_files[1].is_symlink()
        assert bundle_result.output_files[1].read_text() == file_contents[1].replace(
            "<% ctx.native_app.name %>", "test_app"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templates_with_sql_new_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with <% ctx.env.name %>"]

    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)
        templates_processor.process(bundle_result.artifact_to_process, None)

        assert not bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0].replace(
            "<% ctx.env.name %>", "test_name"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templates_with_sql_old_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with &{ ctx.env.name }"]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)
        templates_processor.process(bundle_result.artifact_to_process, None)

        assert not bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0].replace(
            "&{ ctx.env.name }", "test_name"
        )


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {"ctx": {"env": {"name": "test_name"}}})
def test_templates_with_sql_both_old_and_new_syntax():
    file_names = ["test_sql.sql"]
    file_contents = ["This is a sql file with &{ ctx.env.name } and <% ctx.env.name %>"]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_names, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)

        with pytest.raises(InvalidTemplate) as e:
            templates_processor.process(bundle_result.artifact_to_process, None)

        assert "mixes &{ ... } syntax and <% ... %> syntax." in str(e.value)
        assert bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_file_with_syntax_error():
    file_name = ["test_file.txt"]
    file_contents = ["This is a test file with invalid <% ctx.env.TEST_VAR"]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_name, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)

        with pytest.raises(InvalidTemplateInFileError) as e:
            templates_processor.process(bundle_result.artifact_to_process, None)

        assert "does not contain a valid template" in str(e.value)
        assert bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0]


@mock.patch(CLI_GLOBAL_TEMPLATE_CONTEXT, {})
def test_file_with_undefined_variable():
    file_name = ["test_file.txt"]
    file_contents = ["This is a test file with invalid <% ctx.env.TEST_VAR %>"]
    with TemporaryDirectory() as tmp_dir:
        bundle_result = bundle_files(tmp_dir, file_name, file_contents)
        templates_processor = TemplatesProcessor(bundle_ctx=bundle_result.bundle_ctx)

        with pytest.raises(InvalidTemplateInFileError) as e:
            templates_processor.process(bundle_result.artifact_to_process, None)

        assert "'ctx' is undefined" in str(e.value)
        assert "does not contain a valid template" in str(e.value)
        assert bundle_result.output_files[0].is_symlink()
        assert bundle_result.output_files[0].read_text() == file_contents[0]
