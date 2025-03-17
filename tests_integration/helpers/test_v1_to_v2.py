from pathlib import Path

import pytest

from snowflake.cli._plugins.nativeapp.codegen.templates.templates_processor import (
    TemplatesProcessor,
)
from tests.nativeapp.factories import ProjectV11Factory


@pytest.mark.integration
def test_v1_to_v2_converts_templates_in_files(temporary_directory, runner):
    native_app_name = "my_native_app_project"

    src_to_result = {
        # Reference to app field should be converted to v2
        "<% ctx.native_app.application.name %>": "<% ctx.entities.app.identifier %>",
        # Reference to nested app field should be converted to v2
        "<% ctx.native_app.application.role %>": "<% ctx.entities.app.meta.role %>",
        # Reference to package field should be converted to v2
        "<% ctx.native_app.package.name %>": "<% ctx.entities.pkg.identifier %>",
        # Reference to nested package field should be converted to v2
        "<% ctx.native_app.package.role %>": "<% ctx.entities.pkg.meta.role %>",
        # Reference to native_app name field should be a literal
        "<% ctx.native_app.name %>": native_app_name,
        # Reference to ctx.env field should remain as is
        "<% ctx.env.FOO %>": "<% ctx.env.FOO %>",
        # Reference to fn field should remain as is
        "<% fn.get_username('test') %>": "<% fn.get_username('test') %>",
    }
    source_contents = "\n".join(src_to_result.keys())
    expected_templated_contents = "\n".join(src_to_result.values())

    files_using_template_processor = [
        "templated.txt",
        "app/templated.txt",
        "app/manifest.yml",
        "nested/dir/templated.txt",
    ]
    files_not_using_template_processor = [
        "untemplated.txt",
    ]

    ProjectV11Factory(
        pdf__native_app__name=native_app_name,
        pdf__native_app__package__name="my_pkg",
        pdf__native_app__application__name="my_app",
        pdf__native_app__artifacts=[
            dict(src="templated.txt", processors=[TemplatesProcessor.NAME]),
            dict(src="untemplated.txt"),
            dict(src="app/*", processors=[TemplatesProcessor.NAME]),
            dict(src="nested/*", processors=[TemplatesProcessor.NAME]),
        ],
        files={
            filename: source_contents
            for filename in (
                files_using_template_processor + files_not_using_template_processor
            )
        },
    )

    result = runner.invoke(["helpers", "v1-to-v2"], env={"FOO": "bar"})
    assert result.exit_code == 0, result.output

    for filename in files_using_template_processor:
        file_contents = Path(filename).read_text()
        assert file_contents == expected_templated_contents, filename
    for filename in files_not_using_template_processor:
        file_contents = Path(filename).read_text()
        assert file_contents == source_contents, filename
