from pathlib import Path

import pytest

from snowflake.cli._plugins.nativeapp.codegen.compiler import TEMPLATES_PROCESSOR
from tests.nativeapp.factories import ProjectV11Factory


@pytest.mark.integration
def test_v1_to_v2_converts_templates_in_files(temp_dir, runner):
    native_app_name = "my_native_app_project"

    expected_conversions = {
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

    file_with_templates = "app/file.txt"
    ProjectV11Factory(
        pdf__native_app__name=native_app_name,
        pdf__native_app__package__name="my_pkg",
        pdf__native_app__application__name="my_app",
        pdf__native_app__artifacts=[
            dict(src="app/*", processors=[TEMPLATES_PROCESSOR])
        ],
        files={
            "app/manifest.yml": "",  # It just needs to exist for the definition conversion
            file_with_templates: "\n".join(expected_conversions.keys()),
        },
    )

    result = runner.invoke(["helpers", "v1-to-v2"], env={"FOO": "bar"})
    assert result.exit_code == 0, result.output

    file_contents = Path(file_with_templates).read_text()
    expected_file_contents = "\n".join(expected_conversions.values())
    assert file_contents == expected_file_contents, file_contents
