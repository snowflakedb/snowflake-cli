from textwrap import dedent

import pytest

from tests.nativeapp.factories import (
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
    ApplicationEntityModelFactory,
    ManifestFactory,
)


MANIFEST_BASIC = ManifestFactory()

PYTHON_W_SNOWPARK = dedent(
    """\
    from snowflake.snowpark.functions import udf
    @udf(
        name="echo_fn",
        native_app_params={"schema": "core", "application_roles": ["app_public"]},
    )
    def echo_fn(data: str) -> str:
        return "echo_fn: " + data
        """
)


@pytest.fixture
def setup_v2_project_w_subdir(temporary_directory):
    def wrapper():
        readme_v1 = (
            "This is the <% ctx.pkg_v1.stage_subdirectory %> version of this package!"
        )
        readme_v2 = (
            "This is the <% ctx.pkg_v2.stage_subdirectory %> version of this package!"
        )
        project_name = "stage_w_subdirs"
        ProjectV2Factory(
            pdf__entities=dict(
                pkg_v1=ApplicationPackageEntityModelFactory(
                    identifier=f"<% fn.concat_ids('{project_name}_pkg_', ctx.env.USER) %>",
                    manifest="",
                    artifacts=[{"src": "app/v1/*", "dest": "./"}],
                    stage_subdirectory="v1",
                ),
                app_v1=ApplicationEntityModelFactory(
                    fromm__target="pkg_v1",
                    identifier=f"<% fn.concat_ids('{project_name}_app_v1_', ctx.env.USER) %>",
                ),
                pkg_v2=ApplicationPackageEntityModelFactory(
                    identifier=f"<% fn.concat_ids('{project_name}_pkg_', ctx.env.USER) %>",
                    manifest="",
                    artifacts=[{"src": "app/v2/*", "dest": "./"}],
                    stage_subdirectory="v2",
                ),
                app_v2=ApplicationEntityModelFactory(
                    fromm__target="pkg_v2",
                    identifier=f"<% fn.concat_ids('{project_name}_app_v2_', ctx.env.USER) %>",
                ),
            ),
            files={
                "app/v1/manifest.yml": MANIFEST_BASIC,
                "app/v1/README.md": readme_v1,
                "app/v1/setup.sql": "SELECT 1;",
                "app/v2/manifest.yml": MANIFEST_BASIC,
                "app/v2/README.md": readme_v2,
                "app/v2/setup.sql": "SELECT 1;",
            },
        )
        return project_name, temporary_directory

    return wrapper
