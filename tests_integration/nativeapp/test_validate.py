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

from shlex import split
from textwrap import dedent

from tests.nativeapp.factories import (
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
    ApplicationEntityModelFactory,
    ProjectV10Factory,
)
from tests.project.fixtures import *


@pytest.mark.integration
def test_nativeapp_validate_v1(nativeapp_teardown, runner, temporary_directory):
    ProjectV10Factory(
        pdf__native_app__name="myapp",
        pdf__native_app__artifacts=[
            {"src": "app/*", "dest": "./"},
        ],
        files={
            "app/setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "app/README.md": "\n",
            "app/manifest.yml": "\n",
        },
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        # validate the app's setup script
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 0, result.output
        assert "Native App validation succeeded." in result.output


@pytest.mark.integration
@pytest.mark.parametrize(
    "command",
    [
        "app validate",
        "ws validate --entity-id=pkg",
    ],
)
def test_nativeapp_validate_v2(
    command, nativeapp_teardown, runner, temporary_directory
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": "\n",
        },
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        # validate the app's setup script
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 0, result.output
        if command.startswith("ws"):
            assert "Setup script is valid" in result.output
        else:
            assert "Native App validation succeeded." in result.output


@pytest.mark.integration
def test_nativeapp_validate_subdirs(nativeapp_teardown, runner, temporary_directory):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                stage_subdirectory="v1",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": "\n",
        },
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        # validate the app's setup script
        result = runner.invoke_with_connection(split("app validate"))
        assert result.exit_code == 0, result.output
        assert "Native App validation succeeded." in result.output


@pytest.mark.integration
def test_nativeapp_validate_failing(nativeapp_teardown, runner, temporary_directory):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            # Create invalid SQL file
            "setup.sql": dedent(
                """\
                CREATE OR ALTER VERSIONED SCHEMA core;
                Lorem ipsum dolor sit amet
                """
            ),
            "README.md": "\n",
            "manifest.yml": "\n",
        },
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        # validate the app's setup script, this will fail
        # because we include an empty file
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 1, result.output
        assert "Snowflake Native App setup script failed validation." in result.output
        assert "syntax error" in result.output


@pytest.mark.integration
def test_nativeapp_validate_failing_w_subdir(
    nativeapp_teardown, runner, temporary_directory
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                stage_subdirectory="v1",
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            # Create invalid SQL file
            "setup.sql": dedent(
                """\
                CREATE OR ALTER VERSIONED SCHEMA core;
                Lorem ipsum dolor sit amet
                """
            ),
            "README.md": "\n",
            "manifest.yml": "\n",
        },
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        # validate the app's setup script, this will fail
        # because we include an empty file
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 1, result.output
        assert "Snowflake Native App setup script failed validation." in result.output
        assert "syntax error" in result.output


@pytest.mark.integration
def test_nativeapp_validate_with_post_deploy_hooks(
    nativeapp_teardown, runner, temporary_directory
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                meta__post_deploy=[
                    {"sql_script": "pkg_post_deploy1.sql"},
                ],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
                meta__post_deploy=[
                    {"sql_script": "app_post_deploy1.sql"},
                ],
            ),
        ),
        files={
            "app_post_deploy1.sql": "\n",
            "pkg_post_deploy1.sql": "\n",
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": "\n",
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_nativeapp_validate_with_artifacts_processor(
    nativeapp_teardown, runner, temporary_directory
):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier="myapp_pkg",
                artifacts=[
                    "setup.sql",
                    "README.md",
                    "manifest.yml",
                    # just needs to have the templates processor to nest phases
                    {"src": "app/*", "dest": "./", "processors": ["templates"]},
                ],
            ),
            app=ApplicationEntityModelFactory(
                identifier="myapp",
                fromm__target="pkg",
            ),
        ),
        files={
            "setup.sql": "CREATE OR ALTER VERSIONED SCHEMA core;",
            "README.md": "\n",
            "manifest.yml": "\n",
            "app/dummy_file.md": "\n",
        },
    )

    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        result = runner.invoke_with_connection(["app", "validate"])
        assert result.exit_code == 0, result.output
