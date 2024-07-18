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

import os
import os.path
import yaml
import uuid

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import enable_definition_v2_feature_flag
from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.fixture(scope="function", params=["v1", "v2"])
def template_setup(runner, project_directory, request):
    definition_version = request.param
    with enable_definition_v2_feature_flag:
        with project_directory(f"napp_init_{definition_version}") as project_root:
            # Vanilla bundle on the unmodified template
            result = runner.invoke_json(
                ["app", "bundle"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            # The newly created deploy_root is explicitly deleted here, as bundle should take care of it.

            deploy_root = Path(project_root, "output", "deploy")
            assert Path(deploy_root, "manifest.yml").is_file()
            assert Path(deploy_root, "setup_script.sql").is_file()
            assert Path(deploy_root, "README.md").is_file()

            yield project_root, runner, definition_version


def override_snowflake_yml_artifacts(
    definition_version, artifacts_section, deploy_root=Path("output", "deploy")
):
    with open("snowflake.yml", "w") as f:
        if definition_version == "v2":
            file_content = yaml.dump(
                {
                    "definition_version": "2",
                    "entities": {
                        "pkg": {
                            "type": "application package",
                            "name": "myapp_pkg_<% ctx.env.USER %>",
                            "artifacts": artifacts_section,
                            "manifest": "app/manifest.yml",
                            "deploy_root": str(deploy_root),
                        }
                    },
                }
            )
        else:
            file_content = yaml.dump(
                {
                    "definition_version": "1",
                    "native_app": {
                        "name": "myapp",
                        "artifacts": artifacts_section,
                        "deploy_root": str(deploy_root),
                    },
                }
            )
        f.write(file_content)


# Tests that we copy files/directories directly to the deploy root instead of creating symlinks.
@pytest.mark.integration
def test_nativeapp_bundle_does_explicit_copy(
    template_setup,
):
    project_root, runner, definition_version = template_setup

    override_snowflake_yml_artifacts(
        definition_version,
        artifacts_section=[
            {"src": "app", "dest": "./"},
            {"src": "snowflake.yml", "dest": "./app/"},
        ],
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 0
    assert not os.path.exists("app/snowflake.yml")
    app_path = Path("output", "deploy", "app")
    assert app_path.exists() and not app_path.is_symlink()
    assert (
        Path(app_path, "manifest.yml").exists()
        and Path(app_path, "manifest.yml").is_symlink()
    )
    assert (
        Path(app_path, "setup_script.sql").exists()
        and Path(app_path, "setup_script.sql").is_symlink()
    )
    assert (
        Path(app_path, "README.md").exists()
        and Path(app_path, "README.md").is_symlink()
    )
    assert (
        Path(app_path, "snowflake.yml").exists()
        and Path(app_path, "snowflake.yml").is_symlink()
    )


# Tests restrictions on the deploy root: It must be a sub-directory within the project directory
@pytest.mark.integration
def test_nativeapp_bundle_throws_error_due_to_project_root_deploy_root_mismatch(
    template_setup,
):

    project_root, runner, definition_version = template_setup
    # Delete deploy_root since we test requirement of deploy_root being a directory
    shutil.rmtree(Path(project_root, "output", "deploy"))

    deploy_root = Path(project_root, "output")
    # Make deploy root a file instead of directory
    deploy_root_as_file = Path(deploy_root, "deploy")
    deploy_root_as_file.touch(exist_ok=False)

    assert deploy_root_as_file.is_file()

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )

    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result, "exists, but is not a directory!"
    )

    os.remove(deploy_root_as_file)
    deploy_root.rmdir()

    # Make deploy root outside the project directory
    with tempfile.TemporaryDirectory() as tmpdir:
        assert not Path(tmpdir, "output").exists()
        deploy_root = Path(tmpdir, "output", "deploy")
        deploy_root.mkdir(parents=True, exist_ok=False)

    override_snowflake_yml_artifacts(
        definition_version,
        artifacts_section=[
            {"src": "app", "dest": "./"},
            {"src": "snowflake.yml", "dest": "./app/"},
        ],
        deploy_root=deploy_root,
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )

    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result, "is not a descendent of the project directory!"
    )


# Tests restrictions on the src spec that it must be a glob that returns matches
@pytest.mark.integration
def test_nativeapp_bundle_throws_error_on_incorrect_src_glob(template_setup):
    project_root, runner, definition_version = template_setup

    # incorrect glob
    override_snowflake_yml_artifacts(definition_version, artifacts_section=["app/?"])

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result,
        "No match was found for the specified source in the project directory",
    )


# Tests restrictions on the src spec that it must be relative to project root
@pytest.mark.integration
def test_nativeapp_bundle_throws_error_on_bad_src(template_setup):
    project_root, runner, definition_version = template_setup

    # absolute path
    src_path = Path(project_root, "app").absolute()
    override_snowflake_yml_artifacts(
        definition_version, artifacts_section=[f"{src_path}"]
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result, "Source path must be a relative path"
    )


# Tests restrictions on the dest spec: It must be within the deploy root, and must be a relative path
@pytest.mark.integration
def test_nativeapp_bundle_throws_error_on_bad_dest(template_setup):
    project_root, runner, definition_version = template_setup

    override_snowflake_yml_artifacts(
        definition_version, artifacts_section=[{"src": "app/*", "dest": "/"}]
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result, "The specified destination path is outside of the deploy root"
    )

    override_snowflake_yml_artifacts(
        definition_version,
        artifacts_section=[
            {
                "src": "app/*",
                "dest": str(
                    Path(project_root, "output", "deploy", "stagepath").absolute()
                ),
            }
        ],
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result, "Destination path must be a relative path"
    )


# Tests restriction on mapping multiple files to the same destination file
@pytest.mark.integration
def test_nativeapp_bundle_throws_error_on_too_many_files_to_dest(template_setup):
    project_root, runner, definition_version = template_setup

    override_snowflake_yml_artifacts(
        definition_version,
        artifacts_section=[
            {"src": "app/manifest.yml", "dest": "manifest.yml"},
            {"src": "app/setup_script.sql", "dest": "manifest.yml"},
        ],
    )

    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 1
    assert_that_result_failed_with_message_containing(
        result,
        "Multiple file or directories were mapped to one output destination.",
    )


# Tests that bundle wipes out any existing deploy root to recreate it from scratch on every run
@pytest.mark.integration
def test_nativeapp_bundle_deletes_existing_deploy_root(template_setup):
    project_root, runner, definition_version = template_setup

    existing_deploy_root_dest = Path(project_root, "output", "deploy", "dummy.txt")
    existing_deploy_root_dest.mkdir(parents=True, exist_ok=False)
    result = runner.invoke_json(
        ["app", "bundle"],
        env=TEST_ENV,
    )
    assert result.exit_code == 0
    assert not existing_deploy_root_dest.exists()
