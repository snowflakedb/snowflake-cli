import os
import os.path
import uuid
from textwrap import dedent

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import pushd

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.fixture
def template_setup(request, runner, temporary_working_directory):
    try:
        template_name = request.param
    except:
        template_name = "basic"  # default
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name, "--template", template_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0
    return project_name, temporary_working_directory, runner


# Tests that we disallow polluting the project source through symlinks
@pytest.mark.integration
@pytest.mark.parametrize("template_setup", ["basic", "streamlit-python"], indirect=True)
def test_nativeapp_bundle_does_not_create_files_outside_deploy_root(
    template_setup,
):
    project_name, temporary_working_directory, runner = template_setup

    with pushd(Path(temporary_working_directory, project_name)):
        # overwrite the snowflake.yml rules
        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
            definition_version: 1
            native_app:
              name: myapp
              artifacts:
                - src: app
                  dest: ./
                - src: snowflake.yml
                  dest: ./app/
            """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert (
            "The specified destination path is outside of the deploy root"
            in result.output
        )

        assert not os.path.exists("app/snowflake.yml")


# Tests restrictions on the deploy root: It must be a sub-directory within the project directory
@pytest.mark.integration
@pytest.mark.parametrize("template_setup", ["basic", "streamlit-python"], indirect=True)
def test_nativeapp_bundle_throws_error_due_to_project_root_deploy_root_mismatch(
    template_setup,
):

    project_name, temporary_working_directory, runner = template_setup

    with pushd(Path(temporary_working_directory, project_name)) as project_dir:
        deploy_root = Path(project_dir, "output")
        deploy_root.mkdir(parents=True, exist_ok=False)
        # Make deploy root a file instead of directory
        deploy_root_as_file = Path(deploy_root, "deploy")
        with open(deploy_root_as_file, "x"):
            pass

        assert deploy_root_as_file.is_file()

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )

        assert result.exit_code == 1
        assert "exists, but is not a directory!" in result.output

        os.remove(deploy_root_as_file)
        deploy_root.rmdir()

    original_cwd = os.getcwd()
    assert not Path(original_cwd, "output").exists()

    # Make deploy root outside the project directory
    deploy_root = Path(original_cwd, "output", "deploy")
    deploy_root.mkdir(parents=True, exist_ok=False)

    with pushd(Path(original_cwd, project_name)) as project_dir:
        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
            definition_version: 1
            native_app:
              name: myapp
              deploy_root: {deploy_root}
              artifacts:
                - src: app
                  dest: ./
                - src: snowflake.yml
                  dest: ./app/
            """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )

        assert result.exit_code == 1
        assert "is not a descendent of the project directory!" in result.output


# Tests restrictions on the src spec: It must be a glob that returns matches, and must be relative to project root
@pytest.mark.integration
@pytest.mark.parametrize(
    "pdf_content, error_msg",
    [
        [
            dedent(
                f"""
                definition_version: 1
                native_app:
                  name: myapp
                  artifacts:
                    - app/?
                """
            ),
            "No match was found for the specified source in the project directory",
        ],
        [
            dedent(
                f"""
                definition_version: 1
                native_app:
                  name: myapp
                  artifacts:
                    - /app
                """
            ),
            "Source path must be a relative path",
        ],
    ],
)
def test_nativeapp_bundle_throws_error_on_incorrect_src_glob(
    pdf_content, error_msg, template_setup
):
    project_name, temporary_working_directory, runner = template_setup

    with pushd(Path(temporary_working_directory, project_name)):
        # overwrite the snowflake.yml with incorrect glob
        with open("snowflake.yml", "w") as f:
            f.write(pdf_content)

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert error_msg in result.output


# Tests restrictions on the dest spec: It must be within the deploy root, and must be a relative path
@pytest.mark.integration
@pytest.mark.parametrize("template_setup", ["basic", "streamlit-python"], indirect=True)
def test_nativeapp_bundle_throws_error_on_bad_dest(template_setup):
    project_name, temporary_working_directory, runner = template_setup

    with pushd(Path(temporary_working_directory, project_name)):
        # overwrite the snowflake.yml rules
        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
                definition_version: 1
                native_app:
                  name: myapp
                  artifacts:
                    - src: app/*
                      dest: /
                """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert (
            "The specified destination path is outside of the deploy root"
            in result.output
        )

        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
                definition_version: 1
                native_app:
                  name: myapp
                  artifacts:
                    - src: app/*
                      dest: /Users/
                """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert "Destination path must be a relative path" in result.output


# Tests restriction on mapping multiple files to the same destination file
@pytest.mark.integration
@pytest.mark.parametrize("template_setup", ["basic", "streamlit-python"], indirect=True)
def test_nativeapp_bundle_throws_error_on_too_many_files_to_dest(template_setup):

    project_name, temporary_working_directory, runner = template_setup
    with pushd(Path(temporary_working_directory, project_name)):
        # overwrite the snowflake.yml rules
        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
                definition_version: 1
                native_app:
                  name: myapp
                  artifacts:
                    - src: app/manifest.yml
                      dest: manifest.yml
                    - src: app/setup_script.sql
                      dest: manifest.yml
                """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert "Multiple files were mapped to one output file." in result.output


# Tests that bundle wipes out any existing deploy root to recreate it from scratch on every run
@pytest.mark.integration
@pytest.mark.parametrize("template_setup", ["basic", "streamlit-python"], indirect=True)
def test_nativeapp_bundle_deletes_existing_deploy_root(template_setup):
    project_name, temporary_working_directory, runner = template_setup

    with pushd(Path(temporary_working_directory, project_name)) as project_dir:
        existing_deploy_root_dest = Path(project_dir, "output", "deploy", "dummy.txt")
        existing_deploy_root_dest.mkdir(parents=True, exist_ok=False)
        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0
        assert not existing_deploy_root_dest.exists()
