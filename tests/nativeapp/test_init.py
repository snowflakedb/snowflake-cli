from snowcli.cli.nativeapp.init import (
    render_snowflake_yml,
    _sparse_checkout,
    _move_and_rename_project,
    render_nativeapp_readme,
    nativeapp_init,
    CannotInitializeAnExistingProjectError,
    DirectoryAlreadyExistsError,
    GitVersionIncompatibleError,
    InitError,
)
from tests.testing_utils.fixtures import *
from textwrap import dedent

PROJECT_NAME = "demo_na_project"


def test_sparse_checkout(other_directory):
    temp_dir = other_directory
    _sparse_checkout(
        git_url="https://github.com/Snowflake-Labs/sf-samples",
        repo_sub_directory="samples",
        target_parent_directory=os.path.abspath(temp_dir),
    )
    cloned_repo = Path(temp_dir)

    assert cloned_repo.exists()
    assert cloned_repo.joinpath("samples").exists()
    assert not cloned_repo.joinpath("README.md").exists()


def test_move_and_rename_project(other_directory):
    temp_dir = other_directory
    temp_dir_path = Path(temp_dir)
    directory_to_make = temp_dir_path.joinpath("some/random/path/samples")
    directory_to_make.mkdir(parents=True, exist_ok=False)
    _move_and_rename_project(
        source_parent_directory=directory_to_make.parent,
        target_parent_directory=temp_dir_path,
        repo_sub_directory="samples",
        new_name="moved_samples_directory",
    )

    assert temp_dir_path.exists()
    assert temp_dir_path.joinpath("moved_samples_directory").exists()
    assert directory_to_make.parent.exists()
    assert not directory_to_make.exists()


def test_render_snowflake_yml(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=temp_dir,
        contents=[
            dedent(
                """\
            native_app:
                name: {{project_name}}
                artifacts:
                    - app/setup_script.sql
            """
            )
        ],
    )
    expected = dedent(
        f"""\
        native_app:
            name: {temp_dir.name}
            artifacts:
                - app/setup_script.sql

        """
    )
    render_snowflake_yml(temp_dir)
    assert Path.exists(temp_dir.joinpath("snowflake.yml"))
    assert not Path.exists(temp_dir.joinpath("snowflake.yml.jinja"))
    assert temp_dir.joinpath("snowflake.yml").read_text() == expected


@mock.patch("os.getenv", return_value="pytest_user")
def test_render_nativeapp_readme(mock_get_env_username, other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="README.md.jinja",
        dir=temp_dir,
        contents=[
            dedent(
                """\
            ### Calling a function
            SELECT {{application_name}}.versioned_schema.hello_world();
            which should output 'hello world!'
            ```
            SELECT {{application_name}}.versioned_schema.hello_world();
            ```
            """
            )
        ],
    )
    expected = dedent(
        """\
            ### Calling a function
            SELECT random_project_pytest_user.versioned_schema.hello_world();
            which should output 'hello world!'
            ```
            SELECT random_project_pytest_user.versioned_schema.hello_world();
            ```
            
            """
    )
    render_nativeapp_readme(temp_dir, "random_project")
    assert temp_dir.joinpath("README.md").exists()
    assert not temp_dir.joinpath("README.md.jinja").exists()
    assert temp_dir.joinpath("README.md").read_text() == expected


@mock.patch("pathlib.Path.is_file", return_value=True)
def test_init_no_template_w_existing_yml(mock_path_is_file):
    with pytest.raises(CannotInitializeAnExistingProjectError):
        nativeapp_init(name=PROJECT_NAME)


@mock.patch("pathlib.Path.exists", return_value=True)
def test_init_no_template_w_existing_directory(mock_path_exists):
    with pytest.raises(DirectoryAlreadyExistsError):
        nativeapp_init(name=PROJECT_NAME)


@mock.patch("subprocess.check_output", return_value="git version 2.2")
def test_init_no_template_git_fails(mock_get_client_git_version):
    with pytest.raises(GitVersionIncompatibleError):
        nativeapp_init(name=PROJECT_NAME)


@mock.patch(
    "snowcli.cli.nativeapp.init._init_without_user_provided_template",
    side_effect=InitError(),
)
def test_init_no_template_raised_exception(
    mock_init_without_user_provided_template, temp_dir
):
    with pytest.raises(InitError):
        # temp_dir will be cwd for the rest of this test
        nativeapp_init(name=PROJECT_NAME)
