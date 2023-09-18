from snowcli.cli.nativeapp.init import (
    is_valid_project_name,
    render_snowflake_yml,
    render_nativeapp_readme,
    nativeapp_init,
    replace_snowflake_yml_name_with_project,
    validate_and_update_snowflake_yml,
    _init_with_url_and_no_template,
    _init_with_url_and_template,
    CannotInitializeAnExistingProjectError,
    DirectoryAlreadyExistsError,
    InitError,
    ProjectNameInvalidError,
)
from snowcli.exception import MissingConfiguration
from tests.testing_utils.fixtures import *
from textwrap import dedent
from secrets import choice
from string import ascii_letters, digits

PROJECT_NAME = "demo_na_project"
MAX_ALLOWED_NUM_CHARACTERS = 255

# --------------------------------------
# ----- Tests for Helper Methods -------
# --------------------------------------


@pytest.mark.parametrize(
    "project_name, expected",
    [
        ("_", True),  # Edge Case: Only One Character from the Allowed List
        ("A", True),  # Edge Case: Only One Character from the Allowed List
        ("9", False),  # Edge Case: Only One Character not from the Allowed List
        ("_aB3_$", True),  # Test all allowed character types
        ("__", True),  # Test all allowed character types
        (
            "".join(choice(ascii_letters) for i in range(MAX_ALLOWED_NUM_CHARACTERS)),
            True,
        ),
        (
            "".join(
                choice(ascii_letters) for i in range(MAX_ALLOWED_NUM_CHARACTERS - 2)
            ).join("*%"),
            False,
        ),
    ],
)
def test_is_valid_project_name(project_name, expected):
    assert is_valid_project_name(project_name) == expected


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
    assert Path.exists(temp_dir / "snowflake.yml")
    assert not Path.exists(temp_dir / "snowflake.yml.jinja")
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
    assert Path.exists(temp_dir / "README.md")
    assert not Path.exists(temp_dir / "README.md.jinja")
    assert temp_dir.joinpath("README.md").read_text() == expected


def test_replace_snowflake_yml_name_with_project_populated_file(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml",
        dir=temp_dir,
        contents=[
            dedent(
                """\
            native_app:
                name: old_value
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
    path_to_snowflake_yml = temp_dir / "snowflake.yml"
    replace_snowflake_yml_name_with_project(temp_dir)
    assert Path.exists(path_to_snowflake_yml)
    assert path_to_snowflake_yml.read_text() == expected


def test_replace_snowflake_yml_name_with_project_empty_file(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml",
        dir=temp_dir,
        contents=[""],
    )
    expected = dedent(
        f"""\
        native_app:
          name: {temp_dir.name}

        """
    )
    path_to_snowflake_yml = temp_dir / "snowflake.yml"
    replace_snowflake_yml_name_with_project(temp_dir)
    assert Path.exists(path_to_snowflake_yml)
    assert not path_to_snowflake_yml.read_text().strip()


def test_validate_and_update_snowflake_yml_w_missing_yml(other_directory):
    temp_dir = Path(other_directory)

    with pytest.raises(MissingConfiguration):
        validate_and_update_snowflake_yml(target_directory=temp_dir)


# ------------------------------------------------------
# ----- Tests for _init_with_url_and_no_template -------
# ------------------------------------------------------


def test_init_with_url_and_no_template_fail_on_validation(temp_dir):
    # Should fail as git URL provided has no snowflake.yml file to validate
    with pytest.raises(InitError):
        _init_with_url_and_no_template(
            current_working_directory=Path.cwd(),
            project_name="new_project",
            git_url="https://github.com/Snowflake-Labs/sf-samples",
        )
    assert not Path.exists(Path.cwd() / "new_project")


@mock.patch("snowcli.cli.nativeapp.init.Repo.clone_from", side_effect=None)
def test_init_with_url_and_no_template_w_native_app_url(mock_clone_from, temp_dir):
    # Prepare fake repository
    fake_repo = "fake_repo"
    current_working_directory = Path.cwd()
    current_working_directory.joinpath(fake_repo, ".git").mkdir(
        parents=True, exist_ok=False
    )
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory / fake_repo,
        contents=[
            dedent(
                """\
            definition_version: 1
            native_app:
                name: minimal

                artifacts:
                    - setup.sql
                    - README.md
            """
            )
        ],
    )

    # Now mock the mkdir call so that a new project is not created, and the fake repo above is used instead
    with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
        mock_mkdir.return_value = None
        mock_mkdir.side_effect = None

        _init_with_url_and_no_template(
            current_working_directory=Path.cwd(),
            project_name=fake_repo,
            git_url="https://github.com/Snowflake-Labs/native-apps-templates",
        )

        dest = Path.cwd() / fake_repo
        assert dest.exists()
        assert not Path.exists(dest / ".git")


@mock.patch("snowcli.cli.nativeapp.init.Repo.clone_from", side_effect=None)
def test_init_with_url_and_no_template_w_random_url(mock_clone_from, temp_dir):

    # Prepare fake repository
    fake_repo = "fake_repo"
    current_working_directory = Path.cwd()
    current_working_directory.joinpath(fake_repo, ".git").mkdir(
        parents=True, exist_ok=False
    )
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory / fake_repo,
        contents=[
            dedent(
                """\
            definition_version: 1
            native_app:
                name: minimal

                artifacts:
                    - setup.sql
                    - README.md
            """
            )
        ],
    )

    expected = dedent(
        f"""\
        definition_version: 1
        native_app:
          name: fake_repo
          artifacts:
          - setup.sql
          - README.md
        """
    )

    # Now mock the mkdir call so that a new project is not created, and the fake repo above is used instead
    with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
        mock_mkdir.return_value = None
        mock_mkdir.side_effect = None

        _init_with_url_and_no_template(
            current_working_directory=Path.cwd(),
            project_name=fake_repo,
            git_url="https://github.com/Snowflake-Labs/sf-samples",
        )

        dest = Path.cwd() / fake_repo
        assert dest.exists()
        assert not Path.exists(dest / ".git")
        assert dest.joinpath("snowflake.yml").read_text() == expected


# ---------------------------------------------------
# ----- Tests for _init_with_url_and_template -------
# ---------------------------------------------------


@mock.patch("snowcli.cli.nativeapp.init.Repo.clone_from", side_effect=None)
@mock.patch("snowcli.cli.nativeapp.init.move", side_effect=None)
@mock.patch("os.getenv", return_value="pytest_user")
def test_init_with_url_and_template_w_native_app_url_and_template(
    mock_clone_from, mock_move, mock_getenv, temp_dir
):
    # Prepare fake repository
    fake_repo = "fake_repo"
    current_working_directory = Path.cwd()
    current_working_directory.joinpath(fake_repo, "app").mkdir(
        parents=True, exist_ok=False
    )
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=current_working_directory / fake_repo,
        contents=[
            dedent(
                """\
            definition_version: 1
            native_app:
                name: {{project_name}}

                artifacts:
                    - setup.sql
                    - README.md
            """
            )
        ],
    )

    expected_snowflake_yml = dedent(
        """\
            definition_version: 1
            native_app:
                name: fake_repo
            
                artifacts:
                    - setup.sql
                    - README.md
            
            """
    )

    create_named_file(
        file_name="README.md.jinja",
        dir=current_working_directory / fake_repo / "app",
        contents=[dedent("{{application_name}}")],
    )
    expected_readme = dedent("fake_repo_pytest_user\n")

    _init_with_url_and_template(
        current_working_directory=Path.cwd(),
        project_name=fake_repo,
        git_url="https://github.com/Snowflake-Labs/native-apps-templates",
        template="native-apps-basic",
    )

    fake_repo_path = current_working_directory / fake_repo
    assert fake_repo_path.exists()
    assert not Path.exists(fake_repo_path / ".git")
    assert (
        fake_repo_path.joinpath("snowflake.yml").read_text() == expected_snowflake_yml
    )
    assert fake_repo_path.joinpath("app", "README.md").read_text() == expected_readme


@mock.patch("snowcli.cli.nativeapp.init.Repo.clone_from", side_effect=None)
@mock.patch("snowcli.cli.nativeapp.init.move", side_effect=None)
@mock.patch("os.getenv", return_value="pytest_user")
def test_init_with_url_and_template_w_random_url_and_template(
    mock_clone_from, mock_move, mock_getenv, temp_dir, snapshot
):
    # Prepare fake repository
    fake_repo = "fake_repo"
    current_working_directory = Path.cwd()
    current_working_directory.joinpath(fake_repo).mkdir(parents=True, exist_ok=False)
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory / fake_repo,
        contents=[
            dedent(
                """\
            definition_version: 1
            native_app:
                name: <old_name>

                artifacts:
                    - setup.sql
                    - README.md
            """
            )
        ],
    )

    _init_with_url_and_template(
        current_working_directory=Path.cwd(),
        project_name=fake_repo,
        git_url="https://github.com/Snowflake-Labs/sf-samples",
        template="samples",
    )

    fake_repo_path = current_working_directory / fake_repo
    assert fake_repo_path.exists()
    assert not Path.exists(fake_repo_path / ".git")
    assert fake_repo_path.joinpath("snowflake.yml").read_text() == snapshot


# --------------------------------------
# ----- Tests for Short Circuits -------
# --------------------------------------


@mock.patch("pathlib.Path.is_file", return_value=True)
def test_init_w_existing_yml(mock_path_is_file):
    with pytest.raises(CannotInitializeAnExistingProjectError):
        nativeapp_init(name=PROJECT_NAME)


@mock.patch("pathlib.Path.exists", return_value=True)
def test_init_w_existing_directory(mock_path_exists):
    with pytest.raises(DirectoryAlreadyExistsError):
        nativeapp_init(name=PROJECT_NAME)


@mock.patch("snowcli.cli.nativeapp.init.fullmatch", return_value=None)
def test_init_w_invalid_project_name(mock_fullmatch):
    with pytest.raises(ProjectNameInvalidError):
        nativeapp_init(PROJECT_NAME)
