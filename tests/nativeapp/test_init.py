from textwrap import dedent

from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.plugins.nativeapp.init import (
    CannotInitializeAnExistingProjectError,
    DirectoryAlreadyExistsError,
    InitError,
    ProjectNameInvalidError,
    RenderingFromJinjaError,
    TemplateNotFoundError,
    _render_snowflake_yml,
    _replace_snowflake_yml_name_with_project,
    _to_yaml_string,
    _validate_and_update_snowflake_yml,
    nativeapp_init,
)

from tests.testing_utils.fixtures import *

PROJECT_PATH = "demo-na-project"
PROJECT_NAME = "demo_na_project"
CUSTOM_GIT_REPO_URL = "https://testing.com/my-native-app-template"


SNOWFLAKE_YML = dedent(
    """\
    definition_version: 1
    native_app:
        name: demo_fixed_na_project
        artifacts:
            - app/setup_script.sql
"""
)

TEMPLATED_SNOWFLAKE_YML = dedent(
    """\
    definition_version: 1
    native_app:
        name: {{project_name}}
        artifacts:
            - app/setup_script.sql
"""
)

# --------------------------------------
# ----- Test Harness Helpers -------
# --------------------------------------


def fake_clone_template_with_files(files: dict):
    def fake_clone_mock(url: str, to_path: str, filter: list, depth: int):
        repo_path = Path(to_path)
        repo_path.mkdir(parents=True, exist_ok=True)

        # create a fake .git directory
        git_dir_path = repo_path / ".git"
        git_dir_path.mkdir()

        for file_name in files:
            file_contents = files[file_name]
            create_named_file(
                file_name=file_name,
                dir=str(repo_path),
                contents=[file_contents],
            )

    return fake_clone_mock


def fake_clone_template_with_file(file_name: str, file_contents: str):
    return fake_clone_template_with_files({file_name: file_contents})


def fake_clone_default_repo(url: str, to_path: str, filter: list, depth: int):
    assert url == "https://github.com/snowflakedb/native-apps-templates"
    repo_path = Path(to_path)
    repo_path.mkdir(parents=True, exist_ok=True)

    # create a fake .git directory
    git_dir_path = repo_path / ".git"
    git_dir_path.mkdir()

    # create a fake basic template
    basic_template_dir = repo_path / "basic"
    basic_template_dir.mkdir()
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=str(basic_template_dir),
        contents=[TEMPLATED_SNOWFLAKE_YML],
    )

    # create a fake python-streamlit template
    py_template_dir = repo_path / "python-streamlit"
    py_template_dir.mkdir()
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=str(py_template_dir),
        contents=[TEMPLATED_SNOWFLAKE_YML],
    )


def fake_clone_jinja_template_repo(url: str, to_path: str, filter: list, depth: int):
    fn = fake_clone_template_with_file("snowflake.yml.jinja", TEMPLATED_SNOWFLAKE_YML)
    return fn(url=url, to_path=to_path, filter=filter, depth=depth)


def fake_clone_template_repo(url: str, to_path: str, filter: list, depth: int):
    fn = fake_clone_template_with_file("snowflake.yml", SNOWFLAKE_YML)
    return fn(url=url, to_path=to_path, filter=filter, depth=depth)


# --------------------------------------
# ----- Tests for Helper Methods -------
# --------------------------------------


@pytest.mark.parametrize(
    "python_string,yaml_string",
    [
        ("abc", "abc"),
        ("_aBc_$", "_aBc_$"),
        ('"abc"', "'\"abc\"'"),
        ('"abc""def"', '\'"abc""def"\''),
    ],
)
def test_to_yaml_string(python_string, yaml_string):
    assert _to_yaml_string(python_string) == yaml_string


def test_render_snowflake_yml(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=str(temp_dir),
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
            name: {PROJECT_NAME}
            artifacts:
                - app/setup_script.sql

        """
    )
    _render_snowflake_yml(
        parent_to_snowflake_yml=temp_dir, project_identifier=PROJECT_NAME
    )
    assert Path.exists(temp_dir / "snowflake.yml")
    assert not Path.exists(temp_dir / "snowflake.yml.jinja")
    assert temp_dir.joinpath("snowflake.yml").read_text() == expected


def test_render_snowflake_yml_raises_exception(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml.jinja",
        dir=str(temp_dir),
        contents=[
            dedent(
                """\
            native_app:
                name: {{project_name}}
                artifacts:
                    - {{one_more_variable}}
            """
            )
        ],
    )
    with pytest.raises(RenderingFromJinjaError):
        _render_snowflake_yml(
            parent_to_snowflake_yml=temp_dir, project_identifier=PROJECT_NAME
        )


def test_replace_snowflake_yml_name_with_project_populated_file(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml",
        dir=str(temp_dir),
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
          name: {PROJECT_NAME}
          artifacts:
          - app/setup_script.sql
        """
    )
    path_to_snowflake_yml = temp_dir / "snowflake.yml"
    _replace_snowflake_yml_name_with_project(
        target_directory=temp_dir, project_identifier=PROJECT_NAME
    )
    assert Path.exists(path_to_snowflake_yml)
    assert path_to_snowflake_yml.read_text() == expected


def test_replace_snowflake_yml_name_with_project_empty_file(other_directory):
    temp_dir = Path(other_directory)
    create_named_file(
        file_name="snowflake.yml",
        dir=str(temp_dir),
        contents=[""],
    )
    expected = dedent(
        f"""\
        native_app:
          name: {PROJECT_NAME}

        """
    )
    path_to_snowflake_yml = temp_dir / "snowflake.yml"
    _replace_snowflake_yml_name_with_project(
        target_directory=temp_dir, project_identifier=PROJECT_NAME
    )
    assert Path.exists(path_to_snowflake_yml)
    assert not path_to_snowflake_yml.read_text().strip()


def test_validate_and_update_snowflake_yml_w_missing_yml(other_directory):
    temp_dir = Path(other_directory)

    with pytest.raises(MissingConfiguration):
        _validate_and_update_snowflake_yml(
            target_directory=temp_dir, project_identifier=PROJECT_NAME
        )


# --------------------------------------
# ----- Tests for nativeapp_init -------
# --------------------------------------


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_default_repo)
def test_nativeapp_init_with_default_template_and_repo(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(path=PROJECT_PATH, name=PROJECT_NAME)

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert not project_path.joinpath("snowflake.yml.jinja").exists()
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_default_repo)
def test_nativeapp_init_with_template_name_and_default_repo(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(
        path=PROJECT_PATH, name=PROJECT_NAME, template="python-streamlit"
    )

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert not project_path.joinpath("snowflake.yml.jinja").exists()
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_repo)
def test_nativeapp_init_with_custom_repo(mock_clone_from, temp_dir, snapshot):
    project = nativeapp_init(
        path=PROJECT_PATH, name=PROJECT_NAME, git_url=CUSTOM_GIT_REPO_URL
    )

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_jinja_template_repo)
def test_nativeapp_init_with_custom_repo_expands_jinja_snowflake_yml(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(
        path=PROJECT_PATH, name=PROJECT_NAME, git_url=CUSTOM_GIT_REPO_URL
    )

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert not project_path.joinpath("snowflake.yml.jinja").exists()
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_with_files({}))
def test_nativeapp_init_with_custom_repo_missing_snowflake_yml(
    mock_clone_from, temp_dir
):
    with pytest.raises(InitError):
        nativeapp_init(
            path=PROJECT_PATH, name=PROJECT_NAME, git_url=CUSTOM_GIT_REPO_URL
        )


@mock.patch(
    "git.Repo.clone_from",
    side_effect=fake_clone_template_with_file("snowflake.yml.jinja", "{{invalid}}"),
)
def test_nativeapp_init_with_custom_repo_invalid_snowflake_yml(
    mock_clone_from, temp_dir
):
    with pytest.raises(InitError):
        nativeapp_init(
            path=PROJECT_PATH, name=PROJECT_NAME, git_url=CUSTOM_GIT_REPO_URL
        )


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_default_repo)
def test_nativeapp_init_with_unknown_template_name(mock_clone_from, temp_dir):
    with pytest.raises(TemplateNotFoundError):
        nativeapp_init(path=PROJECT_PATH, name=PROJECT_NAME, template="does-not-exist")


@mock.patch(
    "snowflake.cli.plugins.nativeapp.init._init_from_template", return_value=None
)
def test_init_expands_user(mock_init_from_template):
    expanded_project_path = Path("/home/testuser/my_app")
    with mock.patch("pathlib.Path.expanduser", return_value=expanded_project_path) as m:
        project = nativeapp_init(path="~testuser/my_app")
    assert project.name == "my_app"
    assert project.path == expanded_project_path.resolve()


@mock.patch(
    "snowflake.cli.plugins.nativeapp.init._init_from_template", return_value=None
)
def test_init_fails_on_invalid_path(mock_init_from_template):
    with pytest.raises(InitError):
        with mock.patch("pathlib.Path.expanduser", side_effect=RuntimeError()) as m:
            project = nativeapp_init(path="~testuser/my_app")


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_default_repo)
def test_nativeapp_init_with_explicit_quoted_name(mock_clone_from, temp_dir, snapshot):
    project = nativeapp_init(path=PROJECT_PATH, name='"double quoted"')

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert not project_path.joinpath("snowflake.yml.jinja").exists()
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_default_repo)
def test_nativeapp_init_with_explicit_case_sensitive_name(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(path=PROJECT_PATH, name='"DemoNAProject"')

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert not project_path.joinpath("snowflake.yml.jinja").exists()
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_repo)
def test_nativeapp_init_with_explicit_case_sensitive_name_whole_repo(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(
        path=PROJECT_PATH, name='"DemoNAProject"', git_url=CUSTOM_GIT_REPO_URL
    )

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_repo)
def test_nativeapp_init_with_implicit_double_quoted_name(
    mock_clone_from, temp_dir, snapshot
):
    project = nativeapp_init(
        path=PROJECT_PATH, name="demo na app", git_url=CUSTOM_GIT_REPO_URL
    )

    project_path = Path(PROJECT_PATH)
    assert project_path.resolve() == project.path.resolve()
    assert project_path.exists()
    assert not Path.exists(project_path / ".git")
    assert project_path.joinpath("snowflake.yml").read_text() == snapshot


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_repo)
def test_nativeapp_init_with_explicit_unterminated_name(
    mock_clone_from, temp_dir, snapshot
):
    with pytest.raises(ProjectNameInvalidError):
        nativeapp_init(
            path=PROJECT_PATH, name='"demo na app', git_url=CUSTOM_GIT_REPO_URL
        )


@mock.patch("git.Repo.clone_from", side_effect=fake_clone_template_repo)
def test_nativeapp_init_with_explicit_trailing_quote_in_name(
    mock_clone_from, temp_dir, snapshot
):
    with pytest.raises(ProjectNameInvalidError):
        nativeapp_init(
            path=PROJECT_PATH, name='demo na app"', git_url=CUSTOM_GIT_REPO_URL
        )


# --------------------------------------
# ----- Tests for Short Circuits -------
# --------------------------------------


@mock.patch("pathlib.Path.is_file", return_value=True)
@mock.patch("pathlib.Path.exists", return_value=False)
def test_init_w_existing_yml(mock_path_is_file, mock_path_exists):
    with pytest.raises(CannotInitializeAnExistingProjectError):
        nativeapp_init(path=PROJECT_PATH)


@mock.patch("pathlib.Path.exists", return_value=True)
def test_init_w_existing_directory(mock_path_exists):
    with pytest.raises(DirectoryAlreadyExistsError):
        nativeapp_init(path=PROJECT_PATH)


@mock.patch("pathlib.Path.exists", return_value=False)
def test_init_w_invalid_project_name(mock_path_exists):
    with pytest.raises(ProjectNameInvalidError):
        nativeapp_init(path=PROJECT_PATH, name="")  # empty name is rejected
