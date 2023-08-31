from snowcli.cli.nativeapp.manager import (
    render_yml_from_jinja,
    _sparse_checkout,
    _move_and_rename_project,
)
from tests.testing_utils.fixtures import *
from textwrap import dedent


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


def test_render_yml_from_jinja(other_directory):

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
    render_yml_from_jinja(temp_dir)
    assert Path.exists(temp_dir.joinpath("snowflake.yml"))
    assert not Path.exists(temp_dir.joinpath("snowflake.yml.jinja"))
