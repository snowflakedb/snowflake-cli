from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.snowpark.snowpark_project_paths import Artefact

deploy_path = Path("output") / "deploy" / "snowpark"
absolute_deploy_path = Path.cwd().absolute() / "output" / "deploy" / "snowpark"


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, "@db.public.stage/src.zip"),
        ("src/", None, False, "@db.public.stage/src.zip"),
        ("src", "source", False, "@db.public.stage/source/src.zip"),
        ("src/app.py", None, True, "@db.public.stage/src/app.py"),
        ("src/app.py", "source/new_app.py", True, "@db.public.stage/source/new_app.py"),
        ("src/dir/dir2/app.py", None, True, "@db.public.stage/src/dir/dir2/app.py"),
        ("src/dir/dir2/app.py", "source/", True, "@db.public.stage/source/app.py"),
        ("src/*", "source/", False, "@db.public.stage/source/src.zip"),
        ("src/**/*.py", None, False, "@db.public.stage/src.zip"),
        ("src/**/*.py", "source/", False, "@db.public.stage/source/src.zip"),
        ("src/app*", None, False, "@db.public.stage/src.zip"),
        ("src/app[1-5].py", None, False, "@db.public.stage/src.zip"),
    ],
)
@mock.patch("snowflake.cli.api.cli_global_context.get_cli_context")
def test_artifact_import_path(mock_ctx_context, path, dest, is_file, expected_path):
    mock_connection = mock.Mock()
    mock_connection.database = "db"
    mock_connection.schema = "public"
    mock_ctx_context.return_value.connection = mock_connection
    stage = "stage"

    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        import_path = Artefact(
            Path(), Path("output") / "deploy" / "snowpark", Path(path), dest
        ).import_path(stage)

    assert import_path == expected_path


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, "@db.public.stage/"),
        ("src/", None, False, "@db.public.stage/"),
        ("src", "source", False, "@db.public.stage/source/"),
        ("src/app.py", None, True, "@db.public.stage/src/"),
        ("src/app.py", "source/new_app.py", True, "@db.public.stage/source/"),
        ("src/dir/dir2/app.py", None, True, "@db.public.stage/src/dir/dir2/"),
        ("src/dir/dir2/app.py", "source/", True, "@db.public.stage/source/"),
        ("src/*", "source/", False, "@db.public.stage/source/"),
        ("src/**/*.py", None, False, "@db.public.stage/"),
        ("src/**/*.py", "source/", False, "@db.public.stage/source/"),
        ("src/app*", None, False, "@db.public.stage/"),
        ("src/app[1-5].py", None, False, "@db.public.stage/"),
    ],
)
@mock.patch("snowflake.cli.api.cli_global_context.get_cli_context")
def test_artifact_upload_path(mock_ctx_context, path, dest, is_file, expected_path):
    mock_connection = mock.Mock()
    mock_connection.database = "db"
    mock_connection.schema = "public"
    mock_ctx_context.return_value.connection = mock_connection

    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        upload_path = Artefact(
            Path(), Path("output") / "deploy" / "snowpark", Path(path), dest
        ).upload_path("stage")

    assert upload_path == expected_path


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, deploy_path / "src.zip"),
        ("src/", None, False, deploy_path / "src.zip"),
        ("src", "source", False, deploy_path / "source" / "src.zip"),
        ("src/app.py", None, True, deploy_path / "src" / "app.py"),
        (
            "src/app.py",
            "source/new_app.py",
            True,
            deploy_path / "source" / "new_app.py",
        ),
        ("src/*", "source/new_app.py", True, deploy_path / "source" / "new_app.py"),
        (
            "src/dir/dir2/app.py",
            None,
            True,
            deploy_path / "src" / "dir" / "dir2" / "app.py",
        ),
        (
            "src/dir/dir2/app.py",
            "source/",
            True,
            deploy_path / "source" / "app.py",
        ),
        ("src/*", "source/", False, deploy_path / "source" / "src.zip"),
        ("src/**/*.py", None, False, deploy_path / "src.zip"),
        ("src/**/*.py", "source/", False, deploy_path / "source" / "src.zip"),
        ("src/app*", None, False, deploy_path / "src.zip"),
        ("src/app[1-5].py", None, False, deploy_path / "src.zip"),
    ],
)
def test_artifact_post_build_path(path, dest, is_file, expected_path):
    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        post_build_path = Artefact(
            Path(), Path("output") / "deploy" / "snowpark", Path(path), dest
        ).post_build_path

    assert post_build_path == expected_path


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, "@db.public.stage/src.zip"),
        ("src/", None, False, "@db.public.stage/src.zip"),
        ("src", "source", False, "@db.public.stage/source/src.zip"),
        ("src/app.py", None, True, "@db.public.stage/src/app.py"),
        ("src/app.py", "source/new_app.py", True, "@db.public.stage/source/new_app.py"),
        ("src/dir/dir2/app.py", None, True, "@db.public.stage/src/dir/dir2/app.py"),
        ("src/dir/dir2/app.py", "source/", True, "@db.public.stage/source/app.py"),
        ("src/*", "source/", False, "@db.public.stage/source/src.zip"),
        ("src/**/*.py", None, False, "@db.public.stage/src.zip"),
        ("src/**/*.py", "source/", False, "@db.public.stage/source/src.zip"),
        ("src/app*", None, False, "@db.public.stage/src.zip"),
        ("src/app[1-5].py", None, False, "@db.public.stage/src.zip"),
    ],
)
@mock.patch("snowflake.cli.api.cli_global_context.get_cli_context")
def test_artifact_import_path_from_other_directory(
    mock_ctx_context, path, dest, is_file, expected_path
):
    mock_connection = mock.Mock()
    mock_connection.database = "db"
    mock_connection.schema = "public"
    mock_ctx_context.return_value.connection = mock_connection
    stage = "stage"

    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        import_path = Artefact(
            Path("/tmp"),
            Path("/tmp") / "output" / "deploy" / "snowpark",
            Path(path),
            dest,
        ).import_path(stage)

    assert import_path == expected_path


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, "@db.public.stage/"),
        ("src/", None, False, "@db.public.stage/"),
        ("src", "source", False, "@db.public.stage/source/"),
        ("src/app.py", None, True, "@db.public.stage/src/"),
        ("src/app.py", "source/new_app.py", True, "@db.public.stage/source/"),
        ("src/dir/dir2/app.py", None, True, "@db.public.stage/src/dir/dir2/"),
        ("src/dir/dir2/app.py", "source/", True, "@db.public.stage/source/"),
        ("src/*", "source/", False, "@db.public.stage/source/"),
        ("src/**/*.py", None, False, "@db.public.stage/"),
        ("src/**/*.py", "source/", False, "@db.public.stage/source/"),
        ("src/app*", None, False, "@db.public.stage/"),
        ("src/app[1-5].py", None, False, "@db.public.stage/"),
    ],
)
@mock.patch("snowflake.cli.api.cli_global_context.get_cli_context")
def test_artifact_upload_path_from_other_directory(
    mock_ctx_context, path, dest, is_file, expected_path
):
    mock_connection = mock.Mock()
    mock_connection.database = "db"
    mock_connection.schema = "public"
    mock_ctx_context.return_value.connection = mock_connection

    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        upload_path = Artefact(
            Path("/tmp"), Path("/tmp") / "output" / "deploy", Path(path), dest
        ).upload_path("stage")

    assert upload_path == expected_path


@pytest.mark.parametrize(
    "path, dest, is_file, expected_path",
    [
        ("src", None, False, absolute_deploy_path / "src.zip"),
        ("src/", None, False, absolute_deploy_path / "src.zip"),
        (
            "src",
            "source",
            False,
            absolute_deploy_path / "source" / "src.zip",
        ),
        ("src/app.py", None, True, absolute_deploy_path / "src" / "app.py"),
        (
            "src/app.py",
            "source/new_app.py",
            True,
            absolute_deploy_path / "source" / "new_app.py",
        ),
        (
            "src/dir/dir2/app.py",
            None,
            True,
            absolute_deploy_path / "src" / "dir" / "dir2" / "app.py",
        ),
        (
            "src/dir/dir2/app.py",
            "source/",
            True,
            absolute_deploy_path / "source" / "app.py",
        ),
        (
            "src/*",
            "source/",
            False,
            absolute_deploy_path / "source" / "src.zip",
        ),
        ("src/**/*.py", None, False, absolute_deploy_path / "src.zip"),
        (
            "src/**/*.py",
            "source/",
            False,
            absolute_deploy_path / "source" / "src.zip",
        ),
        ("src/app*", None, False, absolute_deploy_path / "src.zip"),
        ("src/app[1-5].py", None, False, absolute_deploy_path / "src.zip"),
    ],
)
def test_artifact_post_build_path_from_other_directory(
    path, dest, is_file, expected_path
):
    with mock.patch.object(Path, "is_file" if is_file else "is_dir", return_value=True):
        post_build_path = Artefact(
            Path.cwd().absolute(),
            Path.cwd().absolute() / "output" / "deploy" / "snowpark",
            Path(path),
            dest,
        ).post_build_path

    assert post_build_path == expected_path
