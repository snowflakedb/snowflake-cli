from __future__ import annotations

from pathlib import Path

import pytest
from snowflake.cli._plugins.stage.manager import DefaultStagePathParts
from snowflake.cli.api.stage_path import StagePath

# (path, is_git_repo)
ROOT_STAGES = [
    ("~", False),
    ("~/", False),
    ("stage", False),
    ("stage/", False),
    ("db.schema.stage", False),
    ("db.schema.stage/", False),
    ("db.schema.repo/branches/main/", True),
    ('db.schema.repo/branches/"main/with/slash"/', True),
]

DIRECTORIES = [
    ("~/my_path", False),
    ("~/my_path/", False),
    ("stage/my_path", False),
    ("stage/my_path/", False),
    ("db.schema.stage/my_path", False),
    ("db.schema.stage/my_path/", False),
    ("db.schema.repo/branches/main/my_path", True),
    ("db.schema.repo/branches/main/my_path/", True),
    ('db.schema.repo/branches/"main/with/slash"/my_path', True),
]

FILES = [
    ("~/file.py", False),
    ("stage/file.py", False),
    ("db.schema.stage/file.py", False),
    ("repo/branches/main/file.py", True),
    ("db.schema.repo/branches/main/file.py", True),
    ('db.schema.repo/branches/"main/with/slash"/file.py', True),
]

FILES_UNDER_PATH = [
    ("~/my_path/file.py", False),
    ("stage/my_path/file.py", False),
    ("db.schema.stage/my_path/file.py", False),
    ("repo/branches/main/my_path/file.py", True),
    ("db.schema.repo/branches/main/my_path/file.py", True),
    ('db.schema.repo/branches/"main/with/slash"/my_path/file.py', True),
]


def with_at_prefix(test_data: list[tuple[str, bool]]):
    return [(f"@{path}", is_git_repo) for path, is_git_repo in test_data]


def with_snow_prefix(test_data: list[tuple[str, bool]]):
    return [(f"snow://{path}", is_git_repo) for path, is_git_repo in test_data]


def parametrize_with(data: list[tuple[str, bool]]):
    return pytest.mark.parametrize(
        "path, is_git_repo", [*data, *with_at_prefix(data), *with_snow_prefix(data)]
    )


def build_stage_path(path, is_git_repo):
    if is_git_repo:
        stage_path = StagePath.from_git_str(path)
    else:
        stage_path = StagePath.from_stage_str(path)
    return stage_path


@parametrize_with(ROOT_STAGES)
def test_root_paths(path, is_git_repo):
    stage_path = build_stage_path(path, is_git_repo)
    assert stage_path.is_root()
    assert stage_path.parts == ()
    assert stage_path.is_dir()
    assert not stage_path.is_file()
    assert stage_path.name == ""
    assert stage_path.suffix == ""
    assert stage_path.stem == ""
    assert stage_path.stage == path.lstrip("@").replace("snow://", "").split("/")[0]
    assert stage_path.absolute_path() == "@" + path.lstrip("@").replace("snow://", "")


@parametrize_with(DIRECTORIES)
def test_dir_paths(path, is_git_repo):
    stage_path = build_stage_path(path, is_git_repo)
    assert not stage_path.is_root()
    assert stage_path.parts == ("my_path",)
    assert stage_path.is_dir()
    assert not stage_path.is_file()
    assert stage_path.name == "my_path"
    assert stage_path.suffix == ""
    assert stage_path.stem == "my_path"
    assert stage_path.stage == path.lstrip("@").replace("snow://", "").split("/")[0]
    assert stage_path.absolute_path() == "@" + path.lstrip("@").replace("snow://", "")


@parametrize_with(FILES)
def test_file_paths(path, is_git_repo):
    stage_path = build_stage_path(path, is_git_repo)
    assert not stage_path.is_root()
    assert stage_path.parts == ("file.py",)
    assert not stage_path.is_dir()
    assert stage_path.is_file()
    assert stage_path.name == "file.py"
    assert stage_path.suffix == ".py"
    assert stage_path.stem == "file"
    assert stage_path.stage == path.lstrip("@").replace("snow://", "").split("/")[0]
    assert stage_path.absolute_path() == "@" + path.lstrip("@").replace(
        "snow://", ""
    ).rstrip("/")


@parametrize_with(FILES_UNDER_PATH)
def test_dir_with_file_paths(path, is_git_repo):
    stage_path = build_stage_path(path, is_git_repo)
    assert not stage_path.is_root()
    assert stage_path.parts == ("my_path", "file.py")
    assert not stage_path.is_dir()
    assert stage_path.is_file()
    assert stage_path.name == "file.py"
    assert stage_path.suffix == ".py"
    assert stage_path.stem == "file"
    assert stage_path.stage == path.lstrip("@").replace("snow://", "").split("/")[0]
    assert stage_path.absolute_path() == "@" + path.lstrip("@").replace(
        "snow://", ""
    ).rstrip("/")


def test_join_path():
    path = StagePath.from_stage_str("@my_stage/path")
    new_path = path.joinpath("new_path").joinpath("file.py")
    assert new_path.parts == ("path", "new_path", "file.py")
    assert path.stage == new_path.stage


def test_join_path_using_division():
    path = StagePath.from_stage_str("@my_stage/path")
    new_path = path / "new_path" / "file.py"
    assert new_path.parts == ("path", "new_path", "file.py")
    assert path.stage == new_path.stage


def test_path_starting_with_slash():
    path = StagePath.from_stage_str("@my_stage")
    new_path = path.joinpath("/file.txt")
    assert new_path.parts == ("file.txt",)
    assert path.stage == new_path.stage
    assert new_path.absolute_path() == "@my_stage/file.txt"


@parametrize_with(FILES_UNDER_PATH)
def test_parent_path(path, is_git_repo):
    path = build_stage_path(path, is_git_repo)
    parent_path = path.parent
    assert parent_path.parts == ("my_path",)
    assert path.stage == parent_path.stage


@pytest.mark.parametrize(
    "stage_name, path",
    [
        ("my_stage", "@my_stage/path/file.py"),
        ("db.schema.my_stage", "@db.schema.my_stage/path/file.py"),
    ],
)
def test_root_path(stage_name, path):
    stage_path = StagePath.from_stage_str(path)
    assert stage_path.root_path() == StagePath.from_stage_str(f"@{stage_name}")


@pytest.mark.parametrize(
    "input_path, path, full_path, schema, stage, stage_name",
    [
        (
            "db.test_schema.test_stage",
            "test_stage",
            "db.test_schema.test_stage",
            "test_schema",
            "db.test_schema.test_stage",
            "test_stage",
        ),
        (
            "db.test_schema.test_stage/subdir",
            "test_stage/subdir",
            "db.test_schema.test_stage/subdir",
            "test_schema",
            "db.test_schema.test_stage",
            "test_stage",
        ),
        (
            "db.test_schema.test_stage/nested/dir",
            "test_stage/nested/dir",
            "db.test_schema.test_stage/nested/dir",
            "test_schema",
            "db.test_schema.test_stage",
            "test_stage",
        ),
        (
            "test_schema.test_stage/nested/dir",
            "test_stage/nested/dir",
            "test_schema.test_stage/nested/dir",
            "test_schema",
            "test_schema.test_stage",
            "test_stage",
        ),
        (
            "test_schema.test_stage/trailing/",
            "test_stage/trailing",
            "test_schema.test_stage/trailing",
            "test_schema",
            "test_schema.test_stage",
            "test_stage",
        ),
        (
            "db.test_schema.test_stage/nested/trailing/",
            "test_stage/nested/trailing",
            "db.test_schema.test_stage/nested/trailing",
            "test_schema",
            "db.test_schema.test_stage",
            "test_stage",
        ),
        (
            "test_stage/nested/trailing/",
            "test_stage/nested/trailing",
            "test_stage/nested/trailing",
            None,
            "test_stage",
            "test_stage",
        ),
        ("test_stage/", "test_stage", "test_stage", None, "test_stage", "test_stage"),
        (
            "test_stage/nested/dir",
            "test_stage/nested/dir",
            "test_stage/nested/dir",
            None,
            "test_stage",
            "test_stage",
        ),
        ("test_stage", "test_stage", "test_stage", None, "test_stage", "test_stage"),
        (
            "test_stage/dir/",
            "test_stage/dir",
            "test_stage/dir",
            None,
            "test_stage",
            "test_stage",
        ),
        (
            "test_schema.test_stage/nested/dir/file.name",
            "test_stage/nested/dir/file.name",
            "test_schema.test_stage/nested/dir/file.name",
            "test_schema",
            "test_schema.test_stage",
            "test_stage",
        ),
        (
            'db.schema."stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            'db.schema."stage.sub/dir"/v1',
            "schema",
            'db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@db.schema."stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            '@db.schema."stage.sub/dir"/v1',
            "schema",
            '@db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@schema."stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            '@schema."stage.sub/dir"/v1',
            "schema",
            '@schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            'schema."stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            'schema."stage.sub/dir"/v1',
            "schema",
            'schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@"stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            '@"stage.sub/dir"/v1',
            None,
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '"stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            '"stage.sub/dir"/v1',
            None,
            '"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            'db.schema."stage.sub/dir"/v1/more/and/more',
            '"stage.sub/dir"/v1/more/and/more',
            'db.schema."stage.sub/dir"/v1/more/and/more',
            "schema",
            'db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@db.schema."stage.sub/dir"/v1/more/and/more/',
            '"stage.sub/dir"/v1/more/and/more',
            '@db.schema."stage.sub/dir"/v1/more/and/more',
            "schema",
            '@db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@schema."stage.sub/dir"/v1/more/and/more/',
            '"stage.sub/dir"/v1/more/and/more',
            '@schema."stage.sub/dir"/v1/more/and/more',
            "schema",
            '@schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            'schema."stage.sub/dir"/v1/more/and/more',
            '"stage.sub/dir"/v1/more/and/more',
            'schema."stage.sub/dir"/v1/more/and/more',
            "schema",
            'schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@"stage.sub/dir"/v1/more/and/more/',
            '"stage.sub/dir"/v1/more/and/more',
            '@"stage.sub/dir"/v1/more/and/more',
            None,
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '"stage.sub/dir"/v1/more/and/more/',
            '"stage.sub/dir"/v1/more/and/more',
            '"stage.sub/dir"/v1/more/and/more',
            None,
            '"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            'db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
            'db.schema."stage.sub/dir"',
            "schema",
            'db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
            '@db.schema."stage.sub/dir"',
            "schema",
            '@db.schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@schema."stage.sub/dir"',
            '"stage.sub/dir"',
            '@schema."stage.sub/dir"',
            "schema",
            '@schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            'schema."stage.sub/dir"',
            '"stage.sub/dir"',
            'schema."stage.sub/dir"',
            "schema",
            'schema."stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
            '@"stage.sub/dir"',
            None,
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
            '@"stage.sub/dir"',
            None,
            '@"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        (
            '"stage.sub/dir"',
            '"stage.sub/dir"',
            '"stage.sub/dir"',
            None,
            '"stage.sub/dir"',
            '"stage.sub/dir"',
        ),
        ("@stage/v1/", "stage/v1", "@stage/v1", None, "@stage", "stage"),
        (
            "@stage/v1/trailing/",
            "stage/v1/trailing",
            "@stage/v1/trailing",
            None,
            "@stage",
            "stage",
        ),
        (
            "@stage/v1/file.name",
            "stage/v1/file.name",
            "@stage/v1/file.name",
            None,
            "@stage",
            "stage",
        ),
        (
            "@stage/file.name",
            "stage/file.name",
            "@stage/file.name",
            None,
            "@stage",
            "stage",
        ),
        (
            "@stage/v1/v2/file.name",
            "stage/v1/v2/file.name",
            "@stage/v1/v2/file.name",
            None,
            "@stage",
            "stage",
        ),
        (
            "@stage/v1/v2/fi?e.name",
            "stage/v1/v2/fi?e.name",
            "@stage/v1/v2/fi?e.name",
            None,
            "@stage",
            "stage",
        ),
        (
            "@stage/v1/v2/fi*e.name",
            "stage/v1/v2/fi*e.name",
            "@stage/v1/v2/fi*e.name",
            None,
            "@stage",
            "stage",
        ),
    ],
)
def test_default_stage_path_parts(
    input_path, path, full_path, schema, stage, stage_name
):
    stage_path_parts = DefaultStagePathParts(input_path)
    assert stage_path_parts.full_path == full_path
    assert stage_path_parts.schema == schema
    assert stage_path_parts.path == path
    assert stage_path_parts.stage == stage
    assert stage_path_parts.stage_name == stage_name


def test_join_system_path():
    stage_path = StagePath.from_stage_str("stage")
    system_path = Path("dir") / "subdir" / "file.txt"
    assert str(stage_path / system_path) == "@stage/dir/subdir/file.txt"
