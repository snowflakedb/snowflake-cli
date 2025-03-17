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

import glob
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest
from snowflake.connector import DictCursor

from tests.stage.test_stage import RecursiveUploadTester, NESTED_STRUCTURE
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


@pytest.mark.integration
def test_stage(runner, snowflake_session, test_database, tmp_path):
    stage_name = "test_stage"

    result = runner.invoke_with_connection_json(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    result = runner.invoke_with_connection_json(["stage", "list"])
    expect = snowflake_session.execute_string(f"show stages like '{stage_name}'")
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    result = runner.invoke_with_connection_json(["stage", "describe", stage_name])
    expect = snowflake_session.execute_string(f"describe stage {stage_name}")
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    filename = "test.txt"
    another_filename = "another.md"
    with tempfile.TemporaryDirectory() as td:
        file_path = Path(td) / filename
        another_file_path = Path(td) / another_filename

        for path in [file_path, another_file_path]:
            path.touch()
            result = runner.invoke_with_connection_json(
                ["stage", "copy", str(path), f"@{stage_name}"]
            )
            assert result.exit_code == 0, result.output
            assert contains_row_with(
                result.json,
                {"source": path.name, "target": path.name, "status": "UPLOADED"},
            )

    result = runner.invoke_with_connection_json(["stage", "list-files", stage_name])
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert result.json == row_from_snowflake_session(expect)

    result = runner.invoke_with_connection_json(
        ["stage", "list-files", stage_name, "--pattern", ".*md"]
    )
    assert contains_row_with(result.json, {"name": f"{stage_name}/{another_filename}"})
    assert not_contains_row_with(result.json, {"name": f"{stage_name}/{filename}"})

    # Operation fails because directory exists
    result = runner.invoke_with_connection_json(
        ["stage", "copy", f"@{stage_name}", tmp_path.parent.__str__()]
    )
    assert result.exit_code == 0, result.output
    assert contains_row_with(result.json, {"file": filename, "status": "DOWNLOADED"})
    assert os.path.isfile(tmp_path.parent / filename)

    result = runner.invoke_with_connection_json(
        ["stage", "remove", stage_name, f"/{filename}"]
    )
    assert contains_row_with(
        result.json,
        {"name": f"{stage_name}/{filename}", "result": "removed"},
    )
    expect = snowflake_session.execute_string(f"list @{stage_name}")
    assert not_contains_row_with(
        row_from_snowflake_session(expect), {"name": f"{stage_name}/{filename}"}
    )

    result = runner.invoke_with_connection_json(["stage", "drop", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"{stage_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(f"show stages like '%{stage_name}%'")
    assert row_from_snowflake_session(expect) == []


@pytest.mark.integration
def test_stage_get_recursive(
    runner,
    snowflake_session,
    test_database,
    test_root_path,
    temporary_working_directory,
):
    project_path = test_root_path / "test_data/projects/stage_get_directory_structure"
    stage_name = "stage_directory_structure"

    result = runner.invoke_with_connection_json(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    file_paths = glob.glob(f"{project_path}/**/*.sql", recursive=True)
    project_path_parts_length = len(project_path.parts)
    for path in file_paths:
        dest_path = "/".join(Path(path).parts[project_path_parts_length:-1])
        result = runner.invoke_with_connection_json(
            ["stage", "copy", path, f"@{stage_name}/{dest_path}"]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(result.json, {"status": "UPLOADED"})

    runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            f"@{stage_name}",
            str(temporary_working_directory),
            "--recursive",
        ]
    )

    downloaded_file_paths = glob.glob("**/*.sql", recursive=True)
    assert downloaded_file_paths == [
        os.path.join(*Path(f).parts[project_path_parts_length:]) for f in file_paths
    ]


@pytest.mark.integration
def test_user_stage_get_recursive(
    runner,
    snowflake_session,
    test_database,
    test_root_path,
    temporary_working_directory,
):
    project_path = test_root_path / "test_data/projects/stage_get_directory_structure"
    user_stage_name = "@~"

    file_paths = glob.glob(f"{project_path}/**/*.sql", recursive=True)
    project_path_parts_length = len(project_path.parts)
    for path in file_paths:
        dest_path = "/".join(Path(path).parts[project_path_parts_length:-1])
        result = runner.invoke_with_connection_json(
            ["stage", "copy", path, f"{user_stage_name}/copy/{dest_path}"]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(
            result.json, {"status": "SKIPPED"}
        ) or contains_row_with(result.json, {"status": "UPLOADED"})

    runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            f"{user_stage_name}/copy",
            str(temporary_working_directory),
            "--recursive",
        ]
    )

    downloaded_file_paths = glob.glob("**/*.sql", recursive=True)
    assert downloaded_file_paths == [
        os.path.join(*Path(f).parts[project_path_parts_length:]) for f in file_paths
    ]


@pytest.mark.integration
def test_stage_execute(runner, test_database, test_root_path, snapshot):
    project_path = test_root_path / "test_data/projects/stage_execute"
    stage_name = "test_stage_execute"

    result = runner.invoke_with_connection_json(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    files = [
        ("script1.sql", ""),
        ("script2.sql", "directory"),
        ("script3.sql", "directory/subdirectory"),
    ]
    for name, stage_path in files:
        result = runner.invoke_with_connection_json(
            [
                "stage",
                "copy",
                f"{project_path}/{name}",
                f"@{stage_name}/{stage_path}",
            ]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(result.json, {"status": "UPLOADED"})

    result = runner.invoke_with_connection_json(["stage", "execute", stage_name])
    assert result.exit_code == 0
    assert result.json == snapshot

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            f"{project_path}/script_template.sql",
            f"@{stage_name}/",
        ]
    )
    assert result.exit_code == 0, result.output
    assert contains_row_with(result.json, {"status": "UPLOADED"})

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "execute",
            f"{stage_name}/script_template.sql",
            "-D",
            " text = 'string' ",
            "-D",
            "value=1",
            "-D",
            "boolean=TRUE",
            "-D",
            "null_value= NULL",
        ]
    )
    assert result.exit_code == 0
    assert result.json == snapshot

    result_fqn = runner.invoke_with_connection_json(
        [
            "stage",
            "execute",
            f"@{test_database}.public.{stage_name}/script_template.sql",
            "-D",
            " text = 'string' ",
            "-D",
            "value=1",
            "-D",
            "boolean=TRUE",
            "-D",
            "null_value= NULL",
        ]
    )
    assert result_fqn.exit_code == 0
    assert result_fqn.json == [
        {
            "File": f"@{test_database}.public.{stage_name}/script_template.sql",
            "Status": "SUCCESS",
            "Error": None,
        }
    ]


@pytest.mark.integration
def test_user_stage_execute(runner, test_database, test_root_path, snapshot):
    project_path = test_root_path / "test_data/projects/stage_execute"
    user_stage_name = "@~"

    files = [
        ("script1.sql", "execute/sql"),
        ("script2.sql", "execute/sql/directory"),
        ("script3.sql", "execute/sql/directory/subdirectory"),
    ]
    for name, stage_path in files:
        result = runner.invoke_with_connection_json(
            [
                "stage",
                "copy",
                f"{project_path}/{name}",
                f"{user_stage_name}/{stage_path}",
            ]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(
            result.json, {"status": "SKIPPED"}
        ) or contains_row_with(result.json, {"status": "UPLOADED"})

    result = runner.invoke_with_connection_json(
        ["stage", "execute", f"{user_stage_name}/execute/sql"]
    )
    assert result.exit_code == 0
    assert result.json == snapshot

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            f"{project_path}/script_template.sql",
            f"{user_stage_name}/execute/template",
        ]
    )
    assert result.exit_code == 0, result.output
    assert contains_row_with(result.json, {"status": "SKIPPED"}) or contains_row_with(
        result.json, {"status": "UPLOADED"}
    )

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "execute",
            f"{user_stage_name}/execute/template/script_template.sql",
            "-D",
            " text = 'string' ",
            "-D",
            "value=1",
            "-D",
            "boolean=TRUE",
            "-D",
            "null_value= NULL",
        ]
    )
    assert result.exit_code == 0
    assert result.json == snapshot


@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="Snowpark is not supported in Python >= 3.12"
)
def test_stage_execute_python(
    snowflake_session, runner, test_database, test_root_path, snapshot
):
    project_path = test_root_path / "test_data/projects/stage_execute"
    stage_name = "test_stage_execute"

    result = runner.invoke_with_connection_json(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    files = ["script1.py", "script_template.py", "requirements.txt"]
    for name in files:
        result = runner.invoke_with_connection_json(
            [
                "stage",
                "copy",
                str(Path(project_path) / name),
                f"@{stage_name}",
            ]
        )
        assert result.exit_code == 0, result.output
        assert contains_row_with(result.json, {"status": "UPLOADED"})

    test_id = f"FOO{time.time_ns()}"
    result = runner.invoke_with_connection_json(
        [
            "stage",
            "execute",
            f"{stage_name}/",
            "-D",
            f"test_database_name={test_database}",
            "-D",
            f"TEST_ID={test_id}",
        ]
    )
    assert result.exit_code == 0
    assert result.json == snapshot

    # Assert side effect created by executed script
    *_, schemas = snowflake_session.execute_string(
        f"show schemas like '{test_id}' in database {test_database};"
    )
    assert len(list(schemas)) == 1


@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="Snowpark is not supported in Python >= 3.12"
)
def test_stage_execute_python_without_requirements(
    snowflake_session, runner, test_database, test_root_path, snapshot
):
    project_path = (
        test_root_path / "test_data/projects/stage_execute_without_requirements"
    )
    stage_name = "test_stage_execute_without_requirements"

    result = runner.invoke_with_connection_json(["stage", "create", stage_name])
    assert contains_row_with(
        result.json,
        {"status": f"Stage area {stage_name.upper()} successfully created."},
    )

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            str(Path(project_path) / "script_template.py"),
            f"@{stage_name}",
        ]
    )
    assert result.exit_code == 0, result.output
    assert contains_row_with(result.json, {"status": "UPLOADED"})

    test_id = f"FOO{time.time_ns()}"
    result = runner.invoke_with_connection_json(
        [
            "stage",
            "execute",
            f"{stage_name}/",
            "-D",
            f"test_database_name={test_database}",
            "-D",
            f"TEST_ID={test_id}",
        ]
    )
    assert result.exit_code == 0
    assert result.json == snapshot

    # Assert side effect created by executed script
    *_, schemas = snowflake_session.execute_string(
        f"show schemas like '{test_id}' in database {test_database};"
    )
    assert len(list(schemas)) == 1


@pytest.mark.integration
def test_stage_diff(runner, snowflake_session, test_database, tmp_path, snapshot):
    stage_name = "test_stage"

    # Only use server-side encryption otherwise md5sum-based diffs don't work correctly
    result = runner.invoke_with_connection(
        [
            "sql",
            "--query",
            f"""
                                            create stage if not exists {stage_name}
                                            encryption = (TYPE = 'SNOWFLAKE_SSE')
                                            DIRECTORY = (ENABLE = TRUE)""",
        ]
    )
    assert result.exit_code == 0, result.output

    filename = "test.txt"
    another_filename = "another.md"
    with tempfile.TemporaryDirectory() as td:
        local_dir = Path(td).resolve()
        result = runner.invoke_with_connection(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.output == snapshot

        # avoid nesting the files into directories because the snapshots would then become system-dependent
        file_path = Path(td) / filename
        another_file_path = Path(td) / another_filename

        for path in [file_path, another_file_path]:
            path.touch()
            with path.open("w") as f:
                f.write(f"Initial contents for {path}\n")

            result = runner.invoke_with_connection(
                ["stage", "diff", stage_name, str(local_dir)]
            )
            assert result.exit_code == 0, result.output
            assert result.output == snapshot

            result = runner.invoke_with_connection_json(
                ["stage", "copy", str(path), f"@{stage_name}"]
            )
            assert result.exit_code == 0, result.output

            result = runner.invoke_with_connection(
                ["stage", "diff", stage_name, str(local_dir)]
            )
            assert result.exit_code == 0, result.output
            assert result.output == snapshot

        with open(file_path, "w") as f:
            f.write("New contents\n")
        result = runner.invoke_with_connection(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.output == snapshot

        result = runner.invoke_with_connection_json(
            ["stage", "copy", str(file_path), f"@{stage_name}"]
        )
        assert result.exit_code == 0, result.output

        with open(another_file_path, "w") as f:
            f.write("Newer contents\n")
        with open(local_dir / "added_file.py", "w") as f:
            f.write("# python source\n")
        file_path.unlink()
        result = runner.invoke_with_connection(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.output == snapshot


@pytest.mark.integration
def test_stage_diff_json(runner, snowflake_session, test_database, tmp_path):
    stage_name = "test_stage"

    # Only use server-side encryption otherwise md5sum-based diffs don't work correctly
    result = runner.invoke_with_connection(
        [
            "sql",
            "--query",
            f"""
                                            create stage if not exists {stage_name}
                                            encryption = (TYPE = 'SNOWFLAKE_SSE')
                                            DIRECTORY = (ENABLE = TRUE)""",
        ]
    )
    assert result.exit_code == 0, result.output

    filename = "test.txt"
    another_filename = "another.md"
    with tempfile.TemporaryDirectory() as td:
        local_dir = Path(td).resolve()
        result = runner.invoke_with_connection_json(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.json == {"modified": [], "added": [], "deleted": []}

        file_path = Path(td) / filename
        another_file_path = Path(td) / another_filename

        for path in [file_path, another_file_path]:
            path.touch()
            with path.open("w") as f:
                f.write(f"Initial contents for {path}\n")

            result = runner.invoke_with_connection_json(
                ["stage", "diff", stage_name, str(local_dir)]
            )
            assert result.exit_code == 0, result.output
            assert result.json == {"modified": [], "added": [path.name], "deleted": []}

            result = runner.invoke_with_connection_json(
                ["stage", "copy", str(path), f"@{stage_name}"]
            )
            assert result.exit_code == 0, result.output

            result = runner.invoke_with_connection_json(
                ["stage", "diff", stage_name, str(local_dir)]
            )
            assert result.exit_code == 0, result.output
            assert result.json == {"modified": [], "added": [], "deleted": []}

        with open(file_path, "w") as f:
            f.write("New contents\n")
        result = runner.invoke_with_connection_json(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.json == {"modified": [filename], "added": [], "deleted": []}

        result = runner.invoke_with_connection_json(
            ["stage", "copy", str(file_path), f"@{stage_name}"]
        )
        assert result.exit_code == 0, result.output

        with open(another_file_path, "w") as f:
            f.write("Newer contents\n")
        with open(local_dir / "added_file.py", "w") as f:
            f.write("# python source\n")
        file_path.unlink()
        result = runner.invoke_with_connection_json(
            ["stage", "diff", stage_name, str(local_dir)]
        )
        assert result.exit_code == 0, result.output
        assert result.json == {
            "modified": [another_filename],
            "added": ["added_file.py"],
            "deleted": [filename],
        }


@pytest.mark.integration
def test_stage_list(runner, test_database):
    stage_name = "test_stage"
    result = runner.invoke_with_connection(["stage", "create", stage_name])
    assert result.exit_code == 0, result.output

    with tempfile.TemporaryDirectory() as td:
        file = Path(td) / "test.txt"
        file.touch()
        result = runner.invoke_with_connection_json(
            ["stage", "copy", str(file), f"@{stage_name}/under/directory/"]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["stage", "list-files", f"@{stage_name}/under/"]
        )
        assert result.exit_code == 0, result.output
        assert result.json[0]["name"] == f"{stage_name}/under/directory/test.txt"


@pytest.mark.integration
@pytest.mark.parametrize("pattern", ["", "**/*", "**"])
def test_recursive_upload(temporary_directory, pattern, runner, test_database):
    stage_name = "@recursive_upload"
    runner.invoke_with_connection_json(["stage", "create", stage_name])

    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)

    result = runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            temporary_directory + "/" + pattern,
            stage_name,
            "--recursive",
        ]
    )

    assert len(result.json) == 9
    assert result.json == [
        {
            "message": "",
            "source": "dir2/dir21/dir211/dir2111/file21111.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir2/dir21/dir211/dir2111/file21111.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/dir12/file121.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/dir12/file121.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/dir12/file122.md",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/dir12/file122.md",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir3/dir32/file321",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir3/dir32/file321",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/file1.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/file1.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/file1.txt",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/file1.txt",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir3/file31",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir3/file31",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir2/file21",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir2/file21",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "file4.foo",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/file4.foo",
            "target_compression": "NONE",
            "target_size": 16,
        },
    ]


@pytest.mark.integration
def test_recursive_upload_with_empty_dir(temporary_directory):
    structure = {}

    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=structure)
    _ = tester.execute(local_path=temporary_directory)

    assert tester.calls == []


@pytest.mark.integration
def test_recursive_upload_glob_file_pattern(temporary_directory, runner, test_database):
    stage_name = "@recursive_upload"
    runner.invoke_with_connection_json(["stage", "create", stage_name])

    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)

    result = runner.invoke_with_connection_json(
        ["stage", "copy", f"{temporary_directory}/**/*.py", stage_name, "--recursive"]
    )

    assert result.exit_code == 0, result.output

    assert len(result.json) == 3
    assert result.json == [
        {
            "message": "",
            "source": "dir2/dir21/dir211/dir2111/file21111.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir2/dir21/dir211/dir2111/file21111.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/dir12/file121.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/dir12/file121.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
        {
            "message": "",
            "source": "dir1/file1.py",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": "@recursive_upload/dir1/file1.py",
            "target_compression": "NONE",
            "target_size": 16,
        },
    ]


@pytest.mark.integration
def test_recursive_upload_no_recursive_glob_pattern(
    temporary_directory, runner, test_database
):
    stage_name = "@recursive_upload"
    runner.invoke_with_connection_json(["stage", "create", stage_name])

    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)

    result = runner.invoke_with_connection_json(
        ["stage", "copy", f"{temporary_directory}/*.foo", stage_name, "--recursive"]
    )

    assert result.exit_code == 0, result.output

    assert len(result.json) == 1
    assert result.json == [
        {
            "message": "",
            "source": "file4.foo",
            "source_compression": "NONE",
            "source_size": 8,
            "status": "UPLOADED",
            "target": f"{stage_name}/file4.foo",
            "target_compression": "NONE",
            "target_size": 16,
        }
    ]
