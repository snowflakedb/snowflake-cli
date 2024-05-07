import glob
import os
import tempfile
from pathlib import Path

import pytest

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

    result = runner.invoke_with_connection_json(["object", "list", "stage"])
    expect = snowflake_session.execute_string(f"show stages like '{stage_name}'")
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

    result = runner.invoke_with_connection_json(["object", "drop", "stage", stage_name])
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
