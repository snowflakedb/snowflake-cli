import uuid

import pytest


@pytest.mark.integration
def test_autocommit_on(runner):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            "show parameters like 'autocommit'",
        ]
    )
    assert result.exit_code == 0, result.output
    assert result.json[0]["key"] == "AUTOCOMMIT"
    assert result.json[0]["default"] == "true"
    assert result.json[0]["value"] == "true"


@pytest.mark.integration
def test_autocommit_on_from_stdin(runner):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "--single-transaction",
            "-i",
        ],
        input="show parameters like 'autocommit'",
    )

    assert result.exit_code == 0, result.output
    assert result.json[1][0]["key"] == "AUTOCOMMIT", result.json
    assert result.json[1][0]["default"] == "true"
    assert result.json[1][0]["value"] == "false"


@pytest.mark.integration
def test_autocommit_on_from_file(runner, tmp_path_factory):
    source_file = tmp_path_factory.mktemp("data") / "source.sql"
    source_file.write_text("show parameters like 'autocommit';")

    result = runner.invoke_with_connection_json(
        [
            "sql",
            "--single-transaction",
            "-f",
            source_file,
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.json[1][0]["key"] == "AUTOCOMMIT", result.json
    assert result.json[1][0]["default"] == "true"
    assert result.json[1][0]["value"] == "false"


@pytest.mark.integration
def test_autocommit_off(runner):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "--single-transaction",
            "-q",
            "show parameters like 'autocommit'",
        ]
    )
    assert result.exit_code == 0, result.output
    assert result.json[1][0]["key"] == "AUTOCOMMIT", result.json
    assert result.json[1][0]["default"] == "true"
    assert result.json[1][0]["value"] == "false"


@pytest.mark.integration
def test_autocommit_off_rollback_on_error(runner):
    tbl_name = f"test_autocommit_{uuid.uuid4().hex}".upper()

    sql = (
        f"create or replace table {tbl_name} (c1 int);"
        f"select count(*) from {tbl_name};"
        f"insert into {tbl_name} values(123);"
        f"insert into {tbl_name} values(124);"
        f"select count(*) from {tbl_name};"
        "select deliberate_error_function();"
    )

    result = runner.invoke_with_connection_json(
        [
            "sql",
            "--enhanced-exit-codes",
            "--single-transaction",
            "-q",
            sql,
        ]
    )
    assert result.exit_code == 5, result.output

    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            f"select count(*) from {tbl_name}",
        ]
    )
    assert result.exit_code == 0, result.output
    assert result.json[0]["COUNT(*)"] == 0, result.json[0]


@pytest.mark.integration
def test_autocommit_off_commit_on_success(runner):
    tbl_name = f"test_autocommit_{uuid.uuid4().hex}".upper()

    sql = (
        f"create or replace table {tbl_name} (c1 int);"
        f"select count(*) from {tbl_name};"
        f"insert into {tbl_name} values(123);"
        f"insert into {tbl_name} values(124);"
        f"select count(*) from {tbl_name};"
    )

    result = runner.invoke_with_connection_json(
        [
            "sql",
            "--enhanced-exit-codes",
            "--single-transaction",
            "-q",
            sql,
        ]
    )
    assert result.exit_code == 0, result.output
    assert (
        result.json[1][0]["status"] == f"Table {tbl_name} successfully created."
    ), result.json[1]
    assert result.json[2][0]["COUNT(*)"] == 0, result.json[2]
    assert result.json[5][0]["COUNT(*)"] == 2, result.json[5]

    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            f"select count(*) from {tbl_name}",
        ]
    )
    assert result.exit_code == 0, result.output
    assert result.json[0]["COUNT(*)"] == 2, result.json[0]
