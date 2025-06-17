import pytest


@pytest.mark.integration
def test_deliberately_flaky8(runner):
    dbname = "snowflake_cli_deliberately_flaky_test_run"

    result = runner.invoke_with_connection(["object", "describe", "database", dbname])
    database_existed = result.exit_code == 0

    if database_existed:
        result = runner.invoke_with_connection(["object", "drop", "database", dbname])
    else:
        result = runner.invoke_with_connection(
            ["object", "create", "database", f"name={dbname}"]
        )
    assert result.exit_code == 0, result.output

    assert database_existed, "This test is deliberately flaky to raise alert"
