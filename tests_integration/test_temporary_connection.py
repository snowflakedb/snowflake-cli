import pytest


@pytest.mark.integration
def test_temporary_connection(runner):
    result = runner.invoke(
        [
            "object",
            "list",
            "warehouse",
            "--temporary-connection",
            "--account",
            "test_acoount",
            "--user",
            "snowcli_test",
            "--password",
            "top_secret",
            "--warehouse",
            "xsmall",
            "--database",
            "test_dv",
            "--schema",
            "PUBLIC",
        ]
    )

    assert result.exit_code == 1
    assert "HTTP 403: Forbidden" in result.output
