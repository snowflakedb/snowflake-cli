COMMAND = "check-snowsql-env-vars"


def test_no_evn_present(runner):
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert "Found 0 SnowSQL environment variables." in result.output
