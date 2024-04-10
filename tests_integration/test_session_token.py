import pytest


@pytest.mark.integration
def test_use_session_token(runner, snowflake_session):
    session_token = snowflake_session.rest.token
    master_token = snowflake_session.rest.master_token

    result_of_setting_variable = runner.invoke(
        [
            "sql",
            "-q",
            "set a = 42",
            "-x",
            "--account",
            snowflake_session.account,
            "--session-token",
            session_token,
            "--master-token",
            master_token,
        ]
    )
    assert result_of_setting_variable.exit_code == 0
    result_of_getting_variable = runner.invoke_json(
        [
            "sql",
            "-q",
            "select $a as dummy",
            "-x",
            "--account",
            snowflake_session.account,
            "--session-token",
            session_token,
            "--master-token",
            master_token,
            "--format",
            "json",
        ]
    )
    assert result_of_getting_variable.exit_code == 0
    assert result_of_getting_variable.json == [{"DUMMY": 42}]
