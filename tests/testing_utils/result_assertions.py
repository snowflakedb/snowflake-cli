from click.testing import Result


def assert_successful_result_message(result: Result, expected_msg: str):
    assert result.exit_code == 0, result.output
    assert result.output == expected_msg + "\n"


def assert_that_result_is_usage_error(
    result: Result, expected_error_message: str
) -> None:
    assert result.exit_code == 2, result.output
    assert expected_error_message in result.output, result.output
    assert isinstance(result.exception, SystemExit)
    assert "traceback" not in result.output.lower()
