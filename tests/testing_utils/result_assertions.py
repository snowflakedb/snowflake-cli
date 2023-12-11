from click.testing import Result


def assert_that_result_is_usage_error(
    result: Result, expected_error_message: str
) -> None:
    assert result.exit_code == 2, result.exit_code
    assert expected_error_message in result.output, result.output
    assert isinstance(result.exception, SystemExit)
    assert "traceback" not in result.output.lower()
