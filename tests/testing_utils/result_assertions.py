from click.testing import Result


def _are_the_same_exceptions(e1: Exception, e2: Exception) -> bool:
    return type(e1) is type(e2) and e1.args == e2.args


def assert_that_result_is_usage_error(
    result: Result, expected_error_message: str
) -> None:
    assert result.exit_code == 2
    assert expected_error_message in result.output
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output


def assert_that_result_is_exception_without_debug_mode(
    result: Result, expected_exception_class_name: str, expected_exception_message: str
) -> None:
    assert result.exit_code == 1
    assert "An unexpected exception occurred:" in result.output
    assert (
        f"{expected_exception_class_name}: {expected_exception_message}"
        in result.output
    )
    assert "Use --debug option to see the full traceback." in result.output
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output


def assert_that_result_is_exception_with_debug_mode(
    result: Result, expected_exception: Exception
) -> None:
    assert result.exit_code == 1
    assert _are_the_same_exceptions(result.exception, expected_exception)
