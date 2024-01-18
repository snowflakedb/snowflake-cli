def test_silent_output_help(runner):
    result = runner.invoke(["streamlit", "get-url", "--help"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    expected_message = "Turns off intermediate output to console"
    assert expected_message in result.output, result.output


def test_proper_context_values_for_silent(runner):
    result = runner.invoke(["streamlit", "get-url", "--silent", "--help"])
    assert runner.app is not None
    assert runner.app

    assert result.exit_code == 0, result.output
    print(dir(runner))
