def test_toplevel_help(runner):
    result = runner.invoke(["--help"])
    assert (
        result.exit_code == 0
        and "Manages git repositories in Snowflake." in result.output
    )

    result = runner.invoke(["git", "--help"])
    assert result.exit_code == 0, result.output
