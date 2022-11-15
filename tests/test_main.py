"""These tests verify that the CLI runs work as expected."""


def test_help_option(runner):
    result = runner.invoke(['--help'])
    assert result.exit_code == 0
