import pytest


@pytest.mark.parametrize("command", ["copy", "create", "diff", "list", "remove"])
def test_object_stage_commands_cause_a_warning(command, runner):
    result = runner.invoke(["object", "stage", command, "--help"])
    assert result.exit_code == 0, result.output
    assert "(deprecated)" in result.output


def test_object_stage_main_command_causes_a_warning(runner):
    result = runner.invoke(["object", "stage", "--help"])
    assert result.exit_code == 0, result.output
    assert "(deprecated)" in result.output
