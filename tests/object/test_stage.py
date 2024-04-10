import pytest


@pytest.mark.parametrize("command", ["copy", "create", "diff", "list", "remove"])
def test_object_stage_commands_cause_a_warning(command, runner):
    result = runner.invoke(["object", "stage", command, "--help"])
    assert result.exit_code == 0, result.output
    assert (
        "`snow object stage` command group is deprecated. Please use `snow stage` instead."
        in result.output
    )
    assert "This command is deprecated. Please use" in result.output


def test_object_stage_main_command_causes_a_warning(runner):
    result = runner.invoke(["object", "stage", "--help"])
    assert result.exit_code == 0, result.output
    assert (
        "`snow object stage` command group is deprecated. Please use `snow stage`"
        in result.output
    )
    assert "This command is deprecated. Please use" in result.output
