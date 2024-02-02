import pytest


@pytest.mark.usefixtures("faker_app")
def test_phase_output(runner):
    result = runner.invoke(("Faker",))
    assert result.exit_code == 0, result.output
    assert "Enter" in result.output, result.output
    assert "Exit" in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_phase_output_muted(runner):
    result = runner.invoke(("Faker", "--silent"))
    assert result.exit_code == 0, result.output
    assert "Enter" not in result.output, result.output
    assert "Exit" not in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_step_output(runner):
    result = runner.invoke(("Faker",))
    assert result.exit_code == 0, result.output
    assert "Teeny Tiny step: UNO UNO" in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_step_output_muted(runner):
    result = runner.invoke(("Faker", "--silent"))
    assert result.exit_code == 0, result.output
    assert "Teeny Tiny step: UNO UNO" not in result.output, result.output
