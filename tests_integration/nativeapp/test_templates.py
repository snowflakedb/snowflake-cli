import pytest


@pytest.mark.integration
def test_list_templates_no_options_success(runner, snapshot):
    args = ["app", "list-templates"]
    result = runner.invoke(args)

    assert result.exit_code == 0
    assert result.output == snapshot
