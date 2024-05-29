import pytest


@pytest.mark.integration
def test_list_templates_no_options_success(runner, snapshot):
    args = ["app", "list-templates"]
    result = runner.invoke_json(args)

    assert result.exit_code == 0
    templates = result.json
    assert len(templates) > 0
    assert "basic" in [t["template"] for t in templates]
