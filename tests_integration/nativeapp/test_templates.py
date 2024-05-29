import pytest


@pytest.mark.integration
def test_list_templates_no_options_success(runner):
    args = ["app", "list-templates"]
    result = runner.invoke_json(args)

    assert result.exit_code == 0
    templates = result.json
    assert len(templates) > 0

    # Check that the basic templates are present, but explicitly avoid checking for an
    # exact list so that adding new templates won't break the tests.
    all_template_names = [t["template"] for t in templates]
    assert "basic" in all_template_names
    assert "streamlit-java" in all_template_names
    assert "streamlit-python" in all_template_names
