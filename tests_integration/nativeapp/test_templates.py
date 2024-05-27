import pytest
from snowflake.connector.compat import IS_WINDOWS


@pytest.mark.integration
@pytest.mark.skipif(IS_WINDOWS, reason="Permissions issue on Windows")
def test_list_templates_no_options_success(runner, snapshot):
    args = ["app", "list-templates"]
    result = runner.invoke(args)

    assert result.exit_code == 0
    assert result.output == snapshot
