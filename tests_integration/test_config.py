import pytest
from snowflake.connector.compat import IS_WINDOWS


@pytest.mark.integration
def test_config_file_permissions_warning(runner, recwarn):
    result = runner.invoke_with_config(["connection", "list"])
    assert result.exit_code == 0, result.output

    is_warning = any(
        "Bad owner or permissions" in str(warning.message) for warning in recwarn
    )
    if IS_WINDOWS:
        assert not is_warning, "Permissions warning found in warnings list (Windows OS)"
    else:
        assert (
            is_warning
        ), "Permissions warning not found in warnings list (OS other than Windows)"
