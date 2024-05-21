import pytest


@pytest.mark.integration
@pytest.mark.skip  # TODO: when we have Cortex activated on test account, unskip this and setup part in sql script
def test_cortex_search(runner):
    result = runner.invoke_with_connection_json(
        [
            "cortex",
            "search",
        ]
    )
