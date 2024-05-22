import sys

import pytest


@pytest.mark.integration
@pytest.mark.skip  # TODO: when we have Cortex activated on test account, unskip this and setup part in sql script
@pytest.mark.skipif(
    sys.version_info >= (3.12,),
    reason="Snowflake Python API currently does not support Python 3.12 and greater",
)
def test_cortex_search(runner):
    result = runner.invoke_with_connection_json(
        [
            "cortex",
            "search",
            "parrot",
            "--service",
            "test_service",
            "--limit",
            "1",
            "--columns",
        ]
    )

    assert result.exit_code == 0
    assert "It has ceased to be!" in result.json[0].get("TRANSCRIPT_TEXT")
