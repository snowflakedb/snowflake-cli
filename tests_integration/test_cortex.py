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
            "--columns",
            "region",
        ]
    )

    expected_result = [
        {
            "": "This parrot is no more! It has ceased to be! It`s expired and gone to meet its maker!",
            "region": "Flying Circus",
        }
    ]
    assert result.exit_code == 0
    assert result.json == expected_result
