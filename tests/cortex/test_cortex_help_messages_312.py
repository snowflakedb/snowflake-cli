import sys

import pytest

if sys.version_info < (3, 12):
    pytest.skip(
        """As Snowflake Python API does not support Python version 3.12 and greater, 
    Cortex Search command should be hidden, resulting in different help messages""",
        allow_module_level=True,
    )


def test_cortex_help_messages_for_311_and_less(runner, snapshot):
    result = runner.invoke(["cortex", "--help"])
    assert result.exit_code == 0
    assert result.output == snapshot


def test_cortex_help_messages_for_311_and_less_no_help_flag(runner, snapshot):
    result = runner.invoke(["cortex"])
    assert result.exit_code == 0
    assert result.output == snapshot
