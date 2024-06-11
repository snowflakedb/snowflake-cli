import sys

import pytest

if sys.version_info >= (3, 12):
    pytest.skip(
        """"Snowflake Python API currently does not support Python 3.12 and greater, 
    so cortex search is only visible when using lower version of Python""",
        allow_module_level=True,
    )


def test_cortex_help_messages_for_312(runner, snapshot):
    result = runner.invoke(["cortex", "--help"])
    assert result.exit_code == 0
    assert result.output == snapshot


def test_cortex_help_messages_for_312_no_help_flag(runner, snapshot):
    result = runner.invoke(["cortex"])
    assert result.exit_code == 0
    assert result.output == snapshot
