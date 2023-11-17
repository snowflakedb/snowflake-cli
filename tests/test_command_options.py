from tests.testing_utils.fixtures import *


def test_format(runner):
    result = runner.invoke(
        ["object", "stage", "list", "stage_name", "--format", "invalid_format"]
    )

    assert result.output == (
        """Usage: default object stage list [OPTIONS] STAGE_NAME
Try 'default object stage list --help' for help.
╭─ Error ──────────────────────────────────────────────────────────────────────╮
│ Invalid value for '--format': 'invalid_format' is not one of 'TABLE',        │
│ 'JSON'.                                                                      │
╰──────────────────────────────────────────────────────────────────────────────╯
"""
    )
