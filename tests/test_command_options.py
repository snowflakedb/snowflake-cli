from tests.testing_utils.fixtures import *


def test_format(runner):
    result = runner.invoke(["stage", "list", "--format", "invalid_format"])

    assert result.output == (
        """Usage: default stage list [OPTIONS] [STAGE_NAME]
Try 'default stage list --help' for help.
╭─ Error ──────────────────────────────────────────────────────────────────────╮
│ Invalid value for '--format': 'invalid_format' is not one of 'TABLE',        │
│ 'JSON'.                                                                      │
╰──────────────────────────────────────────────────────────────────────────────╯
"""
    )
