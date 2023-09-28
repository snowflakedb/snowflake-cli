import json

import pytest

from tests.testing_utils.fixtures import *
from tests.testing_utils.result_assertions import find_conflicts_in_options_dict


def test_global(runner):
    result = runner.invoke(["-h"])

    assert result.exit_code == 0

    assert "SnowCLI - A CLI for Snowflake " in result.output


@pytest.mark.parametrize(
    "namespace, expected",
    [
        ("warehouse", "Manages warehouses."),
        ("snowpark", "Manages functions, procedures, and Snowpark objects."),
        ("streamlit", " Manages Streamlit in Snowflake."),
    ],
)
def test_namespace(namespace, expected, runner):
    result = runner.invoke([namespace, "-h"])

    assert result.exit_code == 0

    assert expected in result.output


@pytest.mark.skip  # skipped until we solve all conflicts
def test_options_structure(runner):
    result = runner.invoke(["--options-structure"])
    assert result.exit_code == 0

    options_json = json.loads(result.output)
    assert find_conflicts_in_options_dict("snow", options_json) is None
