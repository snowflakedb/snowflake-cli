import json
from typing import Dict, Any, Set

from tests.testing_utils.fixtures import *


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


def test_options_structure(runner):
    result = runner.invoke(["--options-structure"])
    assert result.exit_code == 0

    options_json = json.loads(result.output)
    assert find_conflicts_in_options_dict("snow", options_json) is None


def find_conflicts_in_options_dict(path: str, options_dict: Dict[str, Any]):

    keys = list(options_dict.keys())
    duplicates = {}
    if "options" in keys:
        if opts_duplicates := check_options_for_duplicates(options_dict["options"]):
            duplicates[path] = opts_duplicates
        keys.remove("options")

    for key in keys:
        if key_duplicates := find_conflicts_in_options_dict(
            f"{path} {key}", options_dict[key]
        ):
            duplicates.update(key_duplicates)

    if duplicates:
        return duplicates

    return None


def check_options_for_duplicates(options: Dict[str, List[str]]) -> Set[str]:
    RESERVED_FLAGS = ["--help"]  # noqa: N806
    flags = [flag for option in options.values() for flag in option]
    return set(
        [flag for flag in flags if (flags.count(flag) > 1 or flag in RESERVED_FLAGS)]
    )
