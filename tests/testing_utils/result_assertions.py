from typing import Dict, Any, List, Optional

from click.testing import Result


def assert_that_result_is_usage_error(
    result: Result, expected_error_message: str
) -> None:
    assert result.exit_code == 2
    assert expected_error_message in result.output
    assert isinstance(result.exception, SystemExit)
    assert "traceback" not in result.output.lower()


def find_conflicts_in_options_dict(path: str, options_dict: Dict[str, Any]):

    keys = list(options_dict.keys())
    duplicates = {}
    if "options" in keys:
        if opts_duplicates := check_options_for_duplicates(options_dict["options"]):
            duplicates[path] = opts_duplicates
        keys.remove("options")

    for key in keys:
        if key_duplicates := find_conflicts_in_options_dict(key, options_dict[key]):
            duplicates[key] = key_duplicates

    if duplicates:
        return duplicates

    return None


def check_options_for_duplicates(options: Dict[str, List[str]]) -> List[str]:
    RESERVED_FLAGS = ["-h", "--help"]  # noqa: N806
    flags = [flag for option in options.values() for flag in option]
    return list(
        set(
            [
                flag
                for flag in flags
                if (flags.count(flag) > 1 or flag in RESERVED_FLAGS)
            ]
        )
    )
