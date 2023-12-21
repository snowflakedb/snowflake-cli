from __future__ import annotations

import warnings

warnings.filterwarnings("ignore", category=UserWarning)


# TODO: add typing to all functions


def generate_deploy_stage_name(identifier: str) -> str:
    return (
        identifier.replace("()", "")
        .replace(
            "(",
            "_",
        )
        .replace(
            ")",
            "",
        )
        .replace(
            " ",
            "_",
        )
        .replace(
            ",",
            "",
        )
    )
