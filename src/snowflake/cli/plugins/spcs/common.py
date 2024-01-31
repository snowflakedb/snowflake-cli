from __future__ import annotations

import sys
from typing import TextIO

from click import ClickException

if not sys.stdout.closed and sys.stdout.isatty():
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    ORANGE = "\033[38:2:238:76:44m"
    GRAY = "\033[2m"
    ENDC = "\033[0m"
else:
    GREEN = ""
    ORANGE = ""
    BLUE = ""
    GRAY = ""
    ENDC = ""


def _prefix_line(prefix: str, line: str) -> str:
    """
    _prefix_line ensure the prefix is still present even when dealing with return characters
    """
    if "\r" in line:
        line = line.replace("\r", f"\r{prefix}")
    if "\n" in line[:-1]:
        line = line[:-1].replace("\n", f"\n{prefix}") + line[-1:]
    if not line.startswith("\r"):
        line = f"{prefix}{line}"
    return line


def print_log_lines(file: TextIO, name, identifier, logs):
    prefix = f"{GREEN}{name}/{identifier}{ENDC} "
    logs = logs[0:-1]
    for log in logs:
        print(_prefix_line(prefix, log + "\n"), file=file, end="", flush=True)


def strip_empty_lines(lines: list[str]) -> str:
    return "\n".join(stripped for l in lines if (stripped := l.strip()))


def validate_and_set_instances(min_instances, max_instances, instance_name):
    """
    Used to validate that min_instances is positive and that max_instances is not less than min_instances. In the
    case that max_instances is none, sets it equal to min_instances by default. Used like `max_instances =
    validate_and_set_instances(min_instances, max_instances, "name")`.
    """
    if min_instances < 1:
        raise ClickException(f"min_{instance_name} must be positive")

    if max_instances is None:
        max_instances = min_instances
    elif max_instances < min_instances:
        raise ClickException(
            f"max_{instance_name} must be greater or equal to min_{instance_name}"
        )
    return max_instances
