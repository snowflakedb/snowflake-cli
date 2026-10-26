# Copyright (c) 2026 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""One stderr line after a real CLI error, pointing at ``snow feedback``.

The line is not flag-gated. Structured formats skip it so a parser still
sees a single document. Ctrl-C, usage errors, and ``snow feedback`` itself
do not hint.
"""

from __future__ import annotations

import logging

import click
from snowflake.cli.api.exceptions import (
    BaseCliError,
    error_renderer_already_installed,
    link_error_renderer,
)

log = logging.getLogger(__name__)

FEEDBACK_HINT = "To send feedback about Snowflake CLI: snow feedback"
_ARMED = "_snow_feedback_hint"


def arm_feedback_hint(exception: BaseException) -> None:
    """Remember that this error should hint, while the Click context is live.

    Printing happens later, after Typer draws the error panel.
    """
    if not _should_hint(exception):
        return
    try:
        setattr(exception, _ARMED, True)
    except Exception:
        log.debug("Could not mark exception for feedback hint", exc_info=True)


def print_armed_feedback_hint(exception: BaseException) -> None:
    """Write the hint to stderr. No-op when the error was not marked."""
    if not getattr(exception, _ARMED, False):
        return
    if _structured_output():
        return
    try:
        setattr(exception, _ARMED, False)
    except Exception:
        log.debug("Could not clear feedback hint mark", exc_info=True)
    from snowflake.cli.api.console.console import write_raw_line

    write_raw_line(FEEDBACK_HINT)


def install_feedback_error_hint() -> None:
    """Append the hint after Typer's error panel.

    Install after ``install_plain_remediation_error_renderer`` so a
    permissions error still prints its chmod line, then this hint.
    """
    import typer.rich_utils as rich_utils

    current = rich_utils.rich_format_error
    if error_renderer_already_installed(current, "_snow_feedback_hint_renderer"):
        return

    def _format_error(error: click.ClickException) -> None:
        current(error)
        try:
            print_armed_feedback_hint(error)
        except Exception:
            log.debug("Failed to print feedback hint", exc_info=True)

    link_error_renderer(_format_error, "_snow_feedback_hint_renderer", current)
    rich_utils.rich_format_error = _format_error


def _should_hint(exception: BaseException) -> bool:
    # Exit and Abort subclass Exception. KeyboardInterrupt does not.
    if isinstance(exception, (click.exceptions.Exit, click.exceptions.Abort)):
        return False
    if not isinstance(exception, Exception):
        return False
    command = _command_name()
    if command == "feedback" or command.startswith("feedback "):
        return False
    if isinstance(exception, BaseCliError):
        return True
    if isinstance(exception, click.ClickException):
        return False
    return True


def _command_name() -> str:
    try:
        from snowflake.cli._plugins.feedback.last_error import current_command_name

        return current_command_name()
    except Exception:
        log.debug(
            "Could not read the command name for the feedback hint", exc_info=True
        )
        return ""


def _structured_output() -> bool:
    """True when an extra stderr line could sit next to a parsed document.

    Unknown format fails closed: skip the line.
    """
    try:
        from snowflake.cli._app.printing import is_structured_format
        from snowflake.cli.api.cli_global_context import get_cli_context

        return bool(is_structured_format(get_cli_context().output_format))
    except Exception:
        log.debug("Could not read output format for the feedback hint", exc_info=True)
        return True
