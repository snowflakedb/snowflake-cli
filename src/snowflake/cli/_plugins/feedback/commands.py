# Copyright (c) 2024 Snowflake Inc.
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

from __future__ import annotations

from enum import Enum
from typing import Optional

import typer
from snowflake.cli._app.printing import is_structured_format, print_result
from snowflake.cli._plugins.feedback.constants import (
    ISSUES_URL,
    PRIVACY_NOTICE,
    STATUS_CONFIRMATION_REQUIRED,
)
from snowflake.cli._plugins.feedback.manager import (
    FeedbackOutcome,
    submit_feedback,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper, SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import CommandResult, MessageResult, ObjectResult
from snowflake.cli.api.utils.tty import is_tty_interactive


class FeedbackCategory(str, Enum):
    BUG = "bug"
    DOCS = "docs"
    UX = "ux"
    FEATURE = "feature"
    OTHER = "other"


class _FeedbackTyperFactory(SnowTyperFactory):
    def create_instance(self) -> SnowTyper:
        hidden = FeatureFlag.ENABLE_SNOW_FEEDBACK.is_disabled()
        for command in self.commands_to_register:
            command.kwargs["hidden"] = hidden
        return super().create_instance()


app = _FeedbackTyperFactory(
    is_hidden=FeatureFlag.ENABLE_SNOW_FEEDBACK.is_disabled,
)


def _requires_explicit_yes() -> bool:
    # JSON/CSV mute the console, so the notice never reaches the user unless
    # we refuse and put it in the structured payload. Same for --silent.
    if cli_console.is_silent or is_structured_format(get_cli_context().output_format):
        return True
    return not is_tty_interactive()


def _emit(outcome: FeedbackOutcome) -> CommandResult:
    payload = outcome.payload()
    if is_structured_format(get_cli_context().output_format):
        result: CommandResult = ObjectResult(payload)
    else:
        result = MessageResult(outcome.message())
    if outcome.exit_code:
        print_result(result)
        raise typer.Exit(outcome.exit_code)
    return result


@app.command(
    name="feedback",
    requires_connection=True,
    docs=CommandDocs(
        related=(
            link(
                ISSUES_URL,
                "Snowflake CLI GitHub issues",
            ),
        ),
        usage_notes=(
            plain_text(
                "Rating is required. 1 and 2 mean the experience was poor or "
                "unsatisfactory; 3 and 4 mean it was satisfactory or excellent. "
                "There is no middle score for 'I do not know'."
            ),
            plain_text(
                "A note is optional for people and required if you are an AI "
                "agent. Describe the command you ran, what you expected, and "
                "what happened. Do not include passwords, tokens, private keys, "
                "account locators, data from Snowflake accounts, or SQL that "
                "contains customer data."
            ),
            plain_text(
                "When stdin is not a terminal, pass --yes to acknowledge the "
                "storage warning."
            ),
            plain_text(
                "Pass --last-error to attach the id and type of the last CLI "
                "error. The error text is not sent."
            ),
            plain_text(
                "This command is in preview and may be removed. It is hidden "
                "from snow --help until ENABLE_SNOW_FEEDBACK is turned on."
            ),
        ),
        examples=(
            Example(
                description=plain_text("Send a rating without a note."),
                command="snow feedback --rating 4 --yes",
            ),
            Example(
                description=plain_text(
                    "Send a rating and a note. Agents should name the command, "
                    "what they expected, and what happened — not a copy of "
                    "customer SQL."
                ),
                command=(
                    "snow feedback --rating 2 --message "
                    '"snow sql compilation error did not point at the typo" --yes'
                ),
            ),
        ),
    ),
)
def feedback(
    rating: int = typer.Option(
        ...,
        "--rating",
        "-r",
        min=1,
        max=4,
        help="Experience rating from 1 (poor) to 4 (excellent).",
        show_default=False,
    ),
    message: Optional[str] = typer.Option(
        None,
        "--message",
        "-m",
        help=(
            "Optional note. Required when running as an AI agent. "
            "Do not include secrets, customer data, or data from Snowflake accounts."
        ),
    ),
    category: FeedbackCategory = typer.Option(
        FeedbackCategory.OTHER,
        "--category",
        help="Feedback category.",
    ),
    last_error: bool = typer.Option(
        False,
        "--last-error",
        help=(
            "Attach the id and type of the last CLI error. "
            "The error text is not sent."
        ),
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Acknowledge the storage warning. Required when stdin is not a TTY.",
    ),
    **options,
) -> CommandResult:
    """Send a rating and optional note about Snowflake CLI to Snowflake.

    This command sends your feedback to Snowflake to improve Snowflake CLI.
    Do not include passwords, access tokens, private keys, account locators,
    data from Snowflake accounts, SQL that contains customer data, or other
    personal or confidential information.
    """
    if not yes:
        if _requires_explicit_yes():
            return _emit(
                FeedbackOutcome(
                    status=STATUS_CONFIRMATION_REQUIRED,
                    exit_code=2,
                )
            )
        cli_console.plain_message(PRIVACY_NOTICE)
        if not typer.confirm("Send this feedback to Snowflake?", default=False):
            raise typer.Abort()

    return _emit(
        submit_feedback(
            rating=rating,
            category=category.value,
            message=message,
            attach_last_error=last_error,
        )
    )
