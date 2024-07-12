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

import sys
from pathlib import Path
from typing import List, Optional

import click
import typer
from click import UsageError
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.flags import readable_file_option
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import PYTHON_3_12
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.cortex.constants import DEFAULT_MODEL
from snowflake.cli.plugins.cortex.manager import CortexManager
from snowflake.cli.plugins.cortex.types import (
    Language,
    Model,
    Question,
    SourceDocument,
    Text,
)

app = SnowTyperFactory(
    name="cortex",
    help="Provides access to Snowflake Cortex.",
)

SEARCH_COMMAND_ENABLED = sys.version_info < PYTHON_3_12


@app.command(
    requires_connection=True,
    hidden=not SEARCH_COMMAND_ENABLED,
)
def search(
    query: str = typer.Argument(help="The search query string"),
    service: str = typer.Option(
        help="Cortex search service to be used. Example: --service my_cortex_service",
    ),
    columns: Optional[List[str]] = typer.Option(
        help='Columns that will be returned with the results. If none is provided, only search column will be included in results. Example --columns "foo" --columns "bar"',
        default=None,
    ),
    limit: int = typer.Option(help="Maximum number of results retrieved", default=1),
    **options,
):
    """
    Performs query search using Cortex Search Services.
    """

    if not SEARCH_COMMAND_ENABLED:
        raise click.ClickException(
            "Cortex Search uses Snowflake Python API that currently does not support your Python version"
        )

    from snowflake.core import Root

    if not columns:
        columns = []

    conn = cli_context.connection

    search_service = (
        Root(conn)
        .databases[conn.database]
        .schemas[conn.schema]
        .cortex_search_services[service]
    )

    response = search_service.search(
        query=query, columns=columns, limit=limit, filter={}
    )

    return CollectionResult(response.results)


@app.command(
    name="complete",
    requires_connection=True,
)
def complete(
    text: Optional[str] = typer.Argument(
        None,
        help="Prompt to be used to generate a completion. Cannot be combined with --file option.",
        show_default=False,
    ),
    model: Optional[str] = typer.Option(
        DEFAULT_MODEL,
        "--model",
        help="String specifying the model to be used.",
    ),
    file: Optional[Path] = readable_file_option(
        param_name="--file",
        help_str="JSON file containing conversation history to be used to generate a completion. Cannot be combined with TEXT argument.",
    ),
    **options,
) -> CommandResult:
    """
    Given a prompt, the command generates a response using your choice of language model.
    In the simplest use case, the prompt is a single string.
    You may also provide a JSON file with conversation history including multiple prompts and responses for interactive chat-style usage.
    """

    manager = CortexManager()

    if text and file:
        raise UsageError("--file option cannot be used together with TEXT argument.")
    if text:
        result_text = manager.complete_for_prompt(
            text=Text(text),
            model=Model(model),
        )
    elif file:
        result_text = manager.complete_for_conversation(
            conversation_json_file=SecurePath(file),
            model=Model(model),
        )
    else:
        raise UsageError("Either --file option or TEXT argument has to be provided.")

    return MessageResult(result_text.strip())


@app.command(
    name="extract-answer",
    requires_connection=True,
)
def extract_answer(
    question: str = typer.Argument(
        None,
        help="String containing the question to be answered.",
        show_default=False,
    ),
    source_document_text: Optional[str] = typer.Argument(
        None,
        help="String containing the plain-text or JSON document that contains the answer to the question. Cannot be combined with --file option.",
        show_default=False,
    ),
    file: Optional[Path] = readable_file_option(
        param_name="--file",
        help_str="File containing the plain-text or JSON document that contains the answer to the question. Cannot be combined with SOURCE_DOCUMENT_TEXT argument.",
    ),
    **options,
) -> CommandResult:
    """
    Extracts an answer to a given question from a text document.
    The document may be a plain-English document or a string representation of a semi-structured (JSON) data object.
    """

    manager = CortexManager()

    if source_document_text and file:
        raise UsageError(
            "--file option cannot be used together with SOURCE_DOCUMENT_TEXT argument."
        )
    if source_document_text:
        result_text = manager.extract_answer_from_source_document(
            source_document=SourceDocument(source_document_text),
            question=Question(question),
        )
    elif file:
        result_text = manager.extract_answer_from_source_document_file(
            source_document_input_file=SecurePath(file),
            question=Question(question),
        )
    else:
        raise UsageError(
            "Either --file option or SOURCE_DOCUMENT_TEXT argument has to be provided."
        )

    return MessageResult(result_text.strip())


@app.command(
    name="sentiment",
    requires_connection=True,
)
def sentiment(
    text: Optional[str] = typer.Argument(
        None,
        help="String containing the text for which a sentiment score should be calculated. Cannot be combined with --file option.",
        show_default=False,
    ),
    file: Optional[Path] = readable_file_option(
        param_name="--file",
        help_str="File containing the text for which a sentiment score should be calculated. Cannot be combined with TEXT argument.",
    ),
    **options,
) -> CommandResult:
    """
    Returns sentiment as a score between -1 to 1
    (with -1 being the most negative and 1 the most positive,
    with values around 0 neutral) for the given English-language input text.
    """

    manager = CortexManager()

    if text and file:
        raise UsageError("--file option cannot be used together with TEXT argument.")
    if text:
        result_text = manager.calculate_sentiment_for_text(
            text=Text(text),
        )
    elif file:
        result_text = manager.calculate_sentiment_for_text_file(
            text_file=SecurePath(file),
        )
    else:
        raise UsageError("Either --file option or TEXT argument has to be provided.")

    return MessageResult(result_text.strip())


@app.command(
    name="summarize",
    requires_connection=True,
)
def summarize(
    text: Optional[str] = typer.Argument(
        None,
        help="String containing the English text from which a summary should be generated. Cannot be combined with --file option.",
        show_default=False,
    ),
    file: Optional[Path] = readable_file_option(
        param_name="--file",
        help_str="File containing the English text from which a summary should be generated. Cannot be combined with TEXT argument.",
    ),
    **options,
) -> CommandResult:
    """
    Summarizes the given English-language input text.
    """

    manager = CortexManager()

    if text and file:
        raise UsageError("--file option cannot be used together with TEXT argument.")
    if text:
        result_text = manager.summarize_text(
            text=Text(text),
        )
    elif file:
        result_text = manager.summarize_text_file(
            text_file=SecurePath(file),
        )
    else:
        raise UsageError("Either --file option or TEXT argument has to be provided.")

    return MessageResult(result_text.strip())


@app.command(
    name="translate",
    requires_connection=True,
)
def translate(
    text: Optional[str] = typer.Argument(
        None,
        help="String containing the text to be translated. Cannot be combined with --file option.",
        show_default=False,
    ),
    from_language: Optional[str] = typer.Option(
        None,
        "--from",
        help="String specifying the language code for the language the text is currently in. See Snowflake Cortex documentation for a list of supported language codes.",
        show_default=False,
    ),
    to_language: str = typer.Option(
        ...,
        "--to",
        help="String specifying the language code into which the text should be translated. See Snowflake Cortex documentation for a list of supported language codes.",
        show_default=False,
    ),
    file: Optional[Path] = readable_file_option(
        param_name="--file",
        help_str="File containing the text to be translated. Cannot be combined with TEXT argument.",
    ),
    **options,
) -> CommandResult:
    """
    Translates text from the indicated or detected source language to a target language.
    """

    manager = CortexManager()

    source_language = None if from_language is None else Language(from_language)
    target_language = Language(to_language)

    if text and file:
        raise UsageError("--file option cannot be used together with TEXT argument.")
    if text:
        result_text = manager.translate_text(
            text=Text(text),
            source_language=source_language,
            target_language=target_language,
        )
    elif file:
        result_text = manager.translate_text_file(
            text_file=SecurePath(file),
            source_language=source_language,
            target_language=target_language,
        )
    else:
        raise UsageError("Either --file option or TEXT argument has to be provided.")

    return MessageResult(result_text.strip())
