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

"""Tests for forwarding interface-first ``CommandDocs`` onto Click commands."""

from __future__ import annotations

import typer
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    get_command_docs,
    plain_text,
)
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.plugins.command.bridge import build_command_spec
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    SingleCommandSpec,
)


def test_build_command_spec_forwards_command_docs_on_single_command():
    """Bridge ``docs=`` ends up on the Click callback like ``@app.command(docs=...)``."""
    docs = CommandDocs(usage_notes=(plain_text("Usage."),))
    spec = SingleCommandSpec(
        parent_path=(),
        command=CommandDef(
            name="hello",
            help="Hello.",
            handler_method="hello",
            docs=docs,
        ),
    )

    class Handler(CommandHandler):
        def hello(self) -> CommandResult:
            return MessageResult("hi")

    built = build_command_spec(spec, Handler())
    assert get_command_docs(built.command) is docs


def test_build_command_spec_forwards_command_docs_on_group_child():
    """Multi-command groups keep per-child Click commands (e.g. ``snow bundle``)."""
    documented_docs = CommandDocs(usage_notes=(plain_text("Usage."),))
    spec = CommandGroupSpec(
        name="bundle",
        help="Bundle commands.",
        commands=(
            CommandDef(
                name="create",
                help="Create.",
                handler_method="create",
                docs=documented_docs,
            ),
            CommandDef(name="list", help="List.", handler_method="list_items"),
        ),
    )

    class Handler(CommandHandler):
        def create(self) -> CommandResult:
            return MessageResult("created")

        def list_items(self) -> CommandResult:
            return MessageResult("listed")

    built = build_command_spec(spec, Handler())
    group = typer.main.get_command(built.typer_instance)
    assert get_command_docs(group.commands["create"]) is documented_docs
