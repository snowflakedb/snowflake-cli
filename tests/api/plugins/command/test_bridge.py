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

"""Tests for the interface-to-CommandSpec bridge."""

from __future__ import annotations

import pytest

from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandType,
)
from snowflake.cli.api.plugins.command.bridge import (
    InterfaceValidationError,
    _collect_commands,
    build_command_spec,
    validate_interface_handler,
)
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
    REQUIRED,
    SingleCommandSpec,
)


# ---------------------------------------------------------------------------
# Fixtures: minimal spec + handler for testing
# ---------------------------------------------------------------------------


def _simple_spec():
    return CommandGroupSpec(
        name="test-plugin",
        help="A test plugin.",
        parent_path=(),
        commands=(
            CommandDef(
                name="greet",
                help="Say hello.",
                handler_method="greet",
                params=(
                    ParamDef(
                        name="name",
                        type=str,
                        kind=ParamKind.ARGUMENT,
                        help="Name to greet",
                    ),
                ),
            ),
            CommandDef(
                name="count",
                help="Count things.",
                handler_method="count",
                params=(
                    ParamDef(
                        name="n",
                        type=int,
                        kind=ParamKind.OPTION,
                        cli_names=("--number", "-n"),
                        help="How many",
                        default=5,
                        required=False,
                    ),
                ),
            ),
        ),
    )


class _SimpleHandler(CommandHandler):
    def greet(self, name: str) -> CommandResult:
        return MessageResult(f"Hello, {name}!")

    def count(self, n: int = 5) -> CommandResult:
        return MessageResult(f"Counted {n}.")


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_handler_passes(self):
        validate_interface_handler(_simple_spec(), _SimpleHandler())

    def test_missing_method_raises(self):
        class Incomplete(CommandHandler):
            def greet(self, name: str) -> CommandResult:
                return MessageResult(name)

        with pytest.raises(InterfaceValidationError) as exc_info:
            validate_interface_handler(_simple_spec(), Incomplete())
        assert "count" in str(exc_info.value)

    def test_non_callable_raises(self):
        class BadHandler(CommandHandler):
            greet = "not a method"

            def count(self, n: int = 5) -> CommandResult:
                return MessageResult(str(n))

        with pytest.raises(InterfaceValidationError) as exc_info:
            validate_interface_handler(_simple_spec(), BadHandler())
        assert "not callable" in str(exc_info.value)


# ---------------------------------------------------------------------------
# _collect_commands tests
# ---------------------------------------------------------------------------


class TestCollectCommands:
    def test_flat_group(self):
        spec = _simple_spec()
        cmds = _collect_commands(spec)
        assert [c.name for c in cmds] == ["greet", "count"]

    def test_nested_subgroups(self):
        inner = CommandGroupSpec(
            name="sub",
            help="Sub.",
            commands=(
                CommandDef(name="inner-cmd", help="Inner.", handler_method="inner_cmd"),
            ),
        )
        outer = CommandGroupSpec(
            name="outer",
            help="Outer.",
            commands=(
                CommandDef(name="outer-cmd", help="Outer.", handler_method="outer_cmd"),
            ),
            subgroups=(inner,),
        )
        cmds = _collect_commands(outer)
        assert {c.name for c in cmds} == {"outer-cmd", "inner-cmd"}

    def test_single_command_spec(self):
        cmd = CommandDef(name="solo", help="Solo.", handler_method="solo")
        spec = SingleCommandSpec(parent_path=("parent",), command=cmd)
        cmds = _collect_commands(spec)
        assert len(cmds) == 1
        assert cmds[0].name == "solo"


# ---------------------------------------------------------------------------
# build_command_spec tests
# ---------------------------------------------------------------------------


class TestBuildCommandSpec:
    def test_command_group_type(self):
        result = build_command_spec(_simple_spec(), _SimpleHandler())
        assert result.command_type == CommandType.COMMAND_GROUP

    def test_parent_path(self):
        spec = CommandGroupSpec(
            name="nested",
            help="Nested.",
            parent_path=("snowpark",),
            commands=(
                CommandDef(name="hello", help="Hello.", handler_method="hello"),
            ),
        )

        class Handler(CommandHandler):
            def hello(self) -> CommandResult:
                return MessageResult("hi")

        result = build_command_spec(spec, Handler())
        assert result.parent_command_path == CommandPath(["snowpark"])

    def test_single_command_type(self):
        cmd = CommandDef(name="solo", help="Solo.", handler_method="solo")
        spec = SingleCommandSpec(parent_path=(), command=cmd)

        class Handler(CommandHandler):
            def solo(self) -> CommandResult:
                return MessageResult("done")

        result = build_command_spec(spec, Handler())
        assert result.command_type == CommandType.SINGLE_COMMAND

    def test_produces_click_command(self):
        result = build_command_spec(_simple_spec(), _SimpleHandler())
        click_cmd = result.command
        assert click_cmd is not None
        assert click_cmd.name == "test-plugin"

    def test_skip_validation(self):
        class Incomplete(CommandHandler):
            def greet(self, name: str) -> CommandResult:
                return MessageResult(name)

            def count(self, n: int = 5) -> CommandResult:
                return MessageResult(str(n))

        # With validate=False, validation is skipped (useful for production perf).
        # We still need handler methods for building, but no validation error is raised.
        result = build_command_spec(
            _simple_spec(), Incomplete(), validate=False
        )
        assert result.command is not None

    def test_flag_param(self):
        spec = CommandGroupSpec(
            name="flags",
            help="Flags test.",
            commands=(
                CommandDef(
                    name="run",
                    help="Run.",
                    handler_method="run",
                    params=(
                        ParamDef(
                            name="dry_run",
                            type=bool,
                            kind=ParamKind.OPTION,
                            cli_names=("--dry-run",),
                            is_flag=True,
                            default=False,
                            required=False,
                            help="Dry run mode",
                        ),
                    ),
                ),
            ),
        )

        class Handler(CommandHandler):
            def run(self, dry_run: bool = False) -> CommandResult:
                return MessageResult(f"dry_run={dry_run}")

        result = build_command_spec(spec, Handler())
        assert result.command is not None

    def test_unknown_decorator_raises(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(
                    name="run",
                    help="Run.",
                    handler_method="run",
                    decorators=("nonexistent_decorator",),
                ),
            ),
        )

        class Handler(CommandHandler):
            def run(self) -> CommandResult:
                return MessageResult("ok")

        with pytest.raises(ValueError, match="Unknown decorator"):
            build_command_spec(spec, Handler())
