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

"""Tests for plugin testing utilities."""

from __future__ import annotations

import pytest

from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.plugins.command.bridge import InterfaceValidationError
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
)
from snowflake.cli.api.plugins.command.testing import (
    assert_builds_valid_spec,
    assert_handler_satisfies,
    assert_interface_well_formed,
)


def _valid_spec():
    return CommandGroupSpec(
        name="example",
        help="Example plugin.",
        commands=(
            CommandDef(
                name="hello",
                help="Say hello.",
                handler_method="hello",
                params=(
                    ParamDef(name="name", type=str, kind=ParamKind.ARGUMENT, help="Name"),
                ),
            ),
        ),
    )


class _ValidHandler(CommandHandler):
    def hello(self, name: str) -> CommandResult:
        return MessageResult(f"Hello {name}")


class TestAssertInterfaceWellFormed:
    def test_valid_spec_passes(self):
        assert_interface_well_formed(_valid_spec())

    def test_empty_command_name_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="", help="No name.", handler_method="run"),
            ),
        )
        with pytest.raises(AssertionError, match="empty name"):
            assert_interface_well_formed(spec)

    def test_empty_help_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="run", help="", handler_method="run"),
            ),
        )
        with pytest.raises(AssertionError, match="empty help"):
            assert_interface_well_formed(spec)

    def test_empty_handler_method_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="run", help="Run.", handler_method=""),
            ),
        )
        with pytest.raises(AssertionError, match="empty handler_method"):
            assert_interface_well_formed(spec)

    def test_invalid_identifier_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="run", help="Run.", handler_method="not-valid-python"),
            ),
        )
        with pytest.raises(AssertionError, match="not a valid Python identifier"):
            assert_interface_well_formed(spec)

    def test_duplicate_handler_method_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="run", help="Run.", handler_method="run"),
                CommandDef(name="also-run", help="Also run.", handler_method="run"),
            ),
        )
        with pytest.raises(AssertionError, match="Duplicate"):
            assert_interface_well_formed(spec)

    def test_invalid_param_name_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(
                    name="run",
                    help="Run.",
                    handler_method="run",
                    params=(
                        ParamDef(
                            name="not-valid",
                            type=str,
                            kind=ParamKind.ARGUMENT,
                            help="Bad param name",
                        ),
                    ),
                ),
            ),
        )
        with pytest.raises(AssertionError, match="not a valid Python identifier"):
            assert_interface_well_formed(spec)


class TestAssertHandlerSatisfies:
    def test_valid_handler_passes(self):
        assert_handler_satisfies(_valid_spec(), _ValidHandler())

    def test_incomplete_handler_fails(self):
        class Empty(CommandHandler):
            pass

        with pytest.raises(InterfaceValidationError):
            assert_handler_satisfies(_valid_spec(), Empty())


class TestAssertBuildsValidSpec:
    def test_valid_pair_passes(self):
        assert_builds_valid_spec(_valid_spec(), _ValidHandler())
