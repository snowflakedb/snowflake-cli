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
                    ParamDef(
                        name="name", type=str, kind=ParamKind.ARGUMENT, help="Name"
                    ),
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
            commands=(CommandDef(name="", help="No name.", handler_method="run"),),
        )
        with pytest.raises(AssertionError, match="empty name"):
            assert_interface_well_formed(spec)

    def test_empty_help_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(CommandDef(name="run", help="", handler_method="run"),),
        )
        with pytest.raises(AssertionError, match="empty help"):
            assert_interface_well_formed(spec)

    def test_empty_handler_method_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(CommandDef(name="run", help="Run.", handler_method=""),),
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

    def test_duplicate_command_name_in_group_fails(self):
        spec = CommandGroupSpec(
            name="bad",
            help="Bad.",
            commands=(
                CommandDef(name="run", help="Run.", handler_method="run_one"),
                CommandDef(name="run", help="Run again.", handler_method="run_two"),
            ),
        )
        with pytest.raises(AssertionError, match="Duplicate command name 'run'"):
            assert_interface_well_formed(spec)

    def test_same_command_name_across_subgroups_ok(self):
        # The same CLI command name may be reused in different subgroups; the
        # uniqueness check is per-group, not global.
        spec = CommandGroupSpec(
            name="root",
            help="Root.",
            subgroups=(
                CommandGroupSpec(
                    name="a",
                    help="Group A.",
                    commands=(
                        CommandDef(name="list", help="List.", handler_method="a_list"),
                    ),
                ),
                CommandGroupSpec(
                    name="b",
                    help="Group B.",
                    commands=(
                        CommandDef(name="list", help="List.", handler_method="b_list"),
                    ),
                ),
            ),
        )
        assert_interface_well_formed(spec)  # must not raise

    def test_duplicate_subgroup_name_in_group_fails(self):
        # Sibling subgroups share the parent's CLI namespace, so two subgroups
        # with the same name would silently shadow one another in Typer/Click —
        # the same class of bug as duplicate command names.
        spec = CommandGroupSpec(
            name="root",
            help="Root.",
            subgroups=(
                CommandGroupSpec(
                    name="dup",
                    help="First.",
                    commands=(CommandDef(name="a", help="A.", handler_method="a"),),
                ),
                CommandGroupSpec(
                    name="dup",
                    help="Second.",
                    commands=(CommandDef(name="b", help="B.", handler_method="b"),),
                ),
            ),
        )
        with pytest.raises(AssertionError, match="Duplicate name 'dup'"):
            assert_interface_well_formed(spec)

    def test_command_and_subgroup_name_collision_fails(self):
        # A leaf command and a subgroup occupy one namespace within their parent
        # group, so reusing a name across the two collides just like two
        # same-named commands would.
        spec = CommandGroupSpec(
            name="root",
            help="Root.",
            commands=(
                CommandDef(name="overlap", help="Cmd.", handler_method="overlap"),
            ),
            subgroups=(
                CommandGroupSpec(
                    name="overlap",
                    help="Group.",
                    commands=(
                        CommandDef(name="inner", help="Inner.", handler_method="inner"),
                    ),
                ),
            ),
        )
        with pytest.raises(AssertionError, match="Duplicate name 'overlap'"):
            assert_interface_well_formed(spec)

    def test_required_boolean_flag_fails(self):
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
                            name="force",
                            type=bool,
                            kind=ParamKind.OPTION,
                            is_flag=True,
                            # default omitted => REQUIRED
                            help="Force it",
                        ),
                    ),
                ),
            ),
        )
        with pytest.raises(AssertionError, match="boolean flag"):
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

        # assert_handler_satisfies wraps the underlying InterfaceValidationError
        # as AssertionError so all three assert_* helpers share one failure type.
        with pytest.raises(AssertionError, match="hello"):
            assert_handler_satisfies(_valid_spec(), Empty())


class TestAssertBuildsValidSpec:
    def test_valid_pair_passes(self):
        assert_builds_valid_spec(_valid_spec(), _ValidHandler())

    def test_invalid_pair_fails(self):
        # A handler missing a declared method must fail the full build path,
        # exercising the validate=True branch inside assert_builds_valid_spec.
        class Empty(CommandHandler):
            pass

        with pytest.raises(AssertionError, match="hello"):
            assert_builds_valid_spec(_valid_spec(), Empty())
