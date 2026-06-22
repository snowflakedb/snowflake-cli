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

"""Tests for plugin interface dataclasses."""

from __future__ import annotations

import pytest
from snowflake.cli.api.plugins.command.interface import (
    REQUIRED,
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
    SingleCommandSpec,
)


class TestParamDef:
    def test_required_argument(self):
        p = ParamDef(name="name", type=str, kind=ParamKind.ARGUMENT, help="A name")
        assert p.name == "name"
        assert p.type is str
        assert p.kind == ParamKind.ARGUMENT
        assert p.default is REQUIRED

    def test_optional_option_with_default(self):
        p = ParamDef(
            name="count",
            type=int,
            kind=ParamKind.OPTION,
            help="Count",
            default=10,
        )
        assert p.default == 10
        assert p.default is not REQUIRED

    def test_flag_option(self):
        p = ParamDef(
            name="replace",
            type=bool,
            kind=ParamKind.OPTION,
            is_flag=True,
            default=False,
        )
        assert p.is_flag is True
        assert p.default is False

    def test_frozen(self):
        p = ParamDef(name="x", type=str, kind=ParamKind.ARGUMENT)
        with pytest.raises(AttributeError):
            p.name = "y"


class TestCommandDef:
    def test_minimal(self):
        cmd = CommandDef(name="run", help="Run it.", handler_method="run")
        assert cmd.name == "run"
        assert cmd.params == ()
        assert cmd.requires_connection is False
        assert cmd.decorators == ()

    def test_with_params_and_decorators(self):
        cmd = CommandDef(
            name="deploy",
            help="Deploy.",
            handler_method="deploy",
            requires_connection=True,
            decorators=("with_project_definition",),
            params=(
                ParamDef(name="target", type=str, kind=ParamKind.ARGUMENT, help="T"),
            ),
        )
        assert len(cmd.params) == 1
        assert cmd.decorators == ("with_project_definition",)

    def test_frozen(self):
        cmd = CommandDef(name="x", help="h", handler_method="x")
        with pytest.raises(AttributeError):
            cmd.name = "y"


class TestCommandGroupSpec:
    def test_basic_group(self):
        spec = CommandGroupSpec(
            name="notebook",
            help="Notebooks.",
            commands=(
                CommandDef(name="run", help="Run.", handler_method="run"),
                CommandDef(name="stop", help="Stop.", handler_method="stop"),
            ),
        )
        assert spec.name == "notebook"
        assert len(spec.commands) == 2
        assert spec.parent_path == ()
        assert spec.subgroups == ()

    def test_nested_subgroups(self):
        inner = CommandGroupSpec(
            name="pool",
            help="Pool cmds.",
            commands=(
                CommandDef(name="create", help="Create.", handler_method="pool_create"),
            ),
        )
        outer = CommandGroupSpec(
            name="spcs",
            help="SPCS.",
            subgroups=(inner,),
        )
        assert len(outer.subgroups) == 1
        assert outer.subgroups[0].name == "pool"

    def test_frozen(self):
        spec = CommandGroupSpec(name="x", help="h")
        with pytest.raises(AttributeError):
            spec.name = "y"


class TestSingleCommandSpec:
    def test_basic(self):
        cmd = CommandDef(name="sql", help="Execute SQL.", handler_method="execute_sql")
        spec = SingleCommandSpec(parent_path=(), command=cmd)
        assert spec.command.name == "sql"


class TestCommandHandler:
    def test_is_marker_base_with_no_abstract_methods(self):
        # CommandHandler is a *marker* base: it declares no @abstractmethod
        # members, so Python's ABC machinery enforces nothing. The real
        # contract (every declared handler_method exists and is callable) is
        # enforced by the bridge's validate_interface_handler, not by ABC.
        assert CommandHandler.__abstractmethods__ == frozenset()

    def test_subclass_is_instantiable(self):
        # Because there are no abstract methods, even an empty subclass can be
        # instantiated freely — ABC does not block it. This documents that
        # enforcement is deferred to the bridge rather than the type system.
        class Empty(CommandHandler):
            pass

        assert isinstance(Empty(), CommandHandler)


class TestRequiredSentinel:
    def test_is_singleton(self):
        from snowflake.cli.api.plugins.command.interface import _RequiredSentinel

        assert _RequiredSentinel() is _RequiredSentinel()

    def test_repr(self):
        assert repr(REQUIRED) == "REQUIRED"

    def test_falsy(self):
        assert not REQUIRED
