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

"""Test utilities for plugin authors.

These helpers validate interface specs and handler implementations without
requiring a Snowflake connection.  Use them in unit tests to catch contract
mismatches early::

    from snowflake.cli.api.plugins.command.testing import (
        assert_interface_well_formed,
        assert_handler_satisfies,
        assert_builds_valid_spec,
    )

    def test_my_interface():
        assert_interface_well_formed(MY_SPEC)

    def test_my_handler():
        assert_handler_satisfies(MY_SPEC, MyHandlerImpl())

    def test_full_build():
        assert_builds_valid_spec(MY_SPEC, MyHandlerImpl())
"""

from __future__ import annotations

from snowflake.cli.api.plugins.command.bridge import (
    InterfaceValidationError,
    build_command_spec,
    validate_interface_handler,
)
from snowflake.cli.api.plugins.command.interface import (
    REQUIRED,
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    SingleCommandSpec,
)


def assert_interface_well_formed(
    spec: CommandGroupSpec | SingleCommandSpec,
) -> None:
    """Validate the spec dataclass tree is complete and consistent.

    Checks:
    - Every command has a non-empty name, help text, and handler_method.
    - handler_method values are valid Python identifiers and unique across the
      entire tree (each maps to one method on a single handler instance).
    - Command (CLI) names are unique *within their group* — sibling commands
      may not collide, but two different subgroups may reuse a name.
    - Param names are valid Python identifiers.
    - No boolean flag (``is_flag=True``) is left required (``default=REQUIRED``);
      a required flag is meaningless. Mirrors the build-time guard in the bridge.

    Raises ``AssertionError`` on the first violation found.
    """
    # handler_method must be unique across the whole tree: every method resolves
    # against one handler instance, so any collision is a genuine conflict.
    seen_methods: set[str] = set()

    def check_command(cmd: CommandDef) -> None:
        assert cmd.name, "Command has empty name"
        assert cmd.help, f"Command '{cmd.name}' has empty help text"
        assert cmd.handler_method, f"Command '{cmd.name}' has empty handler_method"
        assert cmd.handler_method.isidentifier(), (
            f"Command '{cmd.name}': handler_method '{cmd.handler_method}' "
            f"is not a valid Python identifier"
        )
        assert cmd.handler_method not in seen_methods, (
            f"Duplicate handler_method '{cmd.handler_method}' "
            f"(used by command '{cmd.name}')"
        )
        seen_methods.add(cmd.handler_method)

        for param in cmd.params:
            assert param.name, f"Command '{cmd.name}' has a param with empty name"
            assert param.name.isidentifier(), (
                f"Command '{cmd.name}': param name '{param.name}' "
                f"is not a valid Python identifier"
            )
            assert not (param.is_flag and param.default is REQUIRED), (
                f"Command '{cmd.name}': param '{param.name}' is a boolean flag "
                f"(is_flag=True) with no default; give it an explicit default "
                f"(e.g. default=False)"
            )

    def check_group(group: CommandGroupSpec) -> None:
        # Command names need only be unique among siblings — two different
        # subgroups may legitimately expose the same command name.
        seen_names: set[str] = set()
        for cmd in group.commands:
            check_command(cmd)
            assert (
                cmd.name not in seen_names
            ), f"Duplicate command name '{cmd.name}' in group '{group.name}'"
            seen_names.add(cmd.name)
        for sub in group.subgroups:
            check_group(sub)

    if isinstance(spec, SingleCommandSpec):
        check_command(spec.command)
    else:
        check_group(spec)


def assert_handler_satisfies(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
) -> None:
    """Validate that *handler* implements all methods required by *spec*.

    Delegates to ``validate_interface_handler`` and re-raises its
    ``InterfaceValidationError`` as an ``AssertionError`` so all three
    ``assert_*`` helpers fail with the same exception type — plugin authors
    can write ``pytest.raises(AssertionError)`` uniformly across them.
    """
    try:
        validate_interface_handler(spec, handler)
    except InterfaceValidationError as e:
        raise AssertionError(str(e)) from e


def assert_builds_valid_spec(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
) -> None:
    """Full integration check: spec + handler produce a valid ``CommandSpec``.

    Builds the ``CommandSpec`` (with validation) and verifies that the
    resulting Click command tree was created successfully. Any build or
    validation failure (``InterfaceValidationError`` for a missing handler
    method, ``ValueError`` for a malformed spec) is re-raised as an
    ``AssertionError`` so this helper fails with the same exception type as
    its siblings.
    """
    try:
        result = build_command_spec(spec, handler, validate=True)
    except (InterfaceValidationError, ValueError) as e:
        raise AssertionError(str(e)) from e
    assert (
        result.command is not None
    ), "build_command_spec produced a None Click command"
