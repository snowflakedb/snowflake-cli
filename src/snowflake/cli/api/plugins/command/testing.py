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
    _collect_commands,
    build_command_spec,
    validate_interface_handler,
)
from snowflake.cli.api.plugins.command.interface import (
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
    - No duplicate handler_method values across the entire tree.
    - handler_method values are valid Python identifiers.

    Raises ``AssertionError`` on the first violation found.
    """
    commands = _collect_commands(spec)
    seen_methods: set[str] = set()

    for cmd in commands:
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


def assert_handler_satisfies(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
) -> None:
    """Validate that *handler* implements all methods required by *spec*.

    Delegates to ``validate_interface_handler`` which raises
    ``InterfaceValidationError`` with all violations listed.
    """
    validate_interface_handler(spec, handler)


def assert_builds_valid_spec(
    spec: CommandGroupSpec | SingleCommandSpec,
    handler: CommandHandler,
) -> None:
    """Full integration check: spec + handler produce a valid ``CommandSpec``.

    Builds the ``CommandSpec`` (with validation) and verifies that the
    resulting Click command tree was created successfully.
    """
    result = build_command_spec(spec, handler, validate=True)
    assert result.command is not None, "build_command_spec produced a None Click command"
