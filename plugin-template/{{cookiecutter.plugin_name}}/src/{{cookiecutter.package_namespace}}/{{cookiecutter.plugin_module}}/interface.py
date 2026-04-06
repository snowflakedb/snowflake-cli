"""{{ cookiecutter.plugin_description }}

This file defines the command surface for ``snow {{ cookiecutter.cli_command_name }}``.
Submit this file for review (Phase 1) before writing the implementation.

Commands
--------
{%- if cookiecutter.command_type == "group" %}
- ``snow {{ cookiecutter.cli_command_name }} hello <name>``  -- Say hello.
- ``snow {{ cookiecutter.cli_command_name }} status``        -- Show status.
{%- else %}
- ``snow {{ cookiecutter.cli_command_name }} <name>``  -- Run the command.
{%- endif %}
"""

from __future__ import annotations

from abc import abstractmethod

from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
{%- if cookiecutter.command_type == "group" %}
    CommandGroupSpec,
{%- else %}
    SingleCommandSpec,
{%- endif %}
    CommandHandler,
    ParamDef,
    ParamKind,
    REQUIRED,
)

# ---------------------------------------------------------------------------
# Command surface (reviewable spec)
# ---------------------------------------------------------------------------

{% if cookiecutter.command_type == "group" -%}
PLUGIN_SPEC = CommandGroupSpec(
    name="{{ cookiecutter.cli_command_name }}",
    help="{{ cookiecutter.plugin_description }}",
    parent_path=({{ cookiecutter.cli_parent_path | default("", true) }}),
    commands=(
        CommandDef(
            name="hello",
            help="Say hello to someone.",
            handler_method="hello",
            requires_connection={{ cookiecutter.requires_connection }},
            params=(
                ParamDef(
                    name="name",
                    type=str,
                    kind=ParamKind.ARGUMENT,
                    help="Name to greet.",
                ),
            ),
            output_type="MessageResult",
        ),
        CommandDef(
            name="status",
            help="Show plugin status.",
            handler_method="status",
            requires_connection={{ cookiecutter.requires_connection }},
            output_type="MessageResult",
        ),
    ),
)
{%- else -%}
PLUGIN_SPEC = SingleCommandSpec(
    parent_path=({{ cookiecutter.cli_parent_path | default("", true) }}),
    command=CommandDef(
        name="{{ cookiecutter.cli_command_name }}",
        help="{{ cookiecutter.plugin_description }}",
        handler_method="run",
        requires_connection={{ cookiecutter.requires_connection }},
        params=(
            ParamDef(
                name="name",
                type=str,
                kind=ParamKind.ARGUMENT,
                help="Name argument.",
            ),
        ),
        output_type="MessageResult",
    ),
)
{%- endif %}


# ---------------------------------------------------------------------------
# Handler contract (ABC)
# ---------------------------------------------------------------------------


class {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler(CommandHandler):
    """Handler contract for {{ cookiecutter.cli_command_name }} commands.

    Each abstract method corresponds to a ``CommandDef`` above via
    ``handler_method``.
    """
{% if cookiecutter.command_type == "group" %}
    @abstractmethod
    def hello(self, name: str) -> CommandResult:
        """Say hello."""
        ...

    @abstractmethod
    def status(self) -> CommandResult:
        """Show status."""
        ...
{% else %}
    @abstractmethod
    def run(self, name: str) -> CommandResult:
        """Run the command."""
        ...
{% endif %}
