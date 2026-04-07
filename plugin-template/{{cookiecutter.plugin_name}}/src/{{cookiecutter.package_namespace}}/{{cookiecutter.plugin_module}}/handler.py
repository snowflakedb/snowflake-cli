"""{{ cookiecutter.plugin_description }} -- Implementation.

This file contains the concrete handler implementing the interface
defined in ``interface.py``.  Submit this for review (Phase 2) after
the interface has been approved.
"""

from __future__ import annotations

from snowflake.cli.api.output.types import CommandResult, MessageResult

from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.interface import (
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
)


class {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl(
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
):
{% if cookiecutter.command_type == "group" %}
    def hello(self, name: str) -> CommandResult:
        return MessageResult(f"Hello, {name}!")

    def status(self) -> CommandResult:
        return MessageResult("Plugin is running.")
{%- else %}
    def run(self, name: str) -> CommandResult:
        return MessageResult(f"Hello, {name}!")
{%- endif %}
