"""Tests for {{ cookiecutter.plugin_name }} interface and handler contract."""

from __future__ import annotations

from snowflake.cli.api.plugins.command.testing import (
    assert_builds_valid_spec,
    assert_handler_satisfies,
    assert_interface_well_formed,
)

from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.handler import (
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl,
)
from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.interface import (
    PLUGIN_SPEC,
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
)


def test_interface_is_well_formed():
    assert_interface_well_formed(PLUGIN_SPEC)


def test_handler_satisfies_interface():
    assert_handler_satisfies(PLUGIN_SPEC, {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl())


def test_handler_is_subclass_of_abc():
    assert issubclass(
        {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl,
        {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
    )


def test_builds_valid_command_spec():
    assert_builds_valid_spec(PLUGIN_SPEC, {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl())
