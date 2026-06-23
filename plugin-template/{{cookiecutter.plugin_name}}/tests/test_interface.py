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
