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

"""Tests for the notebook plugin interface and handler contract."""

from __future__ import annotations

from snowflake.cli._plugins.notebook.handler import NotebookHandlerImpl
from snowflake.cli._plugins.notebook.interface import NOTEBOOK_SPEC, NotebookHandler
from snowflake.cli.api.plugins.command.testing import (
    assert_builds_valid_spec,
    assert_handler_satisfies,
    assert_interface_well_formed,
)


def test_notebook_interface_is_well_formed():
    assert_interface_well_formed(NOTEBOOK_SPEC)


def test_notebook_spec_has_5_commands():
    assert len(NOTEBOOK_SPEC.commands) == 5


def test_notebook_spec_command_names():
    names = {cmd.name for cmd in NOTEBOOK_SPEC.commands}
    assert names == {"execute", "get-url", "open", "create", "deploy"}


def test_all_commands_require_connection():
    assert all(cmd.requires_connection for cmd in NOTEBOOK_SPEC.commands)


def test_deploy_uses_project_definition_decorator():
    deploy = next(c for c in NOTEBOOK_SPEC.commands if c.name == "deploy")
    assert "with_project_definition" in deploy.decorators


def test_handler_satisfies_interface():
    assert_handler_satisfies(NOTEBOOK_SPEC, NotebookHandlerImpl())


def test_handler_is_subclass_of_abc():
    assert issubclass(NotebookHandlerImpl, NotebookHandler)


def test_builds_valid_command_spec():
    assert_builds_valid_spec(NOTEBOOK_SPEC, NotebookHandlerImpl())
