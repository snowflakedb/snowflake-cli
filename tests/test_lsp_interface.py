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

from __future__ import annotations

from unittest import mock

import pytest
from snowflake.cli.app.commands_registration import LoadedBuiltInCommandPlugin
from snowflake.cli.plugins.lsp.server import iter_lsp_plugins
from typing import get_type_hints, Any, Iterator, Dict, Callable

from pygls.server import LanguageServer

WORKSPACE_COMMAND = 'snowflake.cli.plugins.lsp.interface.workspace_command'


@pytest.fixture
def mock_workspace_command() -> Iterator[Dict[str, Callable]]:
    found_commands: Dict[str, Callable] = dict() 

    def workspace_command_impl(_server: Any, command_name: str):
        print("bleh", command_name)
        def _wrapper(fn: Callable):
            found_commands[command_name] = fn
        return _wrapper

    with mock.patch(WORKSPACE_COMMAND) as mocker:
        mocker.side_effect = workspace_command_impl
        mocker.found_commands = found_commands
        yield mocker


@pytest.mark.parametrize(
    "plugin",
    iter_lsp_plugins(),
    ids=lambda x: x.plugin_name,
)
def test_command_signatures(snapshot, mock_workspace_command, plugin: LoadedBuiltInCommandPlugin):
    """
    Compare command signatures to snapshots.
    """
    lsp_context = plugin.lsp_spec._lsp_context
    assert isinstance(lsp_context, dict)

    # first, call the lsp spec function to populate commands
    # we use mock_workspace_command to enumerate without doing any real work
    server = mock.Mock(spec=LanguageServer)
    plugin.lsp_spec(server)

    # make sure there are no missing / extra command implementations
    capabilities: dict = lsp_context["lsp_plugin_capabilities"]
    found_commands: dict = mock_workspace_command.found_commands
    assert sorted(found_commands.keys()) == sorted(capabilities.keys())

    # for each found workspace command, snapshot its type signature    
    signatures = { k: get_type_hints(v) for k, v in found_commands.items() }
    assert signatures == snapshot

    # TODO: this does not account for changes to the types themselves
    # we should recursively snapshot any typed dicts / dataclasses /
    # whatever will automatically be marshalled to / from JSON
