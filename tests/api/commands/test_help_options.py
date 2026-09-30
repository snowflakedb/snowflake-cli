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

import click
import pytest
from snowflake.cli.api.commands.flags import (
    CONNECTION_CONFIGURATION_PANEL,
    GLOBAL_CONFIGURATION_PANEL,
)
from snowflake.cli.api.commands.help_options import (
    condensed_option_flag_names,
    condensed_shared_options_panel_title,
    condensed_usage_suffix,
    conflicting_help_flags,
    hidden_injected_help_params,
    injected_help_params_reference,
    is_injected_help_param,
    iter_shared_configuration_params,
    raise_if_conflicting_help_flags,
)
from snowflake.cli.api.exceptions import CliArgumentError


class _HelpPanelParam:
    def __init__(self, panel: str | None) -> None:
        self.rich_help_panel = panel


def test_is_injected_help_param():
    assert is_injected_help_param(_HelpPanelParam(CONNECTION_CONFIGURATION_PANEL))
    assert is_injected_help_param(_HelpPanelParam(GLOBAL_CONFIGURATION_PANEL))
    assert not is_injected_help_param(_HelpPanelParam(None))
    assert not is_injected_help_param(_HelpPanelParam("Options"))


def test_conflicting_help_flags():
    assert not conflicting_help_flags(argv=["snow", "sql", "--help"])
    assert not conflicting_help_flags(argv=["snow", "sql", "-h"])
    assert not conflicting_help_flags(argv=["snow", "sql", "--help-all"])
    assert conflicting_help_flags(argv=["snow", "sql", "--help", "--help-all"])
    assert conflicting_help_flags(argv=["snow", "sql", "--help-all", "--help"])
    assert conflicting_help_flags(argv=["snow", "sql", "-h", "--help-all"])
    assert conflicting_help_flags(argv=["snow", "sql", "--help-all", "-h"])

    ctx = click.Context(click.Command("cmd"))
    ctx.ensure_object(dict)
    ctx.obj["invocation_args"] = ["cmd", "--help", "--help-all"]
    assert conflicting_help_flags(ctx)


def test_raise_if_conflicting_help_flags_raises_cli_argument_error():
    ctx = click.Context(click.Command("cmd"))
    ctx.ensure_object(dict)
    ctx.obj["invocation_args"] = ["cmd", "--help", "--help-all"]

    with pytest.raises(CliArgumentError, match="Cannot use --help with --help-all"):
        raise_if_conflicting_help_flags(ctx)


def test_condensed_usage_suffix_from_reference_command():
    command = injected_help_params_reference()
    ctx = click.Context(command)
    suffix = condensed_usage_suffix(command, ctx)

    assert "[global options]" in suffix
    assert "[connection options]" in suffix


def test_condensed_shared_options_panel_title():
    assert (
        condensed_shared_options_panel_title(
            {CONNECTION_CONFIGURATION_PANEL, GLOBAL_CONFIGURATION_PANEL}
        )
        == "Global and connection options"
    )
    assert condensed_shared_options_panel_title({CONNECTION_CONFIGURATION_PANEL}) == (
        "Connection options"
    )
    assert condensed_shared_options_panel_title({GLOBAL_CONFIGURATION_PANEL}) == (
        "Global options"
    )
    assert condensed_shared_options_panel_title(set()) == ""
    assert condensed_shared_options_panel_title({None}) == ""


def _injected_help_options(
    command: click.Command, ctx: click.Context
) -> list[click.Option]:
    return [
        param
        for param in command.get_params(ctx)
        if isinstance(param, click.Option) and is_injected_help_param(param)
    ]


def test_hidden_injected_help_params_restores_visibility():
    command = injected_help_params_reference()
    ctx = click.Context(command)
    injected = _injected_help_options(command, ctx)
    hidden_before = {param: param.hidden for param in injected}
    assert any(not hidden for hidden in hidden_before.values())

    with hidden_injected_help_params(command, ctx):
        for param in injected:
            if not hidden_before[param]:
                assert param.hidden

    for param, was_hidden in hidden_before.items():
        assert param.hidden == was_hidden


def test_hidden_injected_help_params_restores_on_exception():
    command = injected_help_params_reference()
    ctx = click.Context(command)
    injected = _injected_help_options(command, ctx)
    hidden_before = {param: param.hidden for param in injected}

    with pytest.raises(RuntimeError, match="boom"):
        with hidden_injected_help_params(command, ctx):
            raise RuntimeError("boom")

    for param, was_hidden in hidden_before.items():
        assert param.hidden == was_hidden


def test_condensed_option_flag_names_from_reference_command():
    names = condensed_option_flag_names(
        iter_shared_configuration_params(CONNECTION_CONFIGURATION_PANEL)
    )

    assert names.startswith("--connection,")
    assert "--account" in names
