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

import json
from enum import Enum
from pathlib import Path

import click
from snowflake.cli._app.dev.commands_structure import build_command_catalog


class _Kind(Enum):
    TABLE = "TABLE"


@click.group()
@click.option("--verbose", is_flag=True, help="Chatty output.")
@click.option("--secret-root", hidden=True)
def _cli():
    """Root help."""


@_cli.command()
@click.argument("name")
@click.option("--connection", "-c", help="Connection name.", default="dev")
@click.option("--kind", type=click.Choice([k.value for k in _Kind]), default="TABLE")
@click.option("--hidden-flag", hidden=True)
def show(name, connection, kind, hidden_flag):
    """Show an object."""


@_cli.group()
def nested():
    """Nested group."""


@nested.command()
def list_():
    """List nested things."""


@_cli.command(hidden=True)
def secret():
    """Must not appear in the catalog."""


def test_catalog_includes_help_args_and_flags():
    catalog = build_command_catalog(_cli, name="snow")
    json.dumps(catalog)

    assert catalog["name"] == "snow"
    assert catalog["help"] == "Root help."
    root_flags = {flag for option in catalog["options"] for flag in option["flags"]}
    assert "--verbose" in root_flags
    assert "--secret-root" not in root_flags
    verbose = next(opt for opt in catalog["options"] if "--verbose" in opt["flags"])
    assert verbose["is_flag"] is True
    assert verbose["required"] is False

    names = {child["name"] for child in catalog["commands"]}
    assert names == {"nested", "show"}
    assert "secret" not in names

    show_cmd = next(child for child in catalog["commands"] if child["name"] == "show")
    assert show_cmd["help"] == "Show an object."
    assert show_cmd["arguments"] == [
        {"name": "name", "help": "", "required": True},
    ]
    option_by_name = {opt["name"]: opt for opt in show_cmd["options"]}
    assert option_by_name["connection"]["flags"] == ["--connection", "-c"]
    assert option_by_name["connection"]["default"] == "dev"
    assert option_by_name["connection"]["help"] == "Connection name."
    assert "hidden_flag" not in option_by_name
    assert "commands" not in show_cmd

    nested_cmd = next(
        child for child in catalog["commands"] if child["name"] == "nested"
    )
    assert nested_cmd["help"] == "Nested group."
    assert [child["name"] for child in nested_cmd["commands"]] == ["list-"]


def test_catalog_includes_boolean_pairs_and_choice_values():
    @click.command()
    @click.option(
        "--auto-compile/--no-auto-compile",
        default=None,
        help="Compile on deploy.",
    )
    @click.option(
        "--format",
        type=click.Choice(["TABLE", "JSON"]),
        default="TABLE",
        help="Output format.",
    )
    def deploy(auto_compile, format):  # noqa: A002
        """Deploy."""

    catalog = build_command_catalog(deploy, name="deploy")
    by_name = {option["name"]: option for option in catalog["options"]}
    assert by_name["auto_compile"]["flags"] == [
        "--auto-compile",
        "--no-auto-compile",
    ]
    assert "choices" not in by_name["auto_compile"]
    assert by_name["format"]["choices"] == ["TABLE", "JSON"]


def test_catalog_json_safe_defaults_for_enum_and_path():
    @click.command()
    @click.option("--out", type=click.Path(), default=Path("/tmp/out"))
    def dump(out):
        """Dump."""

    catalog = build_command_catalog(dump, name="dump")
    option = catalog["options"][0]
    assert option["default"] == "/tmp/out"
