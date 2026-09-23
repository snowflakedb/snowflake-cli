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

import logging
from typing import Any, List, Optional

from click import Command
from snowflake.cli._app.dev.docs.template_utils import get_template_environment
from snowflake.cli.api.commands.command_docs import CommandDocs, get_command_docs
from snowflake.cli.api.commands.command_docs_rendering import (
    mdx_escape,
    render_paragraph_mdx,
    render_usage_mdx,
)
from snowflake.cli.api.secure_path import SecurePath
from typer.core import TyperArgument

log = logging.getLogger(__name__)

CMD_USAGE_TMPL = "usage.mdx.jinja2"
OVERVIEW_TMPL = "overview.mdx.jinja2"
CMD_PAGE_TMPL = "page.mdx.jinja2"


def generate_command_docs(
    root: SecurePath, command: Command, cmd_parts: Optional[List] = None
):
    """
    Iterates recursively through commands info. Creates a file structure resembling
    commands structure. For each terminal command creates a "usage" MDX file.
    """
    if getattr(command, "hidden", False):
        return

    root.mkdir(exist_ok=True)
    if cmd_parts is None:
        _render_command_usage(command, root, cmd_parts, template_name=OVERVIEW_TMPL)

    cmd_parts = cmd_parts or []
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            path = root / command.name if command.name != "default" else root
            generate_command_docs(path, command_info, [*cmd_parts, command_name])
    else:
        _render_command_usage(command, root, cmd_parts)


def generate_command_pages(
    root: SecurePath,
    command: Command,
    cmd_parts: Optional[List] = None,
):
    """Iterates recursively through commands. For each terminal command creates a full MDX page."""
    if getattr(command, "hidden", False):
        return

    cmd_parts = cmd_parts or []
    root.mkdir(exist_ok=True)
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            path = root / command.name if command.name != "default" else root
            generate_command_pages(path, command_info, [*cmd_parts, command_name])
    else:
        _write_command_page(command, root, cmd_parts)


def get_main_option(options: List[str]) -> str:
    long_options = [option for option in options if option.startswith("--")]
    if long_options:
        return long_options[0]

    short_options = [option for option in options if option.startswith("-")]
    if short_options:
        return short_options[0]

    return ""


def collapse_whitespace(value: Optional[str]) -> str:
    """Collapses runs of whitespace, including newlines, into single spaces."""
    if value is None:
        return ""
    return " ".join(str(value).split())


def _template_env_with_filters():
    env = get_template_environment()
    env.filters[get_main_option.__name__] = get_main_option
    env.filters[collapse_whitespace.__name__] = collapse_whitespace
    env.filters[mdx_escape.__name__] = mdx_escape
    env.filters[render_usage_mdx.__name__] = render_usage_mdx
    env.filters[render_paragraph_mdx.__name__] = render_paragraph_mdx
    return env


def _split_params(command: Command):
    arguments = []
    options = []
    for param in command.params:
        if isinstance(param, TyperArgument):
            arguments.append(param)
        else:
            options.append(param)
    return arguments, options


def _render_command_usage(
    command: Command,
    root: SecurePath,
    path: Optional[List] = None,
    template_name: str = CMD_USAGE_TMPL,
):
    # This is end command
    command_name = command.name
    env = _template_env_with_filters()
    template = env.get_template(template_name)
    arguments, options = _split_params(command)

    # MDX include fragments that hand-authored command-reference pages in
    # snowflake-prod-docs compose via MDX imports.
    file_path = root / f"usage-{command_name}.mdx"
    log.info("Creating %s", file_path)
    command_help_params = _split_docstring(command.help)
    template_params = {
        "name": command_name,
        "options": options,
        "arguments": arguments,
        "path": path,
    }
    with file_path.open("w+") as fh:
        fh.write(template.render(command_help_params | template_params))


def _write_command_page(command: Command, root: SecurePath, path: List):
    file_path = root / f"{command.name}.mdx"
    log.info("Creating %s", file_path)
    with file_path.open("w+") as fh:
        fh.write(_command_page_markdown(command, path))


def _command_page_markdown(command: Command, path: List) -> str:
    env = _template_env_with_filters()
    template = env.get_template(CMD_PAGE_TMPL)
    arguments, options = _split_params(command)
    command_help_params = _split_docstring(command.help)
    docs = get_command_docs(command)
    template_params = {
        "name": command.name,
        "options": options,
        "arguments": arguments,
        "path": path,
        "docs": docs,
        "help": command_help_params.get("help", ""),
        "usage_notes_fallback": _page_usage_notes_fallback(docs, command_help_params),
        "examples_fallback": _page_examples_fallback(docs, command_help_params),
    }
    return template.render(template_params)


def _split_docstring(command_help: Optional[str]) -> dict[str, Any]:
    if command_help is None:
        return {}

    split_command_help = command_help.split("## ")

    if len(split_command_help) == 1:
        return {"help": split_command_help[0]}

    additional_sections = []
    for section in split_command_help[1:]:
        lines = section.split("\n")
        additional_sections.append(
            {"title": lines[0], "content": "\n".join(lines[1:]).strip()}
        )
    return {
        "help": split_command_help[0],
        "additional_sections": additional_sections,
    }


def _additional_section(command_help_params: dict[str, Any], title: str) -> str | None:
    for section in command_help_params.get("additional_sections") or ():
        if section["title"].strip().casefold() == title.casefold():
            return section["content"] or None
    return None


def _page_usage_notes_fallback(
    docs: CommandDocs, command_help_params: dict[str, Any]
) -> str | None:
    if docs.usage_notes is not None:
        return None
    return _additional_section(command_help_params, "Usage notes")


def _page_examples_fallback(
    docs: CommandDocs, command_help_params: dict[str, Any]
) -> str | None:
    if docs.examples is not None:
        return None
    return _additional_section(command_help_params, "Examples")
