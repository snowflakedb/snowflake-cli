from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from click import Command
from jinja2 import Environment, FileSystemLoader
from typer.core import TyperArgument

log = logging.getLogger(__name__)

CMD_USAGE_TMPL = "usage.rst.jinja2"
OVERVIEW_TMPL = "overview.rst.jinja2"


def generate_docs(root: Path, command: Command, cmd_parts: Optional[List] = None):
    """
    Iterates recursively through commands info. Creates a file structure resembling
    commands structure. For each terminal command creates a "usage" rst file.
    """
    if getattr(command, "hidden", False):
        return

    root.mkdir(exist_ok=True)
    if cmd_parts is None:
        _render_usage(command, root, cmd_parts, template_name=OVERVIEW_TMPL)

    cmd_parts = cmd_parts or []
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            path = root / command.name if command.name != "default" else root
            generate_docs(path, command_info, [*cmd_parts, command_name])
    else:
        _render_usage(command, root, cmd_parts)


def get_main_option(options: List[str]) -> str:
    long_options = [option for option in options if option.startswith("--")]
    short_options = [option for option in options if option.startswith("-")]
    if long_options:
        return long_options[0]
    if short_options:
        return short_options[0]
    return ""


def _render_usage(
    command: Command,
    root: Path,
    path: Optional[List] = None,
    template_name: str = CMD_USAGE_TMPL,
):
    # This is end command
    command_name = command.name
    env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))
    env.filters[get_main_option.__name__] = get_main_option
    template = env.get_template(template_name)
    arguments = []
    options = []
    for param in command.params:
        if isinstance(param, TyperArgument):
            arguments.append(param)
        else:
            options.append(param)
    file_path = root / f"usage-{command_name}.rst"
    log.info("Creating %s", file_path)
    with open(file_path, "w+") as fh:
        fh.write(
            template.render(
                {
                    "help": command.help,
                    "name": command_name,
                    "options": options,
                    "arguments": arguments,
                    "path": path,
                }
            )
        )
