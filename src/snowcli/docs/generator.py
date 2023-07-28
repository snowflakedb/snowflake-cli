from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, List

from click import Command
from jinja2 import FileSystemLoader, Environment
from typer.core import TyperArgument

log = logging.getLogger(__name__)


def generate_docs(root: Path, command: Command, path: Optional[List] = None):
    """
    Iterates recursively through commands info. Creates a file structure resembling
    commands structure. For each terminal command creates a "usage" rst file.
    """
    if getattr(command, "hidden", False):
        return
    root.mkdir(exist_ok=True)
    path = path or []
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            generate_docs(root / command_name, command_info, [*path, command_name])
    else:
        # This is end command
        command_name = command.name
        env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))
        template = env.get_template("usage.rst.jinja2")

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
