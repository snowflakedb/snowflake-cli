from __future__ import annotations

from pathlib import Path
from typing import Optional, List

from jinja2 import FileSystemLoader, Environment


def generate_docs(root: Path, command: dict, path: Optional[List] = None):
    """
    Iterates recursively through commands info. Creates a file structure resembling
    commands structure. For each terminal command creates a "usage" rst file.
    """
    if "hidden" in command and command["hidden"]:
        return
    root.mkdir(exist_ok=True)
    path = path or []
    if "commands" in command:
        for command_name, command_info in command["commands"].items():
            generate_docs(root / command_name, command_info, [*path, command_name])
    else:
        # This is end command
        command_name = command["name"]
        env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))
        template = env.get_template("usage.rst.jinja2")

        with open(root / f"usage-{command_name}.rst", "w+") as fh:
            fh.write(template.render({**command, "path": path}))
