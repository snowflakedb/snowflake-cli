from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from click import Command


@dataclass
class _Node:
    name: str
    children: Dict[str, _Node] = field(default_factory=dict)
    level: int = 0

    def print_node(self):
        print("    " * self.level, self.name)
        for ch in self.children.values():
            ch.print_node()


def generate_commands_structure(command: Command, root: _Node | None = None):
    """
    Iterates recursively through commands info. Creates tree-like structure
    of commands.
    """
    if not root:
        root = _Node(name="snow")

    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            if command_name not in root.children:
                root.children[command_name] = _Node(command_name, level=root.level + 1)
            generate_commands_structure(command_info, root.children[command_name])
    return root
