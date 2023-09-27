from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, List

from click import Command


@dataclass
class _Node:
    name: str
    children: Dict[str, _Node] = field(default_factory=dict)
    level: int = 0
    options: Dict[str, List[str]] = field(default_factory=dict)

    def print(self):
        print("    " * self.level, self.name)
        for ch in self.children.values():
            ch.print()

    def print_with_options(self):
        pass #TODO: fill this



def generate_commands_structure(command: Command, root: _Node | None = None):
    """
    Iterates recursively through commands info. Creates tree-like structure
    of commands.
    """
    if not root:
        root = _Node(name="snow")

    if command.params:
        root.options = {param.human_readable_name : param.opts for param in command.params}

    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            if command_name not in root.children:
                root.children[command_name] = _Node(command_name, level=root.level + 1)
            generate_commands_structure(command_info, root.children[command_name])
    return root


def generate_options_structure(command: Command, root: Optional[_Node] = None):
    pass
