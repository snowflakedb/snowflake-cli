from dataclasses import dataclass

import snowcli


@snowcli.plugin.api.plugin_hook_spec
def plugin_command_group_spec():
    """Plugin command group spec"""
    pass
