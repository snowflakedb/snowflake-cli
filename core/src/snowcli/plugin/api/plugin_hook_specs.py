from dataclasses import dataclass

import snowcli


@snowcli.plugin.api.plugin_hook_spec
def plugin_spec():
    """Plugin spec"""
    pass
