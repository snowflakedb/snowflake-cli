from __future__ import annotations

import logging
from typing import Dict, List, Any

import typer
from click import UsageError

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.snow_cli_global_context import global_context_copy
from snowcli.config import cli_config
from snowcli.output.printing import print_data
from snowcli.plugin import PluginLoadingMode, LoadedPlugin, PluginAfterVerification
from snowcli.plugin.load_plugins import LoadPlugins
from snowcli.plugin.verify_plugins import VerifyPlugins

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="plugin",
    help="Manage plugins",
)
log = logging.getLogger(__name__)


def _info_about_plugin(
    plugin_after_verification: PluginAfterVerification,
) -> Dict[str, Any]:
    plugin = plugin_after_verification.plugin
    command_group = " ".join(
        plugin.command_group_spec.path.path_segments
        + [plugin.command_group_spec.command_group.info.name]
    )
    return {
        "plugin_name": plugin.plugin_name,
        "enabled": plugin.is_enabled,
        "command_group": command_group,
        "package_name": plugin.plugin_package_info.package_name,
        "package_version": plugin.plugin_package_info.package_version,
        "verification_error": plugin_after_verification.error_str(),
    }


def _print_info_about_plugins(plugins_to_list: List[PluginAfterVerification]) -> None:
    data_to_print = [_info_about_plugin(plugin) for plugin in plugins_to_list]
    sorted_data_to_print = sorted(data_to_print, key=lambda p: p["plugin_name"])
    print_data(
        sorted_data_to_print,
        columns=[
            "plugin_name",
            "enabled",
            "command_group",
            "package_name",
            "package_version",
            "verification_error",
        ],
    )


@app.command(name="list")
def list_plugins(
    only_enabled: bool = typer.Option(
        None, "--enabled", help="Show only enabled plugins"
    )
):
    """
    List installed plugins.
    """
    plugin_loading_mode = (
        PluginLoadingMode.ONLY_ENABLED_PLUGINS
        if only_enabled
        else PluginLoadingMode.ALL_INSTALLED_PLUGINS
    )
    plugins_to_list = LoadPlugins(plugin_loading_mode)()
    verified_plugins_to_list = VerifyPlugins(plugins_to_list)()
    _print_info_about_plugins(verified_plugins_to_list)


def _verify_that_plugin_is_installed(plugin_name: str) -> None:
    all_installed_plugin_names = []
    try:
        all_installed_plugin_names = [
            plugin.plugin_name
            for plugin in LoadPlugins(PluginLoadingMode.ALL_INSTALLED_PLUGINS)()
        ]
    except Exception:
        log.debug(
            msg=f"Cannot verify if plugin [{plugin_name}] is installed",
            exc_info=global_context_copy().debug,
        )
        typer.confirm(
            f"Cannot verify if plugin [{plugin_name}] is installed. Do you want to continue?",
            abort=True,
        )
    if plugin_name not in all_installed_plugin_names:
        raise UsageError(f"Plugin [{plugin_name}] is not installed.")


@app.command("enable")
def enable(
    plugin_name: str = typer.Argument(None, help="Name of the plugin to enable"),
):
    """
    Enable plugin.
    """
    _verify_that_plugin_is_installed(plugin_name)
    cli_config.enable_plugin(plugin_name)
    log.info(f"Plugin [{plugin_name}] enabled")


@app.command("disable")
def disable(
    plugin_name: str = typer.Argument(None, help="Name of the plugin to disable"),
):
    """
    Disable plugin.
    """
    cli_config.disable_plugin(plugin_name)
    log.info(f"Plugin [{plugin_name}] disabled")
