from snowflake.cli.api.plugins.command import build_command_spec, plugin_hook_impl

from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.handler import (
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl,
)
from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.interface import PLUGIN_SPEC


@plugin_hook_impl
def command_spec():
    return build_command_spec(PLUGIN_SPEC, {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl())
