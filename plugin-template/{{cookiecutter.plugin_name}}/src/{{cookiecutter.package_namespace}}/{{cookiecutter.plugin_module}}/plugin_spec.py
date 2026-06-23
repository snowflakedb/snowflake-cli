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

from snowflake.cli.api.plugins.command import plugin_hook_impl
from snowflake.cli.api.plugins.command.bridge import build_command_spec

from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.handler import (
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl,
)
from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.interface import PLUGIN_SPEC


@plugin_hook_impl
def command_spec():
    return build_command_spec(PLUGIN_SPEC, {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl())
