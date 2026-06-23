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

"""{{ cookiecutter.plugin_description }} -- Implementation.

This file contains the concrete handler implementing the interface
defined in ``interface.py``.  Submit this for review (Phase 2) after
the interface has been approved.
"""

from __future__ import annotations

from snowflake.cli.api.output.types import CommandResult, MessageResult

from {{ cookiecutter.package_namespace }}.{{ cookiecutter.plugin_module }}.interface import (
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
)


class {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}HandlerImpl(
    {{ cookiecutter.plugin_module | replace('_', ' ') | title | replace(' ', '') }}Handler,
):
{% if cookiecutter.command_type == "group" %}
    def hello(self, name: str) -> CommandResult:
        return MessageResult(f"Hello, {name}!")

    def status(self) -> CommandResult:
        return MessageResult("Plugin is running.")
{%- else %}
    def run(self, name: str) -> CommandResult:
        return MessageResult(f"Hello, {name}!")
{%- endif %}
