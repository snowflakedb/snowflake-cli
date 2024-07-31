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

from typing import Callable

from snowflake.cli.api.cli_global_context import get_cli_context_manager


def register_pre_execute_command(command: Callable[[], None]) -> None:
    get_cli_context_manager().add_typer_pre_execute_commands(command)


def run_pre_execute_commands() -> None:
    for command in get_cli_context_manager().typer_pre_execute_commands:
        command()
