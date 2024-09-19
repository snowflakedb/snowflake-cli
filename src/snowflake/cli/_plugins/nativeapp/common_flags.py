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

import typer
from snowflake.cli._plugins.nativeapp.utils import is_tty_interactive


def interactive_callback(val):
    if val is None:
        return is_tty_interactive()
    return val


InteractiveOption = typer.Option(
    None,
    help=f"""When enabled, this option displays prompts even if the standard input and output are not terminal devices. Defaults to True in an interactive shell environment, and False otherwise.""",
    callback=interactive_callback,
    show_default=False,
)

ForceOption = typer.Option(
    False,
    "--force",
    help=f"""When enabled, this option causes the command to implicitly approve any prompts that arise.
    You should enable this option if interactive mode is not specified and if you want perform potentially destructive actions. Defaults to unset.""",
    is_flag=True,
)
ValidateOption = typer.Option(
    True,
    "--validate/--no-validate",
    help="""When enabled, this option triggers validation of a deployed Snowflake Native App's setup script SQL""",
    is_flag=True,
)
