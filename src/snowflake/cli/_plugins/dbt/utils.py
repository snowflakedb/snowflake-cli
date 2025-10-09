# Copyright (c) 2025 Snowflake Inc.
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

from snowflake.cli._plugins.dbt.constants import KNOWN_SUBCOMMANDS


def _extract_dbt_args(args: list[str]) -> list[str]:
    flags = set()

    for arg in args:
        if arg.startswith("-"):
            if "=" in arg:
                flag_name = arg.split("=", 1)[0]
                flags.add(flag_name)
            else:
                flags.add(arg)
        elif arg in KNOWN_SUBCOMMANDS:
            flags.add(arg)

    return sorted(list(flags))
