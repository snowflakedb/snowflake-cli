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
import os
from typing import Dict, Set

from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.sanitizers import sanitize_for_terminal


def collect_env_vars(declared_names: Set[str]) -> Dict[str, str]:
    """Collect values for declared env var names from the process environment.

    Names declared in the manifest but not present in the environment trigger
    a warning and are omitted — GS handles the absence.
    """
    result = {name: os.environ[name] for name in declared_names if name in os.environ}
    missing = declared_names - result.keys()
    if missing:
        names = ", ".join(sanitize_for_terminal(name) for name in sorted(missing))
        cli_console.warning(
            f"Declared in {MANIFEST_FILE_NAME} but could not be resolved from "
            f"the shell environment: {names}."
        )
    return result
