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
import logging
import os
from typing import Collection, Dict, Optional, Set

from dotenv import dotenv_values
from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)


def _warn_about_missing(missing: Set[str], source_description: str) -> None:
    names = ", ".join(sanitize_for_terminal(name) for name in sorted(missing))
    cli_console.warning(
        f"Declared in {MANIFEST_FILE_NAME} but could not be resolved from "
        f"{source_description}: {names}."
    )


def _log_resolved(names: Collection[str], source_description: str) -> None:
    # Names only, never values -- some of these may be secrets.
    if names:
        log.info("Resolved env vars from %s: %s", source_description, sorted(names))


def collect_env_vars(declared_names: Set[str]) -> Dict[str, str]:
    """Collect values for declared env var names from the process environment.

    Names declared in the manifest but not present in the environment trigger
    a warning and are omitted — GS handles the absence.
    """
    result = {name: os.environ[name] for name in declared_names if name in os.environ}
    _log_resolved(result.keys(), "the shell environment")
    missing = declared_names - result.keys()
    if missing:
        _warn_about_missing(missing, "the shell environment")
    return result


def parse_env_file(path: SecurePath) -> Dict[str, str]:
    """Parse a .env file into a name -> value dict via python-dotenv.

    See https://pypi.org/project/python-dotenv/ for the supported .env
    syntax. Requires UTF-8 encoding.
    """
    if not path.exists():
        raise CliError(f"Env file {path.path} was not found.")
    if not path.is_file():
        raise CliError(f"Env file {path.path} is not a file.")
    with path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8") as fd:
        values = dotenv_values(stream=fd, interpolate=False)
    return {name: value for name, value in values.items() if value is not None}


def resolve_declared_env_vars(
    declared_names: Set[str], env_file: Optional[SecurePath]
) -> Dict[str, str]:
    """Collect declared env var/secret values, layering the shell over an
    optional .env file.

    The shell always wins on a name declared in both; the file only fills in
    names the shell doesn't already provide. env_file is validated eagerly
    (existence, size), even when declared_names is empty.
    """
    if env_file is None:
        return collect_env_vars(declared_names)

    file_values = parse_env_file(env_file)
    from_shell = {name for name in declared_names if name in os.environ}
    result = {
        name: os.environ[name] if name in os.environ else file_values[name]
        for name in declared_names
        if name in os.environ or name in file_values
    }
    _log_resolved(from_shell, "the shell environment")
    _log_resolved(result.keys() - from_shell, f"env file '{env_file.path}'")
    missing = declared_names - result.keys()
    if missing:
        _warn_about_missing(
            missing, f"the shell environment or env file '{env_file.path}'"
        )
    return result
