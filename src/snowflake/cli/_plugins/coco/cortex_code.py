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

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import typer
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError

CORTEX_CODE_BINARY_NAME = "cortex"
SUPPORTED_PLATFORMS = ("Darwin", "Linux")
INSTALL_SCRIPT_URL = "https://ai.snowflake.com/static/cc-scripts/install.sh"


def _get_install_dir() -> Path:
    return Path.home() / ".local" / "share" / "cortex"


def _get_bin_dir() -> Path:
    return Path.home() / ".local" / "bin"


def _get_binary_path() -> Path:
    return _get_bin_dir() / CORTEX_CODE_BINARY_NAME


def _find_cortex_code_binary() -> Optional[str]:
    path = shutil.which(CORTEX_CODE_BINARY_NAME)
    if path:
        return path

    local_path = _get_binary_path()
    if local_path.exists():
        return str(local_path)

    return None


def _download_cortex_code() -> str:
    cli_console.step("Installing Cortex Code CLI...")

    env = os.environ.copy()
    env["NON_INTERACTIVE"] = "1"
    env["SKIP_PODMAN"] = "1"
    env["SKIP_PATH_PROMPT"] = "1"

    channel = os.environ.get("CORTEX_CHANNEL")
    if channel:
        env["CORTEX_CHANNEL"] = channel

    try:
        result = subprocess.run(
            ["sh", "-c", f"curl -fsSL {INSTALL_SCRIPT_URL} | sh"],
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout or "Unknown error"
            raise CliError(f"Failed to install Cortex Code CLI: {error_msg}")

    except FileNotFoundError:
        raise CliError("curl is required to install Cortex Code CLI")

    local_path = _get_binary_path()
    if not local_path.exists():
        raise CliError(
            "Cortex Code CLI installation completed but binary not found at expected location"
        )

    cli_console.step("Cortex Code CLI installed successfully")
    return str(local_path)


def remove_cortex_code() -> None:
    install_dir = _get_install_dir()
    bin_path = _get_binary_path()

    removed_something = False

    if bin_path.exists() or bin_path.is_symlink():
        bin_path.unlink()
        removed_something = True

    if install_dir.exists():
        shutil.rmtree(install_dir)
        removed_something = True

    if not removed_something:
        raise CliError(
            "Failed to remove Cortex Code CLI: Cortex Code CLI not installed through `snow`"
        )


def _check_platform_supported() -> None:
    current_platform = platform.system()
    if current_platform not in SUPPORTED_PLATFORMS:
        raise CliError(
            f"Cortex Code CLI is not supported on {current_platform}. "
            f"Supported platforms: {', '.join(SUPPORTED_PLATFORMS)}"
        )


def run_cortex_code(args: List[str], remove: bool = False) -> int:
    _check_platform_supported()

    if remove:
        if args:
            raise CliError("Cannot use --remove with args")
        remove_cortex_code()
        cli_console.step("Cortex Code CLI removed successfully")
        return 0

    cortex_path = _find_cortex_code_binary()

    if not cortex_path:
        if sys.stdin.isatty():
            confirmed = typer.confirm(
                "Cortex Code CLI is not installed. Would you like to install it?",
                default=True,
            )
            if not confirmed:
                cli_console.warning("Cortex Code CLI was not installed")
                return 1
        elif not os.environ.get("CI"):
            cli_console.warning("Cortex Code CLI not installed")
            return 1

        cortex_path = _download_cortex_code()

    result = subprocess.run([cortex_path] + args)
    return result.returncode
