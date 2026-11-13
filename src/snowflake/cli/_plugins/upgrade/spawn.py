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

"""Spawn the version-dir binary after a successful auto-upgrade.

The shim on PATH has already been retargeted. This starts the binary inside
the version directory (``snow.exe`` on Windows) so we do not re-enter that
shim, and so we do not ``exec`` over a locked ``snow.exe``. User argv is
passed through unchanged. Stdio is inherited: the upgrade path must not read
stdin, and progress stays on the child's stderr.

After the child finishes, the parent ``os._exit``s with the child's status so
its ``finally`` handlers do not run a second time. A negative ``returncode``
(the child died from a signal) is ``128 + signal`` before ``_exit`` — passing
the negative code through would wrap to an 8-bit status (``-2`` becomes 254).
SIGINT is ignored here while waiting, and reset to the default in the child:
``SIG_IGN`` survives ``exec``, and a ``KeyboardInterrupt`` out of ``run``
would skip ``_exit``.

Spawn errors (missing or non-executable binary) propagate. The process-start
hook fail-opens on those and continues the command on the old binary, so
``finally`` does run on that path.
"""

from __future__ import annotations

import os
import signal
import subprocess
from typing import NoReturn, Sequence

from snowflake.cli._plugins.upgrade.controls import AUTO_UPGRADE_DONE_ENV
from snowflake.cli._plugins.upgrade.layout import ManagedLayout
from snowflake.cli.api.exceptions import CliError


def _exit_status(returncode: int) -> int:
    """Map a ``CompletedProcess.returncode`` to an ``os._exit`` status."""
    if returncode < 0:
        return 128 - returncode
    return returncode


def _default_sigint() -> None:
    """Undo a parent ``SIG_IGN`` before the child image starts.

    POSIX keeps ``SIG_IGN`` across ``exec``. Without this, Ctrl-C would not
    reach the version-dir binary.
    """
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def run_new_binary(version: str, argv: Sequence[str]) -> NoReturn:
    """Run ``<managed-home>/<version>/snow`` and exit with its status.

    ``argv`` is the original user arguments (``sys.argv[1:]``), not a new
    command line. Does not inject flags. Sets
    ``SNOWFLAKE_CLI_AUTO_UPGRADE_DONE=1`` on the child only.

    ``os._exit`` runs only after the child finishes. ``FileNotFoundError`` and
    ``PermissionError`` from ``subprocess.run`` propagate so the caller can
    fail open.
    """
    binary = ManagedLayout().binary_path(version)
    env = os.environ.copy()
    env[AUTO_UPGRADE_DONE_ENV] = "1"
    # Omit stdin/stdout/stderr so they are inherited, not pipes.
    # preexec_fn is POSIX-only. Windows console Ctrl+C is per-process, so the
    # child keeps the default handler without it.
    run_kwargs: dict = {"env": env}
    if os.name != "nt":
        run_kwargs["preexec_fn"] = _default_sigint
    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        completed = subprocess.run([str(binary), *argv], **run_kwargs)
        status = _exit_status(completed.returncode)
    except Exception:
        signal.signal(signal.SIGINT, previous_sigint)
        raise
    os._exit(status)


def warm_installed_binary(version: str) -> None:
    """Unpack the new binary before ``snow upgrade`` returns.

    The managed artifact is a PyApp stub. Upgrade extracts that stub and
    retargets the shim; PyApp unpacks the embedded distribution into its
    per-version install directory on first execution. Running the version-dir
    binary once here means the next ``snow`` command does not show
    ``Unpacking distribution``.

    The child is ``<version>/snow --no-auto-upgrade --version``, not the PATH
    shim and not the user's command. ``--version`` is a meta command, and
    ``SNOWFLAKE_CLI_AUTO_UPGRADE_DONE`` is set, so this does not start another
    upgrade. Stdout is discarded. Stderr is inherited so the unpack spinner
    stays on the upgrade's terminal.

    The shim already points at ``version`` when this runs. A start failure is
    reported and does not roll the shim back.
    """
    binary = ManagedLayout().binary_path(version)
    env = os.environ.copy()
    env[AUTO_UPGRADE_DONE_ENV] = "1"
    try:
        completed = subprocess.run(
            [str(binary), "--no-auto-upgrade", "--version"],
            env=env,
            stdout=subprocess.DEVNULL,
            check=False,
        )
    except (FileNotFoundError, PermissionError) as err:
        raise CliError(
            f"Upgraded to {version}. The new binary could not be started."
        ) from err
    if completed.returncode != 0:
        raise CliError(
            f"Upgraded to {version}. The new binary failed to start "
            f"(exit {_exit_status(completed.returncode)})."
        )
