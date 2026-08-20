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

import logging
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

# Span a failed cleanup is attributed to. Cleanup runs in a ``finally`` after
# the command's own spans have closed, so without one the failure would be
# reported against no span at all.
CLEAN_UP_OUTPUT_SPAN = "snowflake_app.bundle.clean_up_output"

# Backoff between attempts of a retried delete. Windows refuses to unlink a file
# that another process holds open (POSIX allows it), and the holder is often a
# scanner or indexer that lets go of it within a moment.
_DELETE_RETRY_DELAYS_SECONDS = (0.1, 0.25, 0.5)

# A delete that still fails after the read-only bits were cleared and it was
# retried is almost always another process holding a file open, which only the
# user can resolve.
_LIKELY_HOLDERS = (
    "a program holding files in it (an editor, terminal, file explorer, "
    "antivirus or file-indexing scan)"
)


def _clear_readonly_bits(directory: Path) -> None:
    """Make *directory* and everything under it writable, ignoring failures.

    Called between attempts of a retried delete. A read-only file is the other
    common reason (besides an open handle) for Windows to refuse an unlink, and
    unlike a lock it will never clear on its own.
    """
    for target in (directory, *directory.rglob("*")):
        try:
            os.chmod(target, os.stat(target).st_mode | stat.S_IWRITE)
        except OSError:
            # Best effort: a path that cannot be chmod-ed will fail the retry
            # anyway, and the error from that attempt is the one worth showing.
            log.debug("Could not clear the read-only bit on %s", target, exc_info=True)


def _remove_directory(directory: SecurePath) -> None:
    """Delete *directory* and its contents, retrying on ``PermissionError``.

    The final error still propagates, so this never turns a failed delete into a
    silent success. It only gives a read-only file or a transient lock a chance
    to be dealt with first.
    """
    for delay in (*_DELETE_RETRY_DELAYS_SECONDS, None):
        try:
            directory.rmdir(recursive=True)
            return
        except PermissionError:
            if delay is None:
                raise
            log.debug(
                "Removing %s failed with PermissionError, retrying in %ss",
                directory.path,
                delay,
                exc_info=True,
            )
            _clear_readonly_bits(directory.path)
            time.sleep(delay)


@dataclass
class SnowflakeAppProjectPaths(ProjectPaths):
    """Paths of a Snowflake App Runtime project, and the cleanup of its bundle.

    Deleting the bundle directory is the part of bundling most likely to fail
    for reasons that have nothing to do with the app: the CLI does not control
    what else has the files open. On Windows an unlink is refused outright while
    a file is read-only or held by another process, so both deletes are retried
    here, and the one that runs after the command's work is finished is allowed
    to give up without failing the command.
    """

    def remove_up_bundle_root(self) -> None:
        """Delete the bundle directory so bundling starts from an empty one.

        Unlike :meth:`clean_up_output` this cannot be downgraded to a warning:
        bundling into a directory that still holds files from a previous run
        would put stale artifacts in the bundle and upload them. When the delete
        cannot be made to work, the error says which directory is in the way and
        what to do about it, rather than surfacing a bare ``PermissionError``.
        """
        if not self.bundle_root.exists():
            return
        try:
            _remove_directory(SecurePath(self.bundle_root))
        except OSError as e:
            raise CliError(
                f"Could not remove the bundle directory "
                f"'{sanitize_for_terminal(str(self.bundle_root))}': {e}. "
                f"Bundling cannot continue, because files left in it would be "
                f"included in the bundle. This is usually {_LIKELY_HOLDERS} — "
                f"close it, or delete the directory yourself, then run the "
                f"command again."
            ) from e

    def clean_up_output(self) -> None:
        """Delete the bundle directory the CLI created. Never raises.

        Only the bundle directory is deleted, and the directories above it only
        for as long as they are empty — so ``output`` still disappears when the
        CLI was the only thing using it, and survives when it is also where the
        project keeps its own build output.

        This runs once the command's real work has finished, so failing to
        delete a directory the CLI no longer needs must not fail the command: a
        single file held open by an editor or an antivirus scanner was enough to
        turn a completed ``snow app deploy`` into a failed one. The user is told
        which directory was left behind instead, and the failure is recorded on
        its own span so that it stays visible.
        """
        bundle_root = SecurePath(self.bundle_root)
        if bundle_root.exists():
            with get_cli_context().metrics.span(CLEAN_UP_OUTPUT_SPAN) as span:
                try:
                    _remove_directory(bundle_root)
                except OSError as e:
                    log.debug("Failed to clean up %s", bundle_root.path, exc_info=True)
                    span.finish(error=e)
                    cli_console.warning(
                        f"Could not remove the bundle directory "
                        f"'{sanitize_for_terminal(str(bundle_root.path))}': {e}. "
                        f"The command itself finished successfully. This is "
                        f"usually {_LIKELY_HOLDERS}; the directory can be "
                        f"deleted at any time."
                    )
                    return
        self._remove_empty_directories_up_to_project_root(self.bundle_root.parent)

    def _remove_empty_directories_up_to_project_root(self, directory: Path) -> None:
        """Remove *directory* and its parents while they are empty.

        Deleting only the bundle directory would leave the ``output`` directory
        the CLI created behind, which for most projects is the only thing in it.
        Anything the project itself put there stops the walk, since a non-empty
        directory is not the CLI's to delete.
        """
        while directory != self.project_root and self.project_root in directory.parents:
            try:
                directory.rmdir()
            except OSError:
                # Not empty, or not removable: either way this is as far up as
                # the CLI may go, and nothing here is worth reporting.
                log.debug("Leaving %s in place", directory, exc_info=True)
                return
            directory = directory.parent
