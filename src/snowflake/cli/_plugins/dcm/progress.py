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

import json
import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import PurePath
from typing import Callable, Dict, List, NoReturn, Optional, Sequence

from pydantic import BaseModel
from rich.console import RenderableType
from rich.table import Table
from rich.text import Text
from rich.tree import Tree
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepProgressUpdater,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class _DcmProjectProgressResult(BaseModel):
    """The parsed result of one ``SYSTEM$GET_DCM_PROJECT_PROGRESS`` poll."""

    phase: str = ""
    progress: int = 0


UPLOAD = StepDefinition("UPLOAD", "UPLOAD")
RENDER = StepDefinition("RENDER", "RENDER")
COMPILE = StepDefinition("COMPILE", "COMPILE")
PLAN = StepDefinition("PLAN", "PLAN")
DEPLOY = StepDefinition("DEPLOY", "DEPLOY")
PREVIEW = StepDefinition("PREVIEW", "PREVIEW")
REFRESH = StepDefinition("REFRESH", "REFRESH")
# The server reports the purge execution phase as "DEPLOY" in
# SYSTEM$GET_DCM_PROJECT_PROGRESS, so the key must match that string.
PURGE = StepDefinition("DEPLOY", "PURGE")
TEST = StepDefinition("TEST", "TEST")
ANALYZE = StepDefinition("ANALYZE", "ANALYZE")


class FileUploadProgress:
    """Drives a single progress step as a file-by-file upload counter."""

    def __init__(self, step: StepProgressUpdater, total: int) -> None:
        self._step = step
        self._done = 0
        self._step.start(total=total)

    def advance(self) -> None:
        self._done += 1
        self._step.update(completed=self._done)


DETAIL_BULLET = "• "
_TREE_HEADER = "Upload files"


def _bulleted(renderable: RenderableType) -> RenderableType:
    """Puts a bullet to the left of ``renderable``.

    Laid out as two cells rather than prefixed text, so a renderable spanning
    several rows keeps them all in the second cell - a tree's guides then line up
    under its title instead of under the bullet. A grid rather than ``Columns``,
    which sizes its columns to the console and would pad every row out to it.
    """
    row = Table.grid(padding=0)
    row.add_row(DETAIL_BULLET, renderable)
    return row


@dataclass(frozen=True)
class UploadFolder:
    """A folder of files to upload, with the count to display beside its name.

    ``_upload_folders`` only ever nests one level: a root folder's ``count`` is
    the files directly inside it, and each subfolder's ``count`` is everything
    beneath it.
    """

    name: str
    count: int
    subfolders: Sequence["UploadFolder"] = ()


def _folder_label(name: str, count: int) -> str:
    if not count:
        return name
    unit = "file" if count == 1 else "files"
    return f"{name} ({count} {unit})"


def _printable_filename(name: str) -> str:
    """A file or folder name reduced to a single printable row.

    ``sanitize_for_terminal`` only strips ANSI escapes, so a name may still
    carry a newline, a tab, a bidi override, or the surrogates ``pathlib``
    yields for undecodable bytes - each one breaking the row, colliding with
    another name, or failing to encode. Anything Python does not call printable
    is escaped as ``repr`` escapes it; printable names, accents and CJK
    included, pass through verbatim.
    """
    sanitized = sanitize_for_terminal(name) or ""
    return sanitized if sanitized.isprintable() else repr(sanitized)[1:-1]


def _detail_text(text: str) -> Text:
    """One line of detail, sanitized and kept to a single row.

    Built as ``Text`` so a name containing square brackets is rendered
    literally rather than read as rich markup, and so a name too long for the
    terminal is cropped instead of wrapping - a wrapped continuation would
    break the tree's guide alignment.
    """
    return Text(sanitize_for_terminal(text) or "", no_wrap=True, overflow="ellipsis")


def _add_folders(node: Tree, folders: Sequence[UploadFolder]) -> None:
    """Adds each folder as a branch, then its subfolders beneath it."""
    for folder in folders:
        label = _folder_label(_printable_filename(folder.name), folder.count)
        branch = node.add(_detail_text(label))
        _add_folders(branch, folder.subfolders)


def upload_tree(
    root_files: Sequence[str], folders: Sequence[UploadFolder]
) -> Optional[RenderableType]:
    """The files to upload, as a tree whose root is the heading line.

    Root files are named individually; folders carry their file count, nested
    as deeply as they are given. Rich picks the guide glyphs when this renders,
    falling back to ASCII where the output stream cannot encode them.
    """
    if not root_files and not folders:
        return None

    tree = Tree(_detail_text(_TREE_HEADER))
    for name in root_files:
        tree.add(_detail_text(_printable_filename(name)))
    _add_folders(tree, folders)

    return tree


_FAST_POLL_INTERVAL = 1
_SLOW_POLL_INTERVAL = 10
_FAST_POLL_THRESHOLD = 60
_NO_DATA_MAX_RETRY = 24


def _poll_interval_seconds(elapsed: float) -> int:
    """How long to wait between polls, backing off after a threshold."""
    if elapsed < _FAST_POLL_THRESHOLD:
        return _FAST_POLL_INTERVAL
    return _SLOW_POLL_INTERVAL


class ServerPoll:
    """Polls a DCM server operation and reflects its phases onto progress steps.

    Used by operations submitted asynchronously. It polls
    ``SYSTEM$GET_DCM_PROJECT_PROGRESS`` until the query finishes, drives the
    ``server_steps`` accordingly (a determinate bar once the reported progress
    is non-zero, an indeterminate spinner while it's still 0), then returns a
    cursor over the query results. The first step starts running immediately,
    so the display shows activity even before - or without - any progress
    report from the server.

    A long-running operation is waited on for as long as it takes; only a
    query whose status Snowflake never reports (``NO_DATA``) ends the wait,
    since without a status there is nothing to wait for.
    """

    def __init__(
        self,
        conn: SnowflakeConnection,
        progress: MultiStepProgress,
        server_steps: Sequence[StepDefinition],
        sfqid: str,
    ) -> None:
        self._conn = conn
        self._progress = progress
        self._server_steps = server_steps
        self._server_step_keys = frozenset(step.key for step in server_steps)
        self._sfqid = sfqid

    def run(self) -> SnowflakeCursor:
        start = time.monotonic()
        self._start_first_step()
        status = self._conn.get_query_status(self._sfqid)
        no_data_polls = 0
        while self._conn.is_still_running(status):
            if status == QueryStatus.NO_DATA:
                no_data_polls += 1
                if no_data_polls > _NO_DATA_MAX_RETRY:
                    self._abandon_unavailable_status()
            else:
                no_data_polls = 0
            self._update_from_poll(self._poll_progress())
            time.sleep(_poll_interval_seconds(time.monotonic() - start))
            status = self._conn.get_query_status(self._sfqid)

        if self._conn.is_an_error(status):
            self._finalize_failure()
        else:
            self._finalize_success()

        result_cursor = self._conn.cursor()
        result_cursor.get_results_from_sfqid(self._sfqid)
        return result_cursor

    def _abandon_unavailable_status(self) -> NoReturn:
        """Stops waiting on a query whose status Snowflake does not report.

        Polling cannot make progress without a status, so waiting longer would
        hang the CLI indefinitely on a query it may never learn anything about.
        """
        raise CliError(
            f"Snowflake reported no status for query {self._sfqid} in "
            f"{_NO_DATA_MAX_RETRY} consecutive checks, so its progress cannot be "
            "tracked. The operation may still be running - check the query in "
            "Snowsight."
        )

    def _start_first_step(self) -> None:
        """Marks the first server step running as soon as the query is submitted.

        The server reports nothing until its first progress poll lands - and
        may never report anything at all - so without this the whole checklist
        would sit pending, looking stalled, for the entire operation.
        """
        if self._server_steps:
            self._progress.start_step(self._server_steps[0].key)

    def _poll_progress(self) -> Optional[_DcmProjectProgressResult]:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    "SELECT SYSTEM$GET_DCM_PROJECT_PROGRESS(?)",
                    (self._sfqid,),
                    _force_qmark_paramstyle=True,
                )
                row = cursor.fetchone()
            if row and row[0]:
                return _DcmProjectProgressResult.model_validate(json.loads(row[0]))
        except Exception:  # noqa: BLE001
            log.debug(
                "Progress poll failed for sfqid=%s; will retry next cycle.",
                self._sfqid,
                exc_info=True,
            )
        return None

    def _update_from_poll(self, result: Optional[_DcmProjectProgressResult]) -> None:
        if result is None:
            return
        if result.phase not in self._server_step_keys:
            log.warning(
                "Ignoring progress poll with unknown phase %r (sfqid=%s).",
                result.phase,
                self._sfqid,
            )
            return

        found_current = False
        for step in self._server_steps:
            if step.key == result.phase:
                found_current = True
                if result.progress:
                    self._progress.start_step(step.key, total=100)
                    self._progress.update(step.key, completed=result.progress)
                else:
                    self._progress.start_step(step.key)
            elif (
                not found_current
                and not self._progress.step_state(step.key).is_terminal()
            ):
                self._progress.complete_step(step.key)

    def _finalize(self, mark_step: Callable[[str], None]) -> None:
        for step in self._server_steps:
            if not self._progress.step_state(step.key).is_terminal():
                mark_step(step.key)

    def _finalize_success(self) -> None:
        self._finalize(self._progress.complete_step)

    def _finalize_failure(self) -> None:
        self._finalize(self._progress.fail_step)


def upload_details(
    stage_fqn: FQN, relative_paths: Sequence[PurePath]
) -> List[RenderableType]:
    """Components shown beneath the upload step.

    Files in the project root are named individually. Deeper files are
    counted two levels deep at most: each root folder shows how many files
    sit directly inside it, and each of its direct subfolders shows how many
    files it holds in total, however deeply nested.
    """
    stage_scope = f" inside {stage_fqn.prefix}" if stage_fqn.prefix else ""
    root_files = sorted(str(rel) for rel in relative_paths if len(rel.parts) == 1)
    details: List[RenderableType] = [
        _detail_text(f"Create temporary stage{stage_scope}")
    ]
    tree = upload_tree(root_files, _upload_folders(relative_paths))
    if tree is not None:
        details.append(tree)
    return [_bulleted(detail) for detail in details]


def _upload_folders(relative_paths: Sequence[PurePath]) -> List[UploadFolder]:
    direct_counts: Counter = Counter()
    subfolder_counts: Dict[str, Counter] = defaultdict(Counter)
    for rel in relative_paths:
        folders = rel.parts[:-1]
        if not folders:
            continue
        if len(folders) == 1:
            direct_counts[folders[0]] += 1
        else:
            subfolder_counts[folders[0]][folders[1]] += 1

    return [
        UploadFolder(
            name=name,
            count=direct_counts[name],
            subfolders=[
                UploadFolder(name=subfolder, count=count)
                for subfolder, count in sorted(subfolder_counts[name].items())
            ],
        )
        for name in sorted(set(direct_counts) | set(subfolder_counts))
    ]
