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

import hashlib
import logging
import time
import uuid
from pathlib import Path
from typing import Optional, Union

from snowflake.cli._plugins.upgrade.layout import ManagedLayout
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

_MACHINE_ID_MODE = 0o600
# Exclusive create and the first write are two steps. A loser of O_EXCL must
# wait for the winner's UUID rather than overwrite the empty file.
_MACHINE_ID_WAIT_SECONDS = 1.0
_MACHINE_ID_POLL_SECONDS = 0.01


def get_or_create_machine_id(layout: Optional[ManagedLayout] = None) -> str:
    """Return the stable UUID stored at ``<managed-home>/.machine-id``.

    Created once (mode 0600). Used only to bucket rollout; never send the raw
    id in telemetry.
    """
    root = layout if layout is not None else ManagedLayout()
    path = root.machine_id_path
    existing = _read_machine_id(path)
    if existing is not None:
        return existing

    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    new_id = str(uuid.uuid4())
    try:
        SecurePath(path).touch(permissions_mask=_MACHINE_ID_MODE, exist_ok=False)
    except FileExistsError:
        return _adopt_existing_machine_id(path)
    return _overwrite_machine_id(path, new_id)


def rollout_bucket(machine_id: Union[str, uuid.UUID]) -> int:
    """Map a machine id to a stable 0–99 rollout bucket.

    ``int(sha256(id_bytes)[:8], 16) % 100`` so a machine stays in the 1% ring
    as Releng raises ``fraction``.
    """
    parsed = machine_id if isinstance(machine_id, uuid.UUID) else uuid.UUID(machine_id)
    digest = hashlib.sha256(parsed.bytes).hexdigest()
    return int(digest[:8], 16) % 100


def _adopt_existing_machine_id(path: Path) -> str:
    """Return the on-disk UUID after losing exclusive create.

    Do not overwrite: the winner may still be writing. After the wait, recover
    from a crashed winner or a corrupt file so the process can still bucket.
    """
    deadline = time.monotonic() + _MACHINE_ID_WAIT_SECONDS
    while True:
        existing = _read_machine_id(path)
        if existing is not None:
            return existing
        if time.monotonic() >= deadline:
            break
        time.sleep(_MACHINE_ID_POLL_SECONDS)
    return _overwrite_machine_id(path, str(uuid.uuid4()))


def _read_machine_id(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    try:
        text = SecurePath(path).read_text(
            file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        )
    except OSError:
        return None
    stripped = text.strip()
    try:
        return str(uuid.UUID(stripped))
    except ValueError:
        if stripped:
            log.debug("Ignoring unreadable machine id at %s", path)
        return None


def _overwrite_machine_id(path: Path, new_id: str) -> str:
    SecurePath(path).write_text(f"{new_id}\n", encoding="utf-8")
    SecurePath(path).chmod(_MACHINE_ID_MODE)
    return new_id
