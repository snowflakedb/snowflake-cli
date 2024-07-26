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
import os.path
import re
from pathlib import Path
from typing import List, Tuple

from click.exceptions import ClickException
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath

EST_CHUNK_GRANULARITY_BYTES = 1024**2
READ_BUFFER_BYTES = 8192
MD5SUM_REGEX = r"^[A-Fa-f0-9]{32}$"
MULTIPART_MD5SUM_REGEX = r"^([A-Fa-f0-9]{32})-(\d+)$"


class UnknownMD5FormatError(ClickException):
    def __init__(self, md5: str):
        super().__init__(f"Unknown md5 format: {md5}")


def is_md5sum(checksum: str) -> bool:
    """
    Could the provided hexadecimal checksum represent a valid md5sum?
    """
    return re.match(MD5SUM_REGEX, checksum) is not None


def get_multipart_md5sum(checksum: str) -> Tuple[str, int] | None:
    """
    Does this represent a multi-part md5sum (i.e. "<md5>-<n>")?
    If so, returns the tuple (md5, n), otherwise None.
    """
    multipart_md5 = re.match(MULTIPART_MD5SUM_REGEX, checksum)
    if multipart_md5:
        return (multipart_md5.group(1), int(multipart_md5.group(2)))
    return None


def compute_md5sum(file: Path, chunk_size: int | None = None) -> str:
    """
    Returns a hexadecimal checksum for the file located at the given path.
    If chunk_size is given, computes a multi-part md5sum.
    """
    if not file.is_file():
        raise ValueError(
            "The provided file does not exist or not a (symlink to a) regular file"
        )

    # If the stage uses SNOWFLAKE_FULL encryption, this will fail to provide
    # a matching md5sum, even when the underlying file is the same, as we do
    # not have access to the encrypted file under checksum.

    file_size = os.path.getsize(file)
    with SecurePath(file).open("rb", read_file_limit_mb=UNLIMITED) as f:
        md5s: List[bytes] = []
        hasher = hashlib.md5()

        remains = file_size
        remains_in_chunk: int = min(chunk_size, remains) if chunk_size else remains
        while remains > 0:
            sz = min(READ_BUFFER_BYTES, remains_in_chunk)
            buf = f.read(sz)
            hasher.update(buf)
            remains_in_chunk -= sz
            remains -= sz
            if remains_in_chunk == 0:
                # push the md5 of this chunk + reset
                md5s.append(hasher.hexdigest().encode())
                hasher = hashlib.md5()

        if not chunk_size:
            # simple md5; only one chunk processed
            return md5s[0].decode()

        # multi-part hash (e.g. aws)
        n = len(md5s)
        for md5 in md5s:
            hasher.update(md5)

        return f"{hasher.hexdigest()}-{n}"


def file_matches_md5sum(local_file: Path, remote_md5: str | None) -> bool:
    if not remote_md5:
        # no hash available
        return False

    if is_md5sum(remote_md5):
        # regular hash
        return compute_md5sum(local_file) == remote_md5

    if multipart_md5 := get_multipart_md5sum(remote_md5):
        # multi-part hash (e.g. aws)
        (_, num_chunks) = multipart_md5
        file_size = os.path.getsize(local_file)
        granules = 1 + ((file_size - 1) // (num_chunks * EST_CHUNK_GRANULARITY_BYTES))
        chunk_size = granules * EST_CHUNK_GRANULARITY_BYTES
        return compute_md5sum(local_file, chunk_size) == multipart_md5

    raise UnknownMD5FormatError(remote_md5)
