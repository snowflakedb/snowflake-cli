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
import math
import os.path
import re
from pathlib import Path
from typing import List, Tuple

from click.exceptions import ClickException
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.constants import S3_CHUNK_SIZE, S3_MAX_PARTS, S3_MIN_PART_SIZE

ONE_MEGABYTE = 1024**2
READ_BUFFER_BYTES = 64 * 1024
MD5SUM_REGEX = r"^[A-Fa-f0-9]{32}$"
MULTIPART_MD5SUM_REGEX = r"^([A-Fa-f0-9]{32})-(\d+)$"

log = logging.getLogger(__name__)


class UnknownMD5FormatError(ClickException):
    def __init__(self, md5: str):
        super().__init__(f"Unknown md5 format: {md5}")


def is_md5sum(checksum: str) -> bool:
    """
    Could the provided hexadecimal checksum represent a valid md5sum?
    """
    return re.match(MD5SUM_REGEX, checksum) is not None


def parse_multipart_md5sum(checksum: str) -> Tuple[str, int] | None:
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
    if file_size == 0:
        # simple md5 with no content
        return hashlib.md5().hexdigest()

    with SecurePath(file).open("rb", read_file_limit_mb=UNLIMITED) as f:
        md5s: List[hashlib._Hash] = []  # noqa: SLF001
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
                if not chunk_size:
                    # simple md5; only one chunk processed
                    return hasher.hexdigest()
                else:
                    # push the hash of this chunk + reset
                    md5s.append(hasher)
                    hasher = hashlib.md5()
                    remains_in_chunk = min(chunk_size, remains)

        # multi-part hash (e.g. aws)
        digests = b"".join(m.digest() for m in md5s)
        digests_md5 = hashlib.md5(digests)
        return f"{digests_md5.hexdigest()}-{len(md5s)}"


def file_matches_md5sum(local_file: Path, remote_md5: str | None) -> bool:
    """
    Try a few different md5sums to determine if a local file is identical
    to a file that has a given remote md5sum.

    Handles the multi-part md5sums generated by e.g. AWS S3, using values
    from the Python connector to make educated guesses on chunk size.

    Assumes that upload time would dominate local hashing time.
    """
    if not remote_md5:
        # no hash available
        return False

    if is_md5sum(remote_md5):
        # regular hash
        return compute_md5sum(local_file) == remote_md5

    if md5_and_chunks := parse_multipart_md5sum(remote_md5):
        # multi-part hash (e.g. aws)
        (_, num_chunks) = md5_and_chunks
        file_size = os.path.getsize(local_file)

        # If this file uses the maximum number of parts supported by the cloud backend,
        # the chunk size is likely not a clean multiple of a megabyte. Try reverse engineering
        # from the file size first, then fall back to the usual detection method.
        # At time of writing this logic would trigger for files >= 80GiB (python connector)
        if num_chunks == S3_MAX_PARTS:
            chunk_size = max(math.ceil(file_size / S3_MAX_PARTS), S3_MIN_PART_SIZE)
            if compute_md5sum(local_file, chunk_size) == remote_md5:
                return True

        # Estimates the chunk size the multi-part file must have been uploaded with
        # by trying chunk sizes that give the most evenly-sized chunks.
        #
        # First we'll try the chunk size that's a multiple of S3_CHUNK_SIZE (8mb) from
        # the python connector that results in num_chunks, then we'll do the same with
        # a smaller granularity (1mb) that is used by default in some AWS multi-part
        # upload implementations.
        #
        # We're working backwards from num_chunks here because it's the only value we know.
        for chunk_size_alignment in [S3_CHUNK_SIZE, ONE_MEGABYTE]:
            # +1 because we need at least one chunk when file_size < num_chunks * chunk_size_alignment
            # -1 because we don't want to add an extra chunk when file_size is an exact multiple of num_chunks * chunk_size_alignment
            multiplier = 1 + ((file_size - 1) // (num_chunks * chunk_size_alignment))
            chunk_size = multiplier * chunk_size_alignment
            if compute_md5sum(local_file, chunk_size) == remote_md5:
                return True

        # we were unable to figure out the chunk size, or the files are different
        log.debug("multi-part md5: %s != %s", remote_md5, local_file)
        return False

    raise UnknownMD5FormatError(remote_md5)
