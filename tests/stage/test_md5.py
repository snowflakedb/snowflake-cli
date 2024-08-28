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

import math
from pathlib import Path
from typing import List, Tuple
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.md5 import (
    ONE_MEGABYTE,
    UnknownMD5FormatError,
    compute_md5sum,
    file_matches_md5sum,
)
from snowflake.connector.constants import S3_CHUNK_SIZE, S3_MAX_PARTS, S3_MIN_PART_SIZE

from tests.testing_utils.files_and_dirs import temp_local_dir


def test_empty_md5sum():
    with temp_local_dir({"empty.txt": ""}) as root:
        assert compute_md5sum(root / "empty.txt") == "d41d8cd98f00b204e9800998ecf8427e"


def test_simple_md5sum():
    with temp_local_dir({"README.md": "12345678"}) as root:
        assert compute_md5sum(root / "README.md") == "25d55ad283aa400af464c76d713c07ad"


def test_multipart_md5sum():
    with temp_local_dir(
        {"README.md": "This is a test. This is a test. This is a test."}
    ) as root:
        # N.B. 8 byte chunk size is not realistic, but we need to cap size of test data
        assert (
            compute_md5sum(root / "README.md", 8)
            == "47754cc91d4369081c0153ef0cb86675-6"
        )


@pytest.mark.parametrize(
    "remote_md5, file_size, chunk_size_and_md5, expected",
    [
        # no remote file (e.g. Azure)
        (None, None, None, False),
        # unsupported md5sum format
        ("badmd5", ONE_MEGABYTE, None, UnknownMD5FormatError),
        # empty w/ correct md5sum
        (
            "d41d8cd98f00b204e9800998ecf8427e",
            0,
            [(None, "d41d8cd98f00b204e9800998ecf8427e")],
            True,
        ),
        # standard md5sum
        (
            "00001111222233334444555566667777",
            55,
            [
                (
                    None,
                    "abcd1111222233334444555566667777",
                )
            ],
            False,
        ),
        # standard, but incorrect md5sum
        (
            "05921111222233334444555566667777",
            55,
            [
                (
                    None,
                    "badmd5",
                )
            ],
            False,
        ),
        # multi-part md5sum w/ default chunk size
        # we check the default S3 chunk size first
        (
            "00001111222233334444555566667777-2",
            math.ceil(S3_CHUNK_SIZE * 1.4),
            [(S3_CHUNK_SIZE, "00001111222233334444555566667777-2")],
            True,
        ),
        # multi-part md5sum w/ 2mb chunk size
        # we check the default S3 chunk size first,
        # then fall back to 1mb-aligned chunk size
        (
            "00001111222233334444555566667777-6",
            math.ceil(S3_CHUNK_SIZE * 1.4),
            [
                (S3_CHUNK_SIZE, "badmd5-2"),
                (2 * ONE_MEGABYTE, "00001111222233334444555566667777-6"),
            ],
            True,
        ),
        # multi-part md5sum w/ max parts + derived chunk size
        (
            f"00001111222233334444555566667777-{S3_MAX_PARTS}",
            S3_MAX_PARTS * S3_MIN_PART_SIZE * 2,
            [
                (
                    S3_MIN_PART_SIZE * 2,
                    f"00001111222233334444555566667777-{S3_MAX_PARTS}",
                )
            ],
            True,
        ),
        # multi-part, but incorrect md5sum
        (
            f"00001111222233334444555566667777-{S3_MAX_PARTS}",
            S3_MAX_PARTS * 50,
            [
                (max(50, S3_MIN_PART_SIZE), f"badmd5-{S3_MAX_PARTS}"),
                (S3_CHUNK_SIZE, f"badmd5-50"),
                (ONE_MEGABYTE, f"badmd5-400"),
            ],
            False,
        ),
    ],
)
@mock.patch("os.path.getsize")
@mock.patch("snowflake.cli._plugins.stage.md5.compute_md5sum")
def test_file_matches_md5sum(
    compute_md5sum: mock.NonCallableMock,
    getsize: mock.NonCallableMock,
    remote_md5: str | None,
    file_size: int | None,  # None if we don't expect it to be called
    chunk_size_and_md5: List[Tuple[int | None, str]] | None,
    expected: UnknownMD5FormatError | bool,
):
    local_file = mock.Mock(spec=Path)
    getsize.return_value = file_size

    def get_md5_for_chunk(_, requested_chunk_size: int | None = None):
        """Returns the test-configured md5 for a given chunk size"""
        if chunk_size_and_md5:
            for (chunk_size, md5) in chunk_size_and_md5:
                if chunk_size == requested_chunk_size:
                    return md5
            # passed in a chunk size we didn't expect
            assert requested_chunk_size in [
                chunk_size for (chunk_size, _) in chunk_size_and_md5
            ]
        else:
            # didn't expect any calls to compute_md5sum
            assert False

    compute_md5sum.side_effect = get_md5_for_chunk

    # actual test
    if isinstance(expected, bool):
        assert file_matches_md5sum(local_file, remote_md5) == expected
    else:
        with pytest.raises(expected):
            file_matches_md5sum(local_file, remote_md5)

    if chunk_size_and_md5 is None:
        compute_md5sum.assert_not_called()
    else:
        compute_md5sum.assert_has_calls(
            [
                mock.call(local_file, chunk_size)
                if chunk_size is not None
                else mock.call(local_file)
                for (chunk_size, _) in chunk_size_and_md5
            ]
        )
