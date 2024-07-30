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

from pathlib import Path
from typing import List, Tuple
from unittest import mock

import pytest
from snowflake.cli.plugins.stage.md5 import (
    UnknownMD5FormatError,
    compute_md5sum,
    file_matches_md5sum,
)

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
        (None, None, None, False),
        (
            "d41d8cd98f00b204e9800998ecf8427e",
            None,
            [(None, "d41d8cd98f00b204e9800998ecf8427e")],
            True,
        ),
    ],
)
@mock.patch("os.path.getsize")
@mock.patch("snowflake.cli.plugins.stage.md5.compute_md5sum")
def test_file_matches_md5sum(
    compute_md5sum: mock.NonCallableMock,
    getsize: mock.NonCallableMock,
    remote_md5: str | None,
    file_size: int | None,
    chunk_size_and_md5: List[Tuple[int | None, str]] | None,
    expected: UnknownMD5FormatError | bool,
):
    local_file = mock.Mock(spec=Path)
    getsize.return_value = file_size

    def get_md5_for_chunk(_, requested_chunk_size: int | None = None):
        if chunk_size_and_md5:
            for (chunk_size, md5) in chunk_size_and_md5:
                if chunk_size == requested_chunk_size:
                    return md5
        # passed in a chunk size we didn't expect
        assert False

    compute_md5sum.side_effect = get_md5_for_chunk

    # actual test
    if isinstance(expected, bool):
        assert file_matches_md5sum(local_file, remote_md5) == expected
    else:
        with pytest.raises(expected):
            file_matches_md5sum(local_file, remote_md5)

    if file_size is None:
        getsize.assert_not_called()
    else:
        getsize.assert_called_with(file_size)

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
