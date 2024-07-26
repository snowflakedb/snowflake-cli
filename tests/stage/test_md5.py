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


from snowflake.cli.plugins.stage.md5 import compute_md5sum

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
