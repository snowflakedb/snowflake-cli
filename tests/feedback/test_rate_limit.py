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

from pathlib import Path
from unittest import mock

from snowflake.cli._plugins.feedback.constants import RATE_LIMIT_MAX
from snowflake.cli._plugins.feedback.rate_limit import check, record


def _patch_rate_dir(tmp_path: Path):
    fake_manager = mock.Mock()
    fake_manager.file_path = tmp_path / "config.toml"
    return mock.patch(
        "snowflake.cli._plugins.feedback.rate_limit.get_config_manager",
        return_value=fake_manager,
    )


def test_rate_limit_allows_five_then_blocks(tmp_path):
    with _patch_rate_dir(tmp_path):
        now = 1_700_000_000.0
        for i in range(RATE_LIMIT_MAX):
            assert check(now=now + i) is None
            record(now=now + i)
        retry = check(now=now + RATE_LIMIT_MAX)
        assert retry == 3595


def test_rate_limit_window_rolls(tmp_path):
    with _patch_rate_dir(tmp_path):
        start = 1_700_000_000.0
        for i in range(RATE_LIMIT_MAX):
            assert check(now=start + i) is None
            record(now=start + i)
        later = start + 3600 + 10
        assert check(now=later) is None


def test_rate_limit_ignores_corrupt_file(tmp_path):
    with _patch_rate_dir(tmp_path):
        (tmp_path / "feedback_rate.json").write_bytes(
            b'{"timestamps": [1.0, \xff\xfe]}'
        )
        assert check(now=1_700_000_000.0) is None
