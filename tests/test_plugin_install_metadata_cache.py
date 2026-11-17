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

import importlib.metadata

from tests_common.plugin_install import install_test_plugin


def test_install_test_plugin_clears_fastpath_cache(tmp_path, monkeypatch):
    recorded: list[list[str]] = []

    def fake_check_call(cmd, *args, **kwargs):
        recorded.append(list(cmd))

    monkeypatch.setattr(
        "tests_common.plugin_install.subprocess.check_call",
        fake_check_call,
    )

    # distributions() does not touch this cache once importlib_metadata
    # (installed on Python < 3.12) has deleted PathFinder.find_distributions.
    # Seed the stdlib FastPath that install_test_plugin clears.
    importlib.metadata.FastPath("fastpath-cache-seed")
    assert importlib.metadata.FastPath.__new__.cache_info().currsize > 0

    plugin_dir = tmp_path / "dummy-plugin"
    plugin_dir.mkdir()
    install_test_plugin(plugin_dir)

    assert recorded
    assert recorded[0][-1] == str(plugin_dir)
    assert importlib.metadata.FastPath.__new__.cache_info().currsize == 0
