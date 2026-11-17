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

import importlib
import importlib.metadata
import subprocess
import sys
from pathlib import Path


def install_test_plugin(plugin_path: Path) -> None:
    """Pip-install a local test plugin and drop importlib.metadata path caches.

    xdist workers often call ``entry_points()`` before a module-scoped install
    fixture runs. ``importlib.metadata.FastPath`` caches the site-packages
    listing, so a plugin installed mid-session stays invisible until that
    cache is cleared. ``importlib.invalidate_caches()`` does that on 3.12
    (PathFinder is on ``sys.meta_path``); on 3.10 it does not, so FastPath
    must be cleared explicitly.
    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", str(plugin_path)])
    importlib.invalidate_caches()
    fast_path = getattr(importlib.metadata, "FastPath", None)
    cache_clear = getattr(getattr(fast_path, "__new__", None), "cache_clear", None)
    if cache_clear is not None:
        cache_clear()
