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

"""Skip-guard for the ``snow feature`` plugin test suite.

The plugin is a thin adapter over the optional
``snowflake-ml-python[feature_store]`` library.  ``snowflake-cli`` does
not depend on that library, so most tests here can only run when it is
installed.  This conftest detects the library once and skips the
dependency-requiring feature tests when it is absent, while leaving the
no-dependency behavior tests (plugin registration) live so the base
suite still proves the plugin registers and errors cleanly without the
library.
"""

from pathlib import Path

import pytest

try:
    import snowflake.ml.feature_store.decl  # noqa: F401

    _SNOWML_AVAILABLE = True
except ImportError:
    _SNOWML_AVAILABLE = False

# Feature tests that intentionally validate no-dependency behavior and
# therefore must run even when the library is not installed.
# ``test_session_cache.py`` exercises only the CLI-owned Snowpark Session
# reuse (``_build_session`` -> ``SqlExecutionMixin.snowpark_session``); the
# manager wraps its ``decl_api`` import in try/except, so this test does not
# need the ML library and must run in the base suite too.
_NO_DEP_SAFE = {
    "test_plugin_registration.py",
    "test_models.py",
    "test_session_cache.py",
}
_HERE = Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    """Skip dependency-requiring feature tests when the library is absent."""
    if _SNOWML_AVAILABLE:
        return
    skip = pytest.mark.skip(reason="requires snowflake-ml-python[feature_store]")
    for item in items:
        path = Path(str(item.fspath))
        if _HERE in path.parents and path.name not in _NO_DEP_SAFE:
            item.add_marker(skip)
