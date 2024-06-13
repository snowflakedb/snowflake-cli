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

from unittest import mock

import pytest


@pytest.fixture(name="_patch_app_version_in_tests", autouse=True)
def mock_app_version_in_tests(request):
    """Set predefined Snowflake-CLI for testing.

    Marker `app_version_patch` can be used in tests to skip it.

    @pytest.mark.app_version_patch(False)
    def test_case():
        ...
    """
    marker = request.node.get_closest_marker("app_version_patch")

    if marker and marker.kwargs.get("use") is False:
        yield
    else:
        with mock.patch(
            "snowflake.cli.__about__.VERSION", "0.0.0-test_patched"
        ) as _fixture:
            yield _fixture
