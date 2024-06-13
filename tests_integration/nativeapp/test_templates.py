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

import pytest


@pytest.mark.integration
def test_list_templates_no_options_success(runner):
    args = ["app", "list-templates"]
    result = runner.invoke_json(args)

    assert result.exit_code == 0
    templates = result.json
    assert len(templates) > 0

    # Check that the basic templates are present, but explicitly avoid checking for an
    # exact list so that adding new templates won't break the tests.
    all_template_names = [t["template"] for t in templates]
    assert "basic" in all_template_names
    assert "streamlit-java" in all_template_names
    assert "streamlit-python" in all_template_names
