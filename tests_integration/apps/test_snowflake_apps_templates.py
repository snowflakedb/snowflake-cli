# Copyright (c) 2026 Snowflake Inc.
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

"""Integration tests for ``snow app templates`` and ``snow app init``.

These read the real template catalog from GitHub, at the templates release tag,
and resolve where the app deploys on the test account. Fetching, checking, and
filling in templates is covered by unit tests in ``tests/apps/test_templates.py``.
"""

import pytest
import yaml


@pytest.mark.integration
def test_templates_lists_the_catalog(runner):
    result = runner.invoke_json(["app", "templates"])

    assert result.exit_code == 0, result.output
    ids = [template["id"] for template in result.json]
    assert "nextjs" in ids
    assert [template["default"] for template in result.json].count(True) == 1


@pytest.mark.integration
def test_init_creates_a_project_from_the_catalog(runner, temporary_working_directory):
    result = runner.invoke_with_connection_json(
        ["app", "init", "templated_app", "--template", "nextjs"]
    )

    assert result.exit_code == 0, result.output
    assert result.json["template"] == "nextjs"
    project = temporary_working_directory / "templated_app"
    app_yml_text = (project / "app.yml").read_text()
    assert app_yml_text.splitlines()[0] == (
        "# created-by: snow-app-init; template=nextjs; ref=templates-v1; "
        f"commit={result.json['commit']}"
    )
    app_yml = yaml.safe_load(app_yml_text)
    assert app_yml["name"] == "TEMPLATED_APP"
    assert app_yml["database"] == result.json["database"]
    assert app_yml["query_warehouse"] == result.json["warehouse"]
    assert (project / "package.json").is_file()
