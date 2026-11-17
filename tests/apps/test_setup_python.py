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

import json
from unittest.mock import patch

import pytest
import yaml
from snowflake.cli._plugins.apps.generate import (
    _generate_app_yml,
    detect_python_project,
    hybrid_app_hint,
)

from tests_common import change_directory


@pytest.fixture(autouse=True)
def no_existing_app_service():
    with patch(
        "snowflake.cli._plugins.apps.commands._app_service_exists",
        return_value=False,
    ):
        yield


class TestDetectPythonProject:
    def test_no_python_files_is_not_detected(self, tmp_path):
        assert detect_python_project(tmp_path) is None

    def test_package_json_alone_is_not_python(self, tmp_path):
        (tmp_path / "package.json").write_text("{}")
        assert detect_python_project(tmp_path) is None

    def test_package_json_with_python_files_stays_node(self, tmp_path):
        (tmp_path / "package.json").write_text("{}")
        (tmp_path / "pyproject.toml").write_text("[project]\n")
        project = detect_python_project(tmp_path)
        assert project is not None
        assert project.node is True
        assert project.runtime == "node"

    @pytest.mark.parametrize("name", ["requirements.txt", "pyproject.toml"])
    def test_dependency_file_marks_python(self, tmp_path, name):
        (tmp_path / name).write_text("flask\n")
        project = detect_python_project(tmp_path)
        assert project is not None
        assert project.streamlit is False
        assert project.node is False
        assert project.runtime == "python"

    @pytest.mark.parametrize(
        "name,content",
        [
            ("requirements.txt", "streamlit\n"),
            ("requirements.txt", "# deps\nStreamlit[snowflake]>=1.40\n"),
            ("pyproject.toml", '[project]\ndependencies = ["streamlit>=1.40"]\n'),
        ],
    )
    def test_streamlit_dependency(self, tmp_path, name, content):
        (tmp_path / name).write_text(content)
        project = detect_python_project(tmp_path)
        assert project is not None and project.streamlit

    @pytest.mark.parametrize(
        "name,content",
        [
            ("requirements.txt", "streamlit-aggrid\n"),
            ("requirements.txt", "# streamlit\nflask\n"),
            ("pyproject.toml", '[project]\ndependencies = ["streamlit-aggrid"]\n'),
        ],
    )
    def test_streamlit_lookalikes_are_not_streamlit(self, tmp_path, name, content):
        (tmp_path / name).write_text(content)
        project = detect_python_project(tmp_path)
        assert project is not None and not project.streamlit

    @pytest.mark.parametrize(
        "pyproject",
        [
            '[project]\nname = "my-streamlit-app"\ndependencies = ["flask"]\n',
            '[project]\nkeywords = ["streamlit"]\ndependencies = ["flask"]\n',
            '[project]\ndependencies = ["flask"]\n'
            '[project.optional-dependencies]\nui = ["streamlit"]\n',
        ],
    )
    def test_pyproject_streamlit_outside_dependencies_is_not_streamlit(
        self, tmp_path, pyproject
    ):
        (tmp_path / "pyproject.toml").write_text(pyproject)
        project = detect_python_project(tmp_path)
        assert project is not None and not project.streamlit

    def test_streamlit_in_requirements_counts_next_to_a_pyproject(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text(
            '[project]\ndependencies = ["flask"]\n'
        )
        (tmp_path / "requirements.txt").write_text("streamlit\n")
        project = detect_python_project(tmp_path)
        assert project is not None and project.streamlit

    @pytest.mark.parametrize(
        "line, expected",
        [
            ("STREAMLIT==1.40\n", True),
            ("streamlit ; python_version > '3.9'\n", True),
            ("streamlit @ https://example.com/streamlit.whl\n", True),
            ("streamlit-aggrid\n", False),
            ("-r streamlit.txt\n", False),
        ],
    )
    def test_requirement_name_is_matched_after_normalization(
        self, tmp_path, line, expected
    ):
        (tmp_path / "requirements.txt").write_text(line)
        project = detect_python_project(tmp_path)
        assert project is not None and project.streamlit is expected

    def test_oversized_dependency_file_is_not_read(self, tmp_path):
        (tmp_path / "requirements.txt").write_text("streamlit\n" + "#" * 1_100_000)
        project = detect_python_project(tmp_path)
        assert project is not None and not project.streamlit

    @pytest.mark.parametrize(
        "pyproject",
        [
            "[project\n",
            'project = "streamlit"\n',
            "[project]\ndependencies = 5\n",
            '[project]\ndependencies = "streamlit"\n',
        ],
    )
    def test_malformed_pyproject_is_python_without_streamlit(self, tmp_path, pyproject):
        (tmp_path / "pyproject.toml").write_text(pyproject)
        project = detect_python_project(tmp_path)
        assert project is not None and not project.streamlit


class TestGenerateAppYmlPython:
    _RESOLVED = {"database": "DB", "schema": "S", "warehouse": "WH"}

    def _generate(self, tmp_path, requirements):
        (tmp_path / "requirements.txt").write_text(requirements)
        return yaml.safe_load(
            _generate_app_yml(
                "my_app",
                self._RESOLVED,
                use_workspace=False,
                python_project=detect_python_project(tmp_path),
            )
        )

    def test_streamlit_manifest_declares_runtime_without_commands(self, tmp_path):
        raw = self._generate(tmp_path, "streamlit\n")
        assert raw["runtime"] == "python"
        assert raw["streamlit"] is True
        assert raw["compute_resource"] == "SERVERLESS"
        for phase in ("install", "build", "run", "dev"):
            assert phase not in raw

    @pytest.mark.parametrize("streamlit", [True, False])
    def test_package_json_manifest_stays_node_with_hybrid_hint(
        self, tmp_path, streamlit
    ):
        from snowflake.cli._plugins.apps.app_yml import AppYmlDefinition

        (tmp_path / "package.json").write_text("{}")
        (tmp_path / "requirements.txt").write_text(
            "streamlit\n" if streamlit else "fastapi\n"
        )
        text = _generate_app_yml(
            "my_app",
            self._RESOLVED,
            use_workspace=False,
            python_project=detect_python_project(tmp_path),
        )
        raw = yaml.safe_load(text)
        assert "runtime" not in raw and "streamlit" not in raw
        assert "compute_resource" not in raw
        assert "#" not in text
        model = AppYmlDefinition(**raw)
        assert model.runtime == "node"
        assert model.missing_python_phases() == []

    @pytest.mark.parametrize("streamlit", [True, False])
    def test_hybrid_hint_names_the_settings(self, streamlit):
        hint = hybrid_app_hint(streamlit)
        assert "`runtime: [node, python]`" in hint
        assert "`compute_resource: SERVERLESS`" in hint
        assert ("`streamlit: true`" in hint) is streamlit
        assert "`install`, `build` and `run`" in hint

    def test_non_streamlit_manifest_omits_streamlit_key(self, tmp_path):
        raw = self._generate(tmp_path, "fastapi\n")
        assert raw["runtime"] == "python"
        assert "streamlit" not in raw

    def test_node_manifest_unchanged(self):
        raw = yaml.safe_load(
            _generate_app_yml("my_app", self._RESOLVED, use_workspace=False)
        )
        assert "runtime" not in raw
        assert "streamlit" not in raw
        assert "compute_resource" not in raw


class TestSetupPythonDetection:
    @staticmethod
    def _defaults(mock_mgr_cls):
        mock_mgr_cls.return_value.fetch_app_service_defaults.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_python_project_writes_runtime_and_warns_about_commands(
        self, mock_mgr_cls, runner, tmp_path
    ):
        self._defaults(mock_mgr_cls)
        (tmp_path / "requirements.txt").write_text("streamlit\n")
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "runtime: python" in result.output
            assert "Declare `run` in app.yml" in result.output
            written = yaml.safe_load((tmp_path / "app.yml").read_text())
        assert written["runtime"] == "python"
        assert written["streamlit"] is True
        assert "install" not in written and "run" not in written

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_dry_run_reports_python_runtime_without_writing(
        self, mock_mgr_cls, runner, tmp_path
    ):
        self._defaults(mock_mgr_cls)
        (tmp_path / "pyproject.toml").write_text("[project]\n")
        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "setup", "--app-name", "my_app", "--dry-run"]
            )
            assert result.exit_code == 0, result.output
            assert "runtime: python" in result.output
            assert not (tmp_path / "app.yml").exists()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_package_json_project_stays_node_and_explains_hybrid(
        self, mock_mgr_cls, runner, tmp_path
    ):
        self._defaults(mock_mgr_cls)
        (tmp_path / "package.json").write_text("{}")
        (tmp_path / "requirements.txt").write_text("fastapi\n")
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "deploys as Node" in result.output
            written = yaml.safe_load((tmp_path / "app.yml").read_text())
        assert "runtime" not in written

    @pytest.mark.parametrize(
        "files, runtime, streamlit",
        [
            ({}, "node", False),
            ({"requirements.txt": "streamlit\n"}, "python", True),
            ({"requirements.txt": "fastapi\n"}, "python", False),
            ({"package.json": "{}", "requirements.txt": "streamlit\n"}, "node", False),
        ],
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_json_result_reports_written_runtime(
        self, mock_mgr_cls, runner, tmp_path, files, runtime, streamlit
    ):
        self._defaults(mock_mgr_cls)
        for name, content in files.items():
            (tmp_path / name).write_text(content)
        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "setup", "--app-name", "my_app", "--format", "json"]
            )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["runtime"] == runtime
        assert payload["streamlit"] is streamlit

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_node_project_has_no_runtime_or_warning(
        self, mock_mgr_cls, runner, tmp_path
    ):
        self._defaults(mock_mgr_cls)
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "Python apps have no default commands" not in result.output
            assert "runtime" not in (tmp_path / "app.yml").read_text()
