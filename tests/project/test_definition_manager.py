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
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import pytest
from click import ClickException
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.utils.models import ProjectEnvironment

from tests.test_data.test_data import definition_v2_duplicated_entity_names


def mock_is_file_for(*known_files):
    def fake_is_file(self):
        return str(self) in [str(Path(f)) for f in known_files]

    return mock.patch.object(Path, "is_file", autospec=True, side_effect=fake_is_file)


class DefinitionManagerTest(TestCase):
    exception_message = "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_no_project_parameter_provided(self, mock_getcwd):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager()
            assert definition_manager._project_config_paths == [  # noqa: SLF001
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_finds_local_project_definition(self, mock_getcwd):
        with mock_is_file_for(
            "/hello/world/snowflake.yml", "/hello/world/snowflake.local.yml"
        ) as mock_is_file:
            definition_manager = DefinitionManager()
            assert definition_manager._project_config_paths == [  # noqa: SLF001
                Path("/hello/world/snowflake.yml"),
                Path("/hello/world/snowflake.local.yml"),
            ]

    @mock.patch("os.path.abspath", return_value="/hello/world/test")
    def test_double_dash_project_parameter_provided(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager("/hello/world/test")
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/hello/world/test")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    @mock.patch("os.path.abspath", return_value="/hello/world/test/again")
    def test_dash_p_parameter_provided_no_snowflake_yml_found(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager("/hello/world/test/again")
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/hello/world/test/again")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch("os.path.abspath", return_value="/hello/world/relative")
    def test_dash_p_with_relative_parameter_provided_but_no_matching_project_definition(
        self, mock_abs, mock_getcwd
    ):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            mock_getcwd.return_value = "/hello/world"
            definition_manager = DefinitionManager("./relative")
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/hello/world/relative")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    @mock.patch("os.path.abspath", return_value="/tmp")
    def test_find_definition_files_under_root_folder(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager("/tmp")
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/tmp")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    @mock.patch("os.path.abspath", return_value="/usr/user1/project")
    @mock.patch("pathlib.Path.home", return_value=Path("/usr/user1"))
    def test_find_definition_under_home_folder(self, mock_abs, path_home):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager("/usr/user1/project")
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/usr/user1/project")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_base_project_definition_absent_but_user_project_definition_present(
        self, mock_getcwd
    ):
        # If only snowflake.local.yml is present, the project root is not found
        with mock_is_file_for("/hello/world/snowflake.local.yml") as mock_is_file:
            definition_manager = DefinitionManager()
            assert not definition_manager.has_definition_file
            assert definition_manager.project_root == Path("/hello/world")
            assert definition_manager.project_definition is None
            assert definition_manager.template_context == {
                "ctx": {"env": ProjectEnvironment(override_env={}, default_env={})}
            }

    def test_find_project_root(self):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            project_root = DefinitionManager.find_project_root(
                Path("/hello/world/relative/search/dir")
            )
            assert project_root == Path("/hello/world")

    @mock.patch("os.path.abspath", return_value="/usr/user1/project")
    @mock.patch("pathlib.Path.home", return_value=Path("/usr/user1"))
    def test_find_project_root_stops_at_home(self, mock_abs, path_home):
        with mock_is_file_for("/usr/snowflake.yml") as mock_is_file:
            assert (
                DefinitionManager.find_project_root(Path("/usr/user1/project")) is None
            )

    @mock.patch("os.path.abspath", return_value="/tmp")
    def test_find_project_root_stops_at_fs_root(self, mock_abs):
        with mock_is_file_for("/hello/work/snowflake.yml") as mock_is_file:
            assert DefinitionManager.find_project_root(Path("/tmp")) is None


def test_loading_yaml_with_duplicated_keys_raises_an_error():
    with pytest.raises(ClickException) as err:
        with TemporaryDirectory() as tmpdir:
            definition_file = Path(tmpdir) / "snowflake.yml"
            definition_file.write_text(definition_v2_duplicated_entity_names)
            _ = DefinitionManager(tmpdir).project_definition

    assert (
        "While loading the project definition file, duplicate key was found: hello_world"
        in err.value.message
    )
