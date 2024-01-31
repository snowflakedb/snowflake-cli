from pathlib import Path
from unittest import TestCase, mock
from unittest.mock import patch

from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.project.fixtures import *
from tests.testing_utils.fixtures import *


def mock_is_file_for(*known_files):
    def fake_is_file(self):
        return str(self) in known_files

    return mock.patch.object(Path, "is_file", autospec=True, side_effect=fake_is_file)


class DefinitionManagerTest(TestCase):
    exception_message = "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_no_project_parameter_provided(self, mock_getcwd):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            definition_manager = DefinitionManager()
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_finds_local_project_definition(self, mock_getcwd):
        with mock_is_file_for(
            "/hello/world/snowflake.yml", "/hello/world/snowflake.local.yml"
        ) as mock_is_file:
            definition_manager = DefinitionManager()
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml"),
                Path("/hello/world/snowflake.local.yml"),
            ]

    @mock.patch("os.path.abspath", return_value="/hello/world/test")
    def test_double_dash_project_parameter_provided(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            with pytest.raises(MissingConfiguration):
                DefinitionManager("/hello/world/test")

    @mock.patch("os.path.abspath", return_value="/hello/world/test/again")
    def test_dash_p_parameter_provided(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            with pytest.raises(MissingConfiguration):
                DefinitionManager("/hello/world/test/again")

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch("os.path.abspath", return_value="/hello/world/relative")
    def test_dash_p_with_relative_parameter_provided(self, mock_abs, mock_getcwd):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            mock_getcwd.return_value = "/hello/world"
            with pytest.raises(MissingConfiguration):
                DefinitionManager("./relative")

    @mock.patch("os.path.abspath", return_value="/tmp")
    def test_find_definition_files_reached_root(self, mock_abs):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            with pytest.raises(Exception) as exception:
                definition_manager = DefinitionManager("/tmp")
                assert definition_manager.project_root is None
            assert str(exception.value) == self.exception_message

    @mock.patch("os.path.abspath", return_value="/usr/user1/project")
    @mock.patch("pathlib.Path.home", return_value=Path("/usr/user1"))
    def test_find_definition_files_reached_home(self, mock_abs, path_home):
        with mock_is_file_for("/hello/world/snowflake.yml") as mock_is_file:
            with pytest.raises(Exception) as exception:
                definition_manager = DefinitionManager("/usr/user1/project")
                assert definition_manager.project_root is None
            assert str(exception.value) == self.exception_message

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_requires_base_project_definition(self, mock_getcwd):
        # If only snowflake.local.yml is present, the project root is not found
        with mock_is_file_for("/hello/world/snowflake.local.yml") as mock_is_file:
            with pytest.raises(Exception) as exception:
                definition_manager = DefinitionManager()
                assert definition_manager.project_root is None
            assert str(exception.value) == self.exception_message

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
