from unittest import TestCase, mock
from unittest.mock import patch
from pathlib import Path
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *


from snowcli.cli.project.definition_manager import DefinitionManager


class DefinitionManagerTest(TestCase):
    exception_message = "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."

    def mock_definition_manager(self):
        return patch(
            "snowcli.cli.project.definition_manager.DefinitionManager._find_definition_files",
            return_value=[Path("/hello/world/snowflake.yml")],
        )

    def mock_base_definition_files(self):
        return patch(
            "snowcli.cli.project.definition_manager.DefinitionManager._base_definition_file_if_available",
            return_value=None,
        )

    @mock.patch("os.getcwd", return_value="/hello/world")
    def test_no_project_parameter_provided(self, mock_getcwd):
        with self.mock_definition_manager() as mock_config_files:
            definition_manager = DefinitionManager()
            mock_config_files.assert_called_with(Path("/hello/world"))
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.path.abspath", return_value="/hello/world/test")
    def test_double_dash_project_parameter_provided(self, mock_abs):
        with self.mock_definition_manager() as mock_config_files:
            definition_manager = DefinitionManager("/hello/world/test")
            mock_config_files.assert_called_with(Path("/hello/world/test"))
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.path.abspath", return_value="/hello/world/test/again")
    def test_dash_p_parameter_provided(self, mock_abs):
        with self.mock_definition_manager() as mock_config_files:
            definition_manager = DefinitionManager("/hello/world/test/again")
            mock_config_files.assert_called_with(Path("/hello/world/test/again"))
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.getcwd", return_value="/hello/world")
    @mock.patch("os.path.abspath", return_value="/hello/world/relative")
    def test_dash_p_with_relative_parameter_provided(self, mock_abs, mock_getcwd):
        with self.mock_definition_manager() as mock_config_files:
            mock_getcwd.return_value = "/hello/world"
            definition_manager = DefinitionManager("./relative")
            mock_abs.assert_called_with("./relative")
            mock_config_files.assert_called_with(Path("/hello/world/relative"))
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml")
            ]

    @mock.patch("os.path.abspath", return_value="/tmp")
    def test_find_definition_files_reached_root(self, mock_abs):
        with self.mock_base_definition_files():
            with pytest.raises(Exception) as exception:
                definition_manager = DefinitionManager("/tmp")
                assert definition_manager.project_root == None
            assert str(exception.value) == self.exception_message

    @mock.patch("os.path.abspath", return_value="/usr/user1/project")
    @mock.patch("pathlib.Path.home", return_value="/usr/user1")
    def test_find_definition_files_reached_home(self, path_home, mock_abs):
        with self.mock_base_definition_files():
            with pytest.raises(Exception) as exception:
                definition_manager = DefinitionManager("/usr/user1/project")
                assert definition_manager.project_root == None
            assert str(exception.value) == self.exception_message
