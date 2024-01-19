from pathlib import Path
from unittest import TestCase, mock

import pytest

from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.project.definition_manager import DefinitionManager

_DEFAULT_PROJECT_FILE = Path("snowflake.yml")


def mock_is_file_for(*known_files):
    def fake_is_file(self):
        return str(self) in known_files

    return mock.patch.object(Path, "is_file", autospec=True, side_effect=fake_is_file)


class DefinitionManagerTest(TestCase):
    exception_message = "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."

    def test_fails_if_project_file_is_not_present_in_root_dir_default(self):
        with pytest.raises(MissingConfiguration):
            DefinitionManager(project_root=Path("/hello/world"))

    def test_fails_if_project_file_is_not_present_in_root_dir_user_provided(self):
        with pytest.raises(MissingConfiguration):
            DefinitionManager(
                project_root=Path("/hello/world"),
                project_files=[Path("snowflake.my.file")],
            )

    def test_uses_local_project_definition_if_no_file_specified_explicitely(self):
        with mock_is_file_for(
            "/hello/world/snowflake.yml", "/hello/world/snowflake.local.yml"
        ) as mock_is_file:
            definition_manager = DefinitionManager(project_root=Path("/hello/world"))
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml"),
                Path("/hello/world/snowflake.local.yml"),
            ]

    def test_does_not_use_local_snowflake_local_if_file_providied_explicitely(self):
        with mock_is_file_for(
            "/hello/world/snowflake.yml",
        ):
            definition_manager = DefinitionManager(
                project_root=Path("/hello/world"), project_files=[_DEFAULT_PROJECT_FILE]
            )
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml"),
            ]

    def test_uses_file_provided_by_user(self):
        with mock_is_file_for(
            "/hello/world/any.yml",
        ):
            definition_manager = DefinitionManager(
                project_root=Path("/hello/world"), project_files=[Path("any.yml")]
            )
            assert definition_manager._project_config_paths == [
                Path("/hello/world/any.yml"),
            ]

    def test_uses_file_provided_by_user_multiple_files(self):
        with mock_is_file_for(
            "/hello/world/snowflake.yml",
            "/hello/world/snowflake.prod.yml",
            "/hello/world/snowflake.setup.yml",
        ):
            definition_manager = DefinitionManager(
                project_root=Path("/hello/world"),
                project_files=[
                    Path("snowflake.yml"),
                    Path("snowflake.prod.yml"),
                    Path("snowflake.setup.yml"),
                ],
            )
            assert definition_manager._project_config_paths == [
                Path("/hello/world/snowflake.yml"),
                Path("/hello/world/snowflake.prod.yml"),
                Path("/hello/world/snowflake.setup.yml"),
            ]
