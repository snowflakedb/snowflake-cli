from pathlib import Path, PosixPath
from shutil import rmtree
from typing import Generator, Tuple, List
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import os
import pytest
import typer
from snowcli import utils

from tests.unit.test_data.test_data import *


class TestUtils:
    APP_ZIP = "app.zip"
    CORRECT_METADATA = "correct_metadata.yaml"
    FILE_IN_A_SUBDIR = "file_in_a_subdir.txt"
    FILE_IN_SECOND_TEST_DIRECTORY = "very_important_file.txt"
    REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
    REQUIREMENTS_TXT = "requirements.txt"
    SECOND_TEST_DIRECTORY = "other_test_dir"
    SUBDIR = "subdir"
    TEMP_DIR_FOR_APP_ZIP = "temp_dir"
    TEMP_TEST_DIRECTORY = ".tests"

    @pytest.mark.parametrize("argument", utils.YesNoAskOptions)
    def test_yes_no_ask_callback_with_correct_argument(self, argument: str):
        result = utils.yes_no_ask_callback(argument)

        assert result == argument

    @pytest.mark.parametrize("argument", bad_arguments_for_yesnoask)
    def test_yes_no_ask_callback_with_incorrect_argument(self, argument):
        with pytest.raises(typer.BadParameter) as e_info:
            utils.yes_no_ask_callback(argument)

        assert (
            e_info.value.message
            == f"Valid values: ['yes', 'no', 'ask']. You provided: {argument}"
        )

    @pytest.mark.parametrize("arguments", positive_arguments_for_deploy_names)
    def test_get_deploy_names_correct(
        self, arguments: Tuple[Tuple[str, str, str], dict]
    ):
        result = utils.get_deploy_names(*arguments[0])

        assert result == arguments[1]

    def test_prepare_app_zip(
        self,
        temp_test_directory: str,
        correct_app_zip: str,
        temp_directory_for_app_zip: str,
    ):
        result = utils.prepare_app_zip(correct_app_zip, temp_directory_for_app_zip)

        assert result == temp_directory_for_app_zip + "/app.zip"

    def test_prepare_app_zip_if_exception_is_raised_if_no_source(
        self, temp_directory_for_app_zip
    ):
        with pytest.raises(FileNotFoundError) as expected_error:
            utils.prepare_app_zip("/non/existent/path", temp_directory_for_app_zip)

        assert expected_error.value.errno == 2
        assert expected_error.type == FileNotFoundError

    def test_prepare_app_zip_if_exception_is_raised_if_no_dst(self, correct_app_zip):
        with pytest.raises(FileNotFoundError) as expected_error:
            utils.prepare_app_zip(correct_app_zip, "/non/existent/path")

        assert expected_error.value.errno == 2
        assert expected_error.type == FileNotFoundError

    def test_parse_requierements_with_correct_file(self, correct_requirements_txt: str):
        result = utils.parse_requirements(correct_requirements_txt)

        assert len(result) == len(requirements)

    def test_parse_requirements_with_nonexistent_file(self, temp_test_directory: str):
        path = os.path.join(temp_test_directory, "non_existent.file")
        result = utils.parse_requirements(path)

        assert result == []

    @patch("tests.unit.test_utils.utils.requests")
    def test_anaconda_packages(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = anaconda_response
        mock_requests.get.return_value = mock_response

        anaconda_packages = utils.parse_anaconda_packages(packages)
        assert (
            Requirement.parse_line("snowflake-connector-python")
            in anaconda_packages.snowflake
        )
        assert (
            Requirement.parse_line("my-totally-awesome-package")
            in anaconda_packages.other
        )

    @patch("tests.unit.test_utils.utils.requests")
    def test_anaconda_packages_streamlit(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = anaconda_response
        mock_requests.get.return_value = mock_response

        packages.append(Requirement.parse_line("streamlit"))
        anaconda_packages = utils.parse_anaconda_packages(packages)

        assert Requirement.parse_line("streamlit") not in anaconda_packages.other

    @patch("tests.unit.test_utils.utils.requests")
    def test_anaconda_packages_with_incorrect_response(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {}
        mock_requests.get.return_value = mock_response

        with pytest.raises(typer.Abort) as abort:
            result = utils.parse_anaconda_packages(packages)

    def test_generate_streamlit_environment_file_with_no_requirements(self):
        result = utils.generate_streamlit_environment_file([])
        assert result is None

    def test_generate_streamlit_environment_file(
        self, streamlit_requirements_txt, temp_test_directory: str
    ):
        os.chdir(temp_test_directory)
        result = utils.generate_streamlit_environment_file([])
        os.chdir("..")

        assert result == PosixPath("environment.yml")
        assert os.path.isfile(os.path.join(temp_test_directory, "environment.yml"))

    def test_generate_streamlit_environment_file_with_excluded_dependencies(
        self, streamlit_requirements_txt, temp_test_directory: str
    ):
        os.chdir(temp_test_directory)
        result = utils.generate_streamlit_environment_file(excluded_anaconda_deps)
        os.chdir("..")
        env_file = os.path.join(temp_test_directory, "environment.yml")
        assert result == PosixPath("environment.yml")
        assert os.path.isfile(env_file)
        with open(env_file, "r") as f:
            for dep in excluded_anaconda_deps:
                assert dep not in f.read()

    def test_generate_streamlit_package_wrapper(self):
        result = utils.generate_streamlit_package_wrapper(
            "example_stage", "example_module", False
        )

        assert os.path.exists(result)
        with open(result, "r") as f:
            assert 'importlib.reload(sys.modules["example_module"])' in f.read()

    def test_get_downloaded_package_names(self):
        pass  # todo: prepare a fixture to test it

    def test_get_package_name_from_metadata_using_correct_data(
        self, correct_metadata_file: str
    ):
        result = utils.get_package_name_from_metadata(correct_metadata_file)
        assert result == Requirement.parse_line("my-awesome-package==0.0.1")

    def test_generate_snowpark_coverage_wrapper(self, temp_test_directory: str):
        path = os.path.join(temp_test_directory, "coverage.py")
        utils.generate_snowpark_coverage_wrapper(
            target_file=path,
            proc_name="process",
            proc_signature="signature",
            handler_module="awesomeModule",
            handler_function="even_better_function",
            coverage_reports_stage="example_stage",
            coverage_reports_stage_path="nyan-cat.jpg",
        )

        assert os.path.isfile(path)
        with open(path) as coverage_file:
            assert (
                "return awesomeModule.even_better_function(*args,**kwargs)"
                in coverage_file.read()
            )

    def test_add_file_to_existing_zip(
        self, correct_app_zip: str, correct_requirements_txt: str
    ):
        utils.add_file_to_existing_zip(correct_app_zip, correct_requirements_txt)
        zip_file = ZipFile(correct_app_zip)

        assert os.path.basename(correct_requirements_txt) in zip_file.namelist()

    def test_install_packages(self):
        pass  # todo: add this

    def test_recursive_zip_packages(
        self,
        temp_test_directory: str,
        file_in_a_subdir: str,
        file_in_other_directory: str,
    ):
        zip_file_path = os.path.join(temp_test_directory, "packed.zip")

        utils.recursive_zip_packages_dir(temp_test_directory, zip_file_path)
        zip_file = ZipFile(zip_file_path)
        print(os.getenv("SNOWCLI_INCLUDE_PATHS"))

        assert os.path.isfile(zip_file_path)
        assert os.getenv("SNOWCLI_INCLUDE_PATHS") is None
        assert (os.path.join(self.SUBDIR, self.FILE_IN_A_SUBDIR)) in zip_file.namelist()
        assert (
            os.path.join(self.SECOND_TEST_DIRECTORY, self.FILE_IN_SECOND_TEST_DIRECTORY)
            not in zip_file.namelist()
        )

    def test_recursive_zip_packages_with_env_variable(
        self,
        temp_test_directory: str,
        file_in_a_subdir: str,
        other_directory: str,
        file_in_other_directory: str,
        include_paths_env_variable,
    ):
        zip_file_path = os.path.join(temp_test_directory, "packed.zip")

        utils.recursive_zip_packages_dir(temp_test_directory, zip_file_path)
        zip_file = ZipFile(zip_file_path)

        assert os.path.isfile(zip_file_path)
        assert (os.path.join(self.SUBDIR, self.FILE_IN_A_SUBDIR)) in zip_file.namelist()
        assert os.path.join(self.FILE_IN_SECOND_TEST_DIRECTORY) in zip_file.namelist()

    def test_standard_zip_dir(self, temp_test_directory: str, file_in_a_subdir: str):
        zip_file_path = os.path.join(temp_test_directory, "packed.zip")
        utils.standard_zip_dir(zip_file_path)
        zip_file = ZipFile(zip_file_path)

        assert os.path.isfile(zip_file_path)
        assert (
            os.path.join("subdir", os.path.basename(file_in_a_subdir))
            not in zip_file.namelist()
        )

    def test_get_snowflake_packages(self, streamlit_requirements_txt):
        os.chdir(".tests")
        result = utils.get_snowflake_packages()
        os.chdir("..")

        assert result == requirements

    def test_get_snowflake_packages_delta(self, streamlit_requirements_txt):
        anaconda_package = requirements[-1]
        os.chdir(".tests")
        result = utils.get_snowflake_packages_delta(anaconda_package)
        os.chdir("..")

        assert result == requirements[:-1]

    def test_convert_resource_details_to_dict(self):
        assert (
            utils.convert_resource_details_to_dict(example_resource_details)
            == expected_resource_dict
        )

    # Setup functions
    # These functions are used to set up files and directories used in tests
    # and delete them, after the tests are performed

    @pytest.fixture
    def temp_test_directory(self) -> Generator:
        path = os.path.join(os.getcwd(), self.TEMP_TEST_DIRECTORY).lower()
        os.mkdir(path)
        yield path
        rmtree(path)  # We delete whole directory in teardown -
        # so, no need to delete any of the files separately

    @pytest.fixture
    def temp_directory_for_app_zip(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.TEMP_DIR_FOR_APP_ZIP)
        os.mkdir(path)
        yield path

    @pytest.fixture
    def correct_app_zip(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.APP_ZIP)
        self.create_file(path, [])
        yield path

    @pytest.fixture
    def correct_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.REQUIREMENTS_TXT)
        self.create_file(path, requirements)
        yield path

    @pytest.fixture
    def streamlit_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.REQUIREMENTS_SNOWFLAKE)
        self.create_file(path, requirements)
        yield path

    @pytest.fixture
    def correct_metadata_file(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.CORRECT_METADATA)
        self.create_file(path, correct_package_metadata)
        yield path

    @pytest.fixture
    def file_in_a_subdir(self, temp_test_directory: str) -> Generator:
        dir_path = os.path.join(temp_test_directory, self.SUBDIR)
        os.mkdir(dir_path)

        path = os.path.join(dir_path, self.FILE_IN_A_SUBDIR)
        self.create_file(path, [])
        yield path

    @pytest.fixture
    def other_directory(self) -> Generator:
        current_path = Path(os.getcwd())
        path = os.path.join(
            current_path.parent.absolute(), self.SECOND_TEST_DIRECTORY
        ).lower()
        os.mkdir(path)
        yield path
        rmtree(path)

    @pytest.fixture
    def file_in_other_directory(self, other_directory: str) -> Generator:
        path = os.path.join(other_directory, self.FILE_IN_SECOND_TEST_DIRECTORY)
        self.create_file(path, [])
        yield path

    @pytest.fixture
    def include_paths_env_variable(self, other_directory: str) -> Generator:
        os.environ["SNOWCLI_INCLUDE_PATHS"] = other_directory
        yield os.environ["SNOWCLI_INCLUDE_PATHS"]
        os.environ.pop("SNOWCLI_INCLUDE_PATHS")

    @staticmethod
    def create_file(filepath: str, contents: List[str]) -> None:
        with open(filepath, "w") as new_file:
            for line in contents:
                new_file.write(line + "\n")
