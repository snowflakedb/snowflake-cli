from pathlib import Path, PosixPath
from shutil import rmtree
from typing import Generator, Tuple
from zipfile import ZipFile

import pytest
import requests_mock
import typer
from snowcli import utils

from tests.unit.test_data.test_data import *

# TODO: check for consistency in using ' or "


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

    def test_anaconda_packages(self):
        anaconda_packages = utils.parse_anaconda_packages(packages)

        assert "snowflake" in anaconda_packages.keys()
        assert "other" in anaconda_packages.keys()
        assert len(anaconda_packages.get("other")) == 1
        assert len(anaconda_packages.get("snowflake")) == 2

    def test_anaconda_packages_streamlit(self):
        packages.append("streamlit==1.2.3")
        anaconda_packages = utils.parse_anaconda_packages(packages)
        assert "snowflake" in anaconda_packages.keys()
        assert "other" in anaconda_packages.keys()
        assert "streamlit" not in anaconda_packages.get("other")
        # TODO: above tests rely on correct response from anaconda.com
        #  - shouldn`t it be mocked?

    def test_anaconda_packages_with_incorrect_response(self, requests_mock):
        requests_mock.get(
            "https://repo.anaconda.com/pkgs/snowflake/channeldata.json", status_code=404
        )
        result = utils.parse_anaconda_packages(packages)
        assert result == {}

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
        result = utils.generate_streamlit_package_wrapper('example_stage', 'example_module', False)

        assert os.path.exists(result)
        with open(result, 'r') as f:
            assert 'importlib.reload(sys.modules["example_module"])' in f.read()

    def test_get_package_name_from_metadata_using_correct_data(
        self, correct_metadata_file: str
    ):
        result = utils.get_package_name_from_metadata(correct_metadata_file)
        assert result == "my-awesome-package"

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

    # Setup functions
    # These functions are used to set up files and directories used in tests
    # and delete them, after the tests are performed

    @pytest.fixture
    def temp_test_directory(self) -> Generator:
        print(os.getcwd())
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
        dummy_file = open(path, "w")
        dummy_file.close()
        yield path

    @pytest.fixture
    def correct_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.REQUIREMENTS_TXT)
        with open(path, "w") as req_file:
            for req in requirements:
                req_file.writelines(req + "\n")
        yield path

    @pytest.fixture
    def streamlit_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.REQUIREMENTS_SNOWFLAKE)
        with open(path, "w") as dummy_file:
            for req in requirements:
                dummy_file.writelines(req + "\n")
        yield path

    @pytest.fixture
    def correct_metadata_file(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, self.CORRECT_METADATA)
        with open(path, "w") as dummy_file:
            dummy_file.write(correct_package_metadata)
        yield path

    @pytest.fixture
    def file_in_a_subdir(self, temp_test_directory: str) -> Generator:
        dir_path = os.path.join(temp_test_directory, self.SUBDIR)
        os.mkdir(dir_path)

        file_path = os.path.join(dir_path, self.FILE_IN_A_SUBDIR)
        dummy_file = open(file_path, "w")
        dummy_file.close()
        yield file_path

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
        dummy_file = open(path, "w")
        dummy_file.close()
        yield path

    @pytest.fixture
    def include_paths_env_variable(self, other_directory: str) -> Generator:
        os.environ["SNOWCLI_INCLUDE_PATHS"] = other_directory
        yield os.environ["SNOWCLI_INCLUDE_PATHS"]
        os.environ.pop("SNOWCLI_INCLUDE_PATHS")
