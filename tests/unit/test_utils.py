from shutil import rmtree
from typing import Generator

import pytest
import typer
from snowcli import utils

from tests.unit.test_data.test_data import *

# TODO: check for consistency in using ' or "


class TestUtils:
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
        self, arguments: tuple[tuple[str, str, str], dict]
    ):
        result = utils.get_deploy_names(*arguments[0])
        assert result == arguments[1]

    # TODO: think what you can break in getDeployNames

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

    def test_generate_streamlit_environment_file(self):
        pass  # todo: create this

    def test_get_package_name_from_metadata_using_correct_data(self, correct_metadata_file: str):
        result = utils.get_package_name_from_metadata(correct_metadata_file)
        with open(correct_metadata_file,'r') as f:
            print(f)
        assert result == 'my-awesome-package'

    # Setup functions
    # These functions are used to set up files and directories used in tests
    # and delete them, after the tests are performed

    @pytest.fixture
    def temp_test_directory(self) -> Generator:
        path = os.path.join(os.getcwd(), ".tests").lower()
        os.mkdir(path)  # TODO: should we check if the directory was correctly created?
        print(path)
        yield path
        rmtree(path)  # We delete whole directory in teardown -
        # so, no need to delete any of the files separately

    @pytest.fixture
    def temp_directory_for_app_zip(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, "temp_dir")
        os.mkdir(path)
        yield path

    @pytest.fixture
    def correct_app_zip(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, "app.zip")
        dummy_file = open(path, "w")
        dummy_file.close()
        yield path

    @pytest.fixture
    def correct_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, "requirements.txt")
        with open(path, "w") as req_file:
            for req in requirements:
                req_file.writelines(req + "\n")
        yield path

    @pytest.fixture
    def streamlit_requirements_txt(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, "requirements.snowflake.txt")
        dummy_file = open(path, "w")
        dummy_file.close()
        yield path

    @pytest.fixture
    def correct_metadata_file(self, temp_test_directory: str) -> Generator:
        path = os.path.join(temp_test_directory, 'correct_metadata.yaml')
        with open(path, 'w') as dummy_file:
            dummy_file.write(correct_package_metadata)
        yield path
