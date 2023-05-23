import pytest
from shutil import rmtree
import typer

from snowcli import utils
from tests.unit.test_data.test_data import *

# TODO: check for consistency in using ' or "


class TestUtils:

    @pytest.mark.parametrize('argument', utils.YesNoAskOptions)
    def test_yes_no_ask_callback_with_correct_argument(self, argument: str):
        result = utils.yes_no_ask_callback(argument)
        assert result == argument

    @pytest.mark.parametrize('argument', bad_arguments_for_yesnoask)
    def test_yes_no_ask_callback_with_incorrect_argument(self, argument):
        with pytest.raises(typer.BadParameter) as e_info:
            utils.yes_no_ask_callback(argument)
        assert e_info.value.message == f"Valid values: ['yes', 'no', 'ask']. You provided: {argument}"

    @pytest.mark.parametrize('arguments', positive_arguments_for_deploy_names)
    def test_get_deploy_names_correct(self, arguments: tuple[tuple[str, str, str], dict]):
        result = utils.get_deploy_names(*arguments[0])
        assert result == arguments[1]
    # TODO: think what you can break in getDeployNames

    def test_prepare_app_zip(self, temp_test_directory, correct_app_zip, temp_directory_for_app_zip):
        result = utils.prepare_app_zip(correct_app_zip, temp_directory_for_app_zip )
        assert result == temp_directory_for_app_zip + '/app.zip'

    def test_prepare_app_zip_if_exception_is_raised_if_no_source(self,temp_directory_for_app_zip):
        with pytest.raises(FileNotFoundError) as expected_error:
            utils.prepare_app_zip('/non/existent/path', temp_directory_for_app_zip)
        assert expected_error.value.errno == 2
        assert expected_error.type == FileNotFoundError

    def test_prepare_app_zip_if_exception_is_raised_if_no_dst(self,correct_app_zip):
        with pytest.raises(FileNotFoundError) as expected_error:
            utils.prepare_app_zip(correct_app_zip, '/non/existent/path')
        assert expected_error.value.errno == 2
        assert expected_error.type == FileNotFoundError

# Setup functions
# These functions are used to set up files and directories used in tests
# and delete them, after the tests are performed
    @pytest.fixture
    def temp_test_directory(self) -> str:
        path = os.path.join(os.getcwd(), '.tests').lower()
        os.mkdir(path)  # TODO: should we check if the directory was correctly created?
        print(path)
        yield path
        rmtree(path)

    @pytest.fixture
    def temp_directory_for_app_zip(self,temp_test_directory):
        path = os.path.join(temp_test_directory,'temp_dir')
        os.mkdir(path)
        yield path

    @pytest.fixture
    def correct_app_zip(self, temp_test_directory) -> str:
        path = os.path.join(temp_test_directory, 'app.zip')
        dummy_file = open(path, 'w')
        dummy_file.close()
        yield path

