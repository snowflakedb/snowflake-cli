import pytest
import typer
import os

from snowcli import utils
from tests.unit.test_data.test_data import *

# TODO: check for consistency in using ' or "


class TestUtils:
    def setup_class(self):
        self.temp_directory = self.create_temp_test_directory()
        self.create_correct_app_zip()

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

    def test_prepare_app_zip(self):
        path = os.getcwd()
        result = utils.prepare_app_zip('app.zip', path)
        assert result == path + 'app.zip'

    def create_correct_app_zip(self) -> str:
        path = os.path.join(self.temp_directory, 'app.zip')
        dummy_file = open(path, 'w')
        dummy_file.close()
        print('*********')
        return path

    @staticmethod
    def create_temp_test_directory() -> str:
        path = os.path.join(os.getcwd(),'.tests')
        os.mkdir(path)
        return path
