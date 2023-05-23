import pytest
import typer
import os

from snowcli import utils
from tests.unit.test_data.test_data import *

# TODO: check for consistency in using ' or "

def setup_module():
    print(os.getcwd())
    print('**************************************')


@pytest.mark.parametrize('argument', utils.YesNoAskOptions)
def test_yes_no_ask_callback_with_correct_argument(argument: str):
    result = utils.yes_no_ask_callback(argument)
    assert result == argument


@pytest.mark.parametrize('argument', bad_arguments_for_yesnoask)
def test_yes_no_ask_callback_with_incorrect_argument(argument):
    with pytest.raises(typer.BadParameter) as e_info:
        utils.yes_no_ask_callback(argument)
    assert e_info.value.message == f"Valid values: ['yes', 'no', 'ask']. You provided: {argument}"


@pytest.mark.parametrize('arguments', positive_arguments_for_deploy_names)
def test_get_deploy_names_correct(arguments: tuple[tuple[str, str, str], dict]):
    result = utils.getDeployNames(*arguments[0])
    assert result == arguments[1]
# TODO: think what you can break in getDeployNames


def test_prepare_app_zip():
   pass
