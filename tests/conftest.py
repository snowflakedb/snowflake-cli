import pytest
from typer.testing import CliRunner

from snowcli import app

@pytest.fixture
def runner():
    return CliRunner(app)

