import pytest

from snowflake import connector


@pytest.fixture(scope="session")
def snowflake_session():
    config = {
        "account": "",
        "application": "",
        "db": "",
        "host": "",
        "password": "",
        "port": "",
        "protocol": "",
        "schema": "",
        "user": "",
    }
    connection = connector.connect(**config)
    yield connection
    connection.close()
