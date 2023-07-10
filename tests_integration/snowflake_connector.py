import pytest

from snowflake import connector


@pytest.fixture(scope="session")
def snowflake_session():
    config = {
        "account": "na_consumer_qa6",
        "application": "SNOWCLI",
        "db": "",
        "host": "na_consumer_qa6.qa6.us-west-2.aws.snowflakecomputing.com",
        "password": "test",
        "port": "",
        "protocol": "",
        "schema": "",
        "user": "admin",
    }
    connection = connector.connect(**config)
    yield connection
    connection.close()
