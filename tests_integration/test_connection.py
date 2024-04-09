import pytest
from tests_integration.snowflake_connector import (
    setup_test_database,
    setup_test_schema,
    add_uuid_to_name,
)


@pytest.mark.integration
def test_connection_test_simple(runner):
    result = runner.invoke_with_connection_json(["connection", "test"])
    assert result.exit_code == 0, result.output
    assert result.json["Status"] == "OK"


@pytest.mark.integration
def test_connection_dashed_database(runner, snowflake_session):
    database = add_uuid_to_name("dashed-database")
    with setup_test_database(snowflake_session, database):
        result = runner.invoke_with_connection_json(["connection", "test"])
        assert result.exit_code == 0, result.output
        assert result.json["Database"] == database


@pytest.mark.integration
@pytest.mark.skip(
    reason="BUG: connections test command seem to override every setting with schema PUBLIC"
)
def test_connection_dashed_schema(
    runner, test_database, snowflake_session, snowflake_home
):
    schema = "dashed-schema-name"
    with setup_test_schema(snowflake_session, schema):
        result = runner.invoke_with_connection(["connection", "test", "--debug"])
        assert result.exit_code == 0, result.output
        assert f'use schema "{schema}"' in result.output
