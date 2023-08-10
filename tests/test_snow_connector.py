import os
import textwrap
from unittest import mock

import pytest

from snowcli.snow_connector import SnowflakeConnector, SnowflakeCursor


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "warehouse": "warehouseValue",
}


@pytest.mark.parametrize(
    "cmd,expected",
    [
        (["sql", "-q", "foo"], "SNOWCLI.SQL"),
        (["warehouse", "status"], "SNOWCLI.WAREHOUSE.STATUS"),
    ],
)
@mock.patch("snowcli.snow_connector.snowflake.connector.connect")
def test_command_context_is_passed_to_snowflake_connection(
    mock_conn, runner, cmd, expected
):
    mock_cursor = mock.Mock(spec=SnowflakeCursor)
    mock_cursor.description = []
    mock_cursor.fetchall.return_value = []

    mock_conn.return_value.execute_stream.return_value = [mock_cursor]
    mock_conn.return_value.execute_string.return_value = [mock_cursor]
    result = runner.invoke_with_config(cmd)
    assert result.exit_code == 0, result.output
    kwargs = mock_conn.call_args_list[-1][-1]
    assert "application" in kwargs
    assert kwargs["application"] == expected


@mock.patch("snowflake.connector")
def test_create_procedure(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_procedure(
        name="nameValue",
        input_parameters="(string a, variant b)",
        return_type="returnTypeValue",
        handler="handlerValue",
        imports="import1, import2",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        overwrite=True,
        packages=["aaa", "bbb"],
        execute_as_caller=True,
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_execute_procedure(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.execute_procedure(
        procedure="procedureValue",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_describe_procedure(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.describe_procedure(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
        name="nameValue",
        input_parameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_set_procedure_comment(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.set_procedure_comment(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
        name="nameValue",
        input_parameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
        comment="commentValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_list_procedures(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.list_procedures(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_drop_procedure(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.drop_procedure(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_list_streamlits(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.list_streamlits(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_create_streamlit(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_streamlit(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        file="fileValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_create_streamlit_from_stage(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_streamlit(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        file="fileValue",
        from_stage_command="FROM @stageValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_share_streamlit(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.share_streamlit(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        to_role="to_roleValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_drop_streamlit(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.drop_streamlit(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_deploy_streamlit(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (mock.MagicMock(),)
    connector.upload_file_to_stage = mock.MagicMock()

    connector.deploy_streamlit(
        name="nameValue",
        file_path="file_pathValue",
        stage_path="stage_pathValue",
        role="roleValue",
        database="databaseValue",
        schema="schemaValue",
        warehouse="warehouseValue",
        overwrite=True,
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@pytest.mark.parametrize(
    "create_stage",
    [True, False],
)
@pytest.mark.parametrize("stage_name", ["namedStageValue", "snow://embeddedStageValue"])
@mock.patch("snowflake.connector")
def test_upload_file_to_stage(_, snapshot, create_stage, stage_name):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.upload_file_to_stage(
        file_path="file_pathValue",
        destination_stage=stage_name,
        path="pathValue",
        role="roleValue",
        database="databaseValue",
        schema="schemaValue",
        warehouse="warehouseValue",
        overwrite="overwriteValue",
        create_stage=create_stage,
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_describe_streamlit(_, snapshot):
    connector = SnowflakeConnector(connection_parameters=MOCK_CONNECTION)
    connector.ctx.execute_stream.return_value = (None, None)

    connector.describe_streamlit(
        name="nameValue",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowcli.cli.snowpark.registry.connect_to_snowflake")
def test_registry_get_token(mock_conn, runner):
    mock_conn.return_value.ctx._rest._token_request.return_value = {
        "data": {
            "sessionToken": "token1234",
            "validityInSecondsST": 42,
        }
    }
    result = runner.invoke(["snowpark", "registry", "token"])
    assert result.exit_code == 0, result.output
    assert result.stdout == '{"token": "token1234", "expires_in": 42}'


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_connectivity_error(runner):
    result = runner.invoke_with_config(["sql", "-q", "select 1"])
    assert result.exit_code == 1
    assert "Invalid connection configuration" in result.output
    assert "User is empty" in result.output
