import textwrap
from unittest import mock

import pytest
from snowcli.snow_connector import SnowflakeConnector


# Used as a solution to syrupy having some problems with comparing multilines string
class custom_str(str):
    def __repr__(self):
        return str(self)


@mock.patch("snowflake.connector")
def test_createFunction(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_function(
        name="nameValue",
        inputParameters="(string a, variant b)",
        returnType="returnTypeValue",
        handler="handlerValue",
        imports="import1, import2",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        overwrite=True,
        packages=["aaa", "bbb"],
    )
    query_io, *_ = connector.ctx.execute_stream.call_args.args
    query_str = query_io.getvalue()
    assert custom_str(query_str) == snapshot


@mock.patch("snowflake.connector")
def test_createProcedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_procedure(
        name="nameValue",
        inputParameters="(string a, variant b)",
        returnType="returnTypeValue",
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
def test_executeFunction(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.execute_function(
        function="functionValue",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_executeProcedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_describeFunction(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.describe_function(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
        name="nameValue",
        inputParameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_describeProcedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.describe_procedure(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
        name="nameValue",
        inputParameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_listFunctions(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.list_functions(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_listStages(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.list_stages(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@pytest.mark.parametrize(
    "stage_name", [("namedStageValue"), ("snow://embeddedStageValue")]
)
@mock.patch("snowflake.connector")
def test_listStage(_, snapshot, stage_name):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.list_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name=stage_name,
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@pytest.mark.parametrize(
    "stage_name", [("namedStageValue"), ("snow://embeddedStageValue")]
)
@mock.patch("snowflake.connector")
def test_getStage(_, snapshot, stage_name):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.get_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name=stage_name,
        path="pathValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_setProcedureComment(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.setProcedureComment(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
        name="nameValue",
        inputParameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
        comment="commentValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@pytest.mark.parametrize(
    "stage_name", [("namedStageValue"), ("snow://embeddedStageValue")]
)
@mock.patch("snowflake.connector")
def test_putStage(_, snapshot, stage_name):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.put_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name=stage_name,
        path="pathValue",
        overwrite=True,
        parallel="parallelValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert textwrap.dedent(query.getvalue()) == snapshot


@pytest.mark.parametrize(
    "stage_name", [("namedStageValue"), ("snow://embeddedStageValue")]
)
@mock.patch("snowflake.connector")
def test_removeFromStage(_, snapshot, stage_name):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.remove_from_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name=stage_name,
        path="/pathValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_createStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_dropStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.drop_stage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_listProcedures(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_dropFunction(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.drop_function(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        signature="signatureValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_dropProcedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_listStreamlits(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_showWarehouses(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.show_warehouses(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_createStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_createStreamlitFromStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_shareStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_dropStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_deployStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (mock.MagicMock(),)
    connector.upload_file_to_stage = mock.MagicMock()

    connector.deploy_streamlit(
        name="nameValue",
        file_path="file_pathValue",
        stage_path="stage_pathValue",
        role="roleValue",
        database="databaseValue",
        schema="schemaValue",
        overwrite=True,
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_describeStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
