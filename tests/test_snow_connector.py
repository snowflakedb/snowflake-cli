import textwrap
from unittest import mock

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

    connector.createFunction(
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
    print(custom_str(query_str))
    assert custom_str(query_str) == snapshot


@mock.patch("snowflake.connector")
def test_createProcedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.createProcedure(
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

    connector.executeFunction(
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

    connector.executeProcedure(
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

    connector.describeFunction(
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

    connector.describeProcedure(
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

    connector.listFunctions(
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

    connector.listStages(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_listStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.listStage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        like="likeValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_getStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.getStage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
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


@mock.patch("snowflake.connector")
def test_putStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.putStage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        path="pathValue",
        overwrite=True,
        parallel="parallelValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert textwrap.dedent(query.getvalue()) == snapshot


@mock.patch("snowflake.connector")
def test_removeFromStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.removeFromStage(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        path="pathValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_createStage(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.createStage(
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

    connector.dropStage(
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

    connector.listProcedures(
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

    connector.dropFunction(
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

    connector.dropProcedure(
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

    connector.listStreamlits(
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

    connector.showWarehouses(
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

    connector.createStreamlit(
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
def test_shareStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.shareStreamlit(
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

    connector.dropStreamlit(
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
        name="nameValue",
        drop_stage="drop_stageValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_deployStreamlit(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (mock.MagicMock(),)
    connector.uploadFileToStage = mock.MagicMock()

    connector.deployStreamlit(
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

    connector.describeStreamlit(
        name="nameValue",
        database="databaseValue",
        schema="schemaValue",
        role="roleValue",
        warehouse="warehouseValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot
