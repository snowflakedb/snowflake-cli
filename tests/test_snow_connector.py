import textwrap
from unittest import mock

import pytest
from snowcli.snow_connector import SnowflakeConnector


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


@mock.patch("snowflake.connector")
def test_create_function(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
    connector.ctx.execute_stream.return_value = (None, None)

    connector.create_function(
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
    )
    query_io, *_ = connector.ctx.execute_stream.call_args.args
    query_str = query_io.getvalue()
    assert CustomStr(query_str) == snapshot


@mock.patch("snowflake.connector")
def test_create_procedure(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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
def test_execute_function(_, snapshot):
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
def test_execute_procedure(_, snapshot):
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
def test_describe_function(_, snapshot):
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
        input_parameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_describe_procedure(_, snapshot):
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
        input_parameters="(string a, variant b)",
        show_exceptions="show_exceptionsValue",
    )
    query, *_ = connector.ctx.execute_stream.call_args.args
    assert query.getvalue() == snapshot


@mock.patch("snowflake.connector")
def test_list_functions(_, snapshot):
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
def test_list_stages(_, snapshot):
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
def test_list_stage(_, snapshot, stage_name):
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
def test_get_stage(_, snapshot, stage_name):
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
def test_set_procedure_comment(_, snapshot):
    connector = SnowflakeConnector(
        connection_name="foo", snowsql_config=mock.MagicMock()
    )
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


@pytest.mark.parametrize(
    "stage_name", [("namedStageValue"), ("snow://embeddedStageValue")]
)
@mock.patch("snowflake.connector")
def test_put_stage(_, snapshot, stage_name):
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
def test_remove_from_stage(_, snapshot, stage_name):
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
def test_create_stage(_, snapshot):
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
def test_drop_stage(_, snapshot):
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
def test_list_procedures(_, snapshot):
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
def test_drop_function(_, snapshot):
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
def test_drop_procedure(_, snapshot):
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
def test_list_streamlits(_, snapshot):
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
def test_show_warehouses(_, snapshot):
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
def test_create_streamlit(_, snapshot):
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
def test_create_streamlit_from_stage(_, snapshot):
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
def test_share_streamlit(_, snapshot):
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
def test_drop_streamlit(_, snapshot):
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
def test_deploy_streamlit(_, snapshot):
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
def test_describe_streamlit(_, snapshot):
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
