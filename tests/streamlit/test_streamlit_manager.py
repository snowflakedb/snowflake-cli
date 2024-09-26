from pathlib import Path
from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli.api.identifiers import FQN

mock_streamlit_exists = mock.patch(
    "snowflake.cli._plugins.streamlit.manager.ObjectManager.object_exists",
    lambda _, **kwargs: False,
)


@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.get_url")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
@mock_streamlit_exists
def test_deploy_streamlit(mock_execute_query, _, mock_stage_manager, temp_dir):
    mock_stage_manager().get_standard_stage_prefix.return_value = "stage_root"

    main_file = Path(temp_dir) / "main.py"
    main_file.touch()

    st = StreamlitEntityModel(
        type="streamlit",
        identifier="my_streamlit_app",
        title="MyStreamlit",
        query_warehouse="My_WH",
        main_file=str(main_file),
        imports=["@stage/foo.py", "@stage/bar.py"],
        # Possibly can be PathMapping
        artifacts=[main_file],
    )

    StreamlitManager(MagicMock(database="DB", schema="SH")).deploy(
        streamlit=st, replace=False
    )

    mock_execute_query.assert_called_once_with(
        dedent(
            f"""\
        CREATE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')
        ROOT_LOCATION = 'stage_root'
        MAIN_FILE = '{main_file}'
        IMPORTS = ('@stage/foo.py', '@stage/bar.py')
        QUERY_WAREHOUSE = My_WH
        TITLE = 'MyStreamlit'"""
        )
    )


@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.get_url")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
@mock_streamlit_exists
def test_deploy_streamlit_with_api_integrations(
    mock_execute_query, _, mock_stage_manager, temp_dir
):
    mock_stage_manager().get_standard_stage_prefix.return_value = "stage_root"

    main_file = Path(temp_dir) / "main.py"
    main_file.touch()

    st = StreamlitEntityModel(
        type="streamlit",
        identifier="my_streamlit_app",
        title="MyStreamlit",
        query_warehouse="My_WH",
        main_file=str(main_file),
        # Possibly can be PathMapping
        artifacts=[main_file],
        external_access_integrations=["MY_INTERGATION", "OTHER"],
        secrets={"my_secret": "SecretOfTheSecrets", "other": "other_secret"},
    )

    StreamlitManager(MagicMock(database="DB", schema="SH")).deploy(
        streamlit=st, replace=False
    )

    mock_execute_query.assert_called_once_with(
        dedent(
            f"""\
        CREATE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')
        ROOT_LOCATION = 'stage_root'
        MAIN_FILE = '{main_file}'
        QUERY_WAREHOUSE = My_WH
        TITLE = 'MyStreamlit'
        external_access_integrations=(MY_INTERGATION, OTHER)
        secrets=('my_secret'=SecretOfTheSecrets, 'other'=other_secret)"""
        )
    )


@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.get_url")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
@mock_streamlit_exists
def test_deploy_streamlit_with_comment(
    mock_execute_query, _, mock_stage_manager, temp_dir
):
    mock_stage_manager().get_standard_stage_prefix.return_value = "stage_root"

    main_file = Path(temp_dir) / "main.py"
    main_file.touch()

    st = StreamlitEntityModel(
        type="streamlit",
        identifier="my_streamlit_app",
        title="MyStreamlit",
        query_warehouse="My_WH",
        main_file=str(main_file),
        artifacts=[main_file],
        comment="This is a test comment",
    )

    StreamlitManager(MagicMock(database="DB", schema="SH")).deploy(
        streamlit=st, replace=False
    )

    mock_execute_query.assert_called_once_with(
        dedent(
            f"""\
            CREATE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')
            ROOT_LOCATION = 'stage_root'
            MAIN_FILE = '{main_file}'
            QUERY_WAREHOUSE = My_WH
            TITLE = 'MyStreamlit'
            COMMENT = 'This is a test comment'"""
        )
    )


@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
@mock_streamlit_exists
def test_execute_streamlit(mock_execute_query):
    app_name = FQN(database="DB", schema="SH", name="my_streamlit_app")

    StreamlitManager(MagicMock()).execute(app_name=app_name)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')()"
    )
