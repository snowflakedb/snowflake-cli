from pathlib import Path
from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli.api.project.schemas.entities.streamlit_entity_model import (
    StreamlitEntityModel,
)


@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.get_url")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
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
        QUERY_WAREHOUSE = My_WH
        TITLE = 'MyStreamlit'"""
        )
    )


@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.get_url")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager._execute_query")
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
