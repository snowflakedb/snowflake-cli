from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import StreamlitEntityModel
from snowflake.cli.api.identifiers import FQN

mock_streamlit_exists = mock.patch(
    "snowflake.cli._plugins.streamlit.manager.ObjectManager.object_exists",
    lambda _, **kwargs: False,
)


@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query")
def test_grant_privileges_to_streamlit(mock_execute):
    st = StreamlitEntityModel(
        type="streamlit",
        identifier="my_streamlit_app",
        title="MyStreamlit",
        main_file="main.py",
        artifacts=["main.py"],
        comment="This is a test comment",
        grants=[
            {"privilege": "AAAA", "role": "FOO"},
            {"privilege": "BBBB", "role": "BAR"},
        ],
    )

    StreamlitManager(MagicMock(database="DB", schema="SH")).grant_privileges(
        entity_model=st
    )

    mock_execute.assert_has_calls(
        [
            mock.call(
                "GRANT AAAA ON STREAMLIT IDENTIFIER('my_streamlit_app') TO ROLE FOO"
            ),
            mock.call(
                "GRANT BBBB ON STREAMLIT IDENTIFIER('my_streamlit_app') TO ROLE BAR"
            ),
        ]
    )


@mock.patch("snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query")
@mock_streamlit_exists
def test_execute_streamlit(mock_execute_query):
    app_name = FQN(database="DB", schema="SH", name="my_streamlit_app")

    StreamlitManager(MagicMock()).execute(app_name=app_name)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')()"
    )
