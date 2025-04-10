from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli.api.identifiers import FQN

from tests.streamlit.streamlit_test_class import StreamlitTestClass


class TestStreamlitManager(StreamlitTestClass):
    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_execute_streamlit(self, mock_execute_query):
        app_name = FQN(database="DB", schema="SH", name="my_streamlit_app")

        StreamlitManager(MagicMock()).execute(app_name=app_name)

        mock_execute_query.assert_called_once_with(
            query="EXECUTE STREAMLIT IDENTIFIER('DB.SH.my_streamlit_app')()"
        )
