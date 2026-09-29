from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.connector import ProgrammingError

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

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_location_usage_refusal_is_reported_not_raised(self, mock_execute_query):
        """The app grant does not need these, so the share must survive a refusal."""
        mock_execute_query.side_effect = [
            ProgrammingError("Insufficient privileges to operate on database"),
            None,
        ]

        problems = StreamlitManager(MagicMock()).grant_location_usage(
            FQN.from_string("db.sch.my_app"),
            [Grant(privilege="USAGE", user="AAMADHAVAN")],
        )

        assert problems == [
            "Could not grant USAGE on database to USER AAMADHAVAN:"
            " Insufficient privileges to operate on database"
        ]

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_location_usage_refusal_sanitizes_the_grantee(self, mock_execute_query):
        """The grantee name comes from --to-user, so it reaches the terminal raw."""
        mock_execute_query.side_effect = [
            ProgrammingError("Insufficient privileges"),
            None,
        ]

        problems = StreamlitManager(MagicMock()).grant_location_usage(
            FQN.from_string("db.sch.my_app"),
            [Grant(privilege="USAGE", user="\x1b[31mEVIL")],
        )

        assert problems == [
            'Could not grant USAGE on database to USER "EVIL": Insufficient privileges'
        ]

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_location_usage_needs_a_database(self, mock_execute_query):
        problems = StreamlitManager(MagicMock()).grant_location_usage(
            FQN.from_string("my_app"), [Grant(privilege="USAGE", user="AAMADHAVAN")]
        )

        assert problems == []
        mock_execute_query.assert_not_called()
