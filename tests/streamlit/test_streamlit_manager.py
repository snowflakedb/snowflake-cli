from unittest import mock
from unittest.mock import MagicMock

import pytest
from snowflake.cli._plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
)
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import StreamlitEntityModel
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES
from snowflake.cli.api.exceptions import CliError
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

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.make_snowsight_url",
        side_effect=MissingConnectionRegionError("acct.custom.tld"),
    )
    def test_get_url_does_not_substitute_commercial_host(self, _mock_url):
        manager = StreamlitManager(MagicMock())
        with pytest.raises(MissingConnectionRegionError):
            manager.get_url(FQN.from_string("db.sch.my_app"))

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_grant_privileges_revokes_grants_removed_from_yaml(
        self, mock_execute_query
    ):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "OLD_ROLE",
                "grant_option": False,
            },
            {
                "privilege": "OWNERSHIP",
                "granted_to": "ROLE",
                "grantee_name": "OWNER_ROLE",
                "grant_option": True,
            },
        ]
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="my_app",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            grants=[Grant(privilege="USAGE", role="NEW_ROLE")],
        )
        model.set_entity_id("my_app")

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = [str(call) for call in mock_execute_query.call_args_list]
        assert any("REVOKE USAGE" in s and "OLD_ROLE" in s for s in sqls)
        assert any("GRANT USAGE" in s and "NEW_ROLE" in s for s in sqls)
        assert not any("REVOKE" in s and "OWNERSHIP" in s for s in sqls)

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.make_snowsight_url",
        side_effect=MissingConnectionAccountError(MagicMock()),
    )
    def test_get_url_raises_when_account_is_missing(self, _mock_url):
        manager = StreamlitManager(MagicMock())
        name = FQN.from_string("DB.SCH.my_app")
        with mock.patch.object(FQN, "using_connection", return_value=name):
            with pytest.raises(MissingConnectionAccountError):
                manager.get_url(streamlit_name=name)

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_grant_privileges_fails_when_show_grants_fails(self, mock_execute_query):
        mock_execute_query.side_effect = ProgrammingError(
            errno=INSUFFICIENT_PRIVILEGES, msg="Insufficient privileges"
        )
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="my_app",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            grants=[],
        )
        model.set_entity_id("my_app")

        with pytest.raises(CliError, match="Could not list the grants on Streamlit"):
            StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = [str(call) for call in mock_execute_query.call_args_list]
        assert len(sqls) == 1 and "SHOW GRANTS ON STREAMLIT" in sqls[0]

    def _queries(self, mock_execute_query) -> list[str]:
        return [
            call.args[0] if call.args else call.kwargs["query"]
            for call in mock_execute_query.call_args_list
        ]

    def _revokes(self, sqls: list[str]) -> list[str]:
        return [sql for sql in sqls if sql.startswith("REVOKE ")]

    def _grant_model(self, grants: list[Grant]) -> StreamlitEntityModel:
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="my_app",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            grants=grants,
        )
        model.set_entity_id("my_app")
        return model

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_dropping_grant_option_revokes_only_the_option(self, mock_execute_query):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "ANALYST",
                "grant_option": "TRUE",
            }
        ]
        model = self._grant_model([Grant(privilege="USAGE", role="ANALYST")])

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = self._queries(mock_execute_query)
        assert self._revokes(sqls) == [
            "REVOKE GRANT OPTION FOR USAGE ON STREAMLIT "
            "IDENTIFIER('my_app') FROM ROLE ANALYST"
        ]
        assert "GRANT USAGE ON STREAMLIT IDENTIFIER('my_app') TO ROLE ANALYST" in sqls
        assert not any(sql.startswith("REVOKE USAGE") for sql in sqls)

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_dropping_grant_option_for_a_user_revokes_only_the_option(
        self, mock_execute_query
    ):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "USER",
                "grantee_name": "AAMADHAVAN",
                "grant_option": True,
            }
        ]
        model = self._grant_model([Grant(privilege="USAGE", user="AAMADHAVAN")])

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = self._queries(mock_execute_query)
        assert self._revokes(sqls) == [
            "REVOKE GRANT OPTION FOR USAGE ON STREAMLIT "
            "IDENTIFIER('my_app') FROM USER AAMADHAVAN"
        ]

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_adding_grant_option_does_not_revoke(self, mock_execute_query):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "ANALYST",
                "grant_option": False,
            }
        ]
        model = self._grant_model(
            [Grant(privilege="USAGE", role="ANALYST", with_grant_option=True)]
        )

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = self._queries(mock_execute_query)
        assert self._revokes(sqls) == []
        assert any(sql.endswith("WITH GRANT OPTION") for sql in sqls)

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_unchanged_grant_option_is_not_revoked(self, mock_execute_query):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "ANALYST",
                "grant_option": "YES",
            }
        ]
        model = self._grant_model(
            [Grant(privilege="USAGE", role="ANALYST", with_grant_option=True)]
        )

        StreamlitManager(MagicMock()).grant_privileges(model)

        assert self._revokes(self._queries(mock_execute_query)) == []

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_removed_grant_with_option_revokes_the_privilege(self, mock_execute_query):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "ANALYST",
                "grant_option": True,
            }
        ]
        model = self._grant_model([])

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = self._queries(mock_execute_query)
        assert self._revokes(sqls) == [
            "REVOKE USAGE ON STREAMLIT IDENTIFIER('my_app') FROM ROLE ANALYST"
        ]
        assert not any("GRANT OPTION" in sql for sql in sqls)

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.StreamlitManager.execute_query"
    )
    def test_ownership_is_not_revoked_when_grants_are_cleared(self, mock_execute_query):
        mock_execute_query.return_value.fetchall.return_value = [
            {
                "privilege": "OWNERSHIP",
                "granted_to": "ROLE",
                "grantee_name": "OWNER_ROLE",
                "grant_option": True,
            },
            {
                "privilege": "USAGE",
                "granted_to": "ROLE",
                "grantee_name": "ANALYST",
                "grant_option": True,
            },
        ]
        model = self._grant_model([])

        StreamlitManager(MagicMock()).grant_privileges(model)

        sqls = self._queries(mock_execute_query)
        assert not any("OWNERSHIP" in sql and sql.startswith("REVOKE") for sql in sqls)
        assert any(sql.startswith("REVOKE USAGE") for sql in sqls)
