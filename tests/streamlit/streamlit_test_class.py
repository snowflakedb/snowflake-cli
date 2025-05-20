from re import match
from typing import List
from unittest import mock

from tests.conftest import MockCursor

STREAMLIT_NAME = "test_streamlit"

EXECUTE_QUERY = (
    "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._execute_query"
)

TYPER = "snowflake.cli._plugins.streamlit.commands.typer"


class StreamlitTestClass:
    def setup_method(self):
        self.mock_conn = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._conn"
        ).start()
        self.mock_conn.schema = None
        self.mock_conn.database = None

        self.mock_connector = mock.patch("snowflake.connector.connect").start()

        self.mock_execute = mock.patch(EXECUTE_QUERY).start()

        self.mock_create_stage = mock.patch(
            "snowflake.cli._plugins.stage.manager.StageManager.create",
        ).start()

        self.mock_list_files = mock.patch(
            "snowflake.cli._plugins.stage.manager.StageManager.list_files",
            return_value=MockCursor.from_input([], []),
        ).start()

        self.mock_put = mock.patch(
            "snowflake.cli._plugins.stage.manager.StageManager.put"
        ).start()

        self.mock_get_account = mock.patch(
            "snowflake.cli._plugins.connection.util.get_account"
        ).start()
        self.mock_get_account.return_value = "my_account"

        self.mock_snowsight_url = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.make_snowsight_url",
            return_value="https://foo.bar",
        ).start()

        self.mock_streamlit_exists = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists",
            lambda _, **kwargs: False,
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def _assert_that_exactly_those_files_were_put_to_stage(
        self,
        put_files: List[str],
        streamlit_name: str = "test_streamlit",
    ):
        # assert self.mock_put.call_count == len(put_files)  # type: ignore

        re_local_path = f".*/{streamlit_name}/(?P<filename>.*)"
        uploaded_files = set()
        for call in self.mock_put.call_args_list:
            if path := call.kwargs.get("local_path"):
                matched_path = match(re_local_path, path.as_posix())
                if matched_path:
                    uploaded_files.add(matched_path.group("filename"))

        assert set(put_files) == uploaded_files, uploaded_files
