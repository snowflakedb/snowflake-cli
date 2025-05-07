from pathlib import Path
from typing import List
from unittest import mock
from unittest.mock import MagicMock

from snowflake.core.stage import StageResource

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

        mock_stage_resource = StageResource(
            name="stage_resource_mock", collection=MagicMock()
        )
        self.mock_create_stage = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._create_stage_if_not_exists",
            return_value=mock_stage_resource,
        ).start()

        self.mock_put = mock.patch(
            "snowflake.core.stage._stage.StageResource.put"
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
        assert self.mock_put.call_count == len(put_files)  # type: ignore

        for file in put_files:
            if isinstance(file, dict):
                local = Path(file["local"])
                stage = f"/{streamlit_name}{file['stage'] if file['stage'] else ''}"
            else:
                local = Path(file)
                stage = f"/{streamlit_name}/{str(Path(file).parent) if Path(file).parent != Path('.') else ''}"

            self.mock_put.assert_any_call(  # type: ignore
                local_file_name=local,
                stage_location=stage,
                overwrite=True,
                auto_compress=False,
            )
