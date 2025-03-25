# TODO: think of a better name for this test file
from pathlib import Path
from typing import List
from unittest import mock
from unittest.mock import MagicMock

import pytest
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.core.stage import StageResource

GET_UI_PARAMETERS = "snowflake.cli._plugins.connection.util.get_ui_parameters"
STREAMLIT_NAME = "test_streamlit"


class StreamlitTestClass:
    @pytest.fixture(autouse=True)
    def setup(
        self, mock_ctx, mock_cursor, project_directory, alter_snowflake_yml, runner
    ):
        self.ctx = mock_ctx(
            mock_cursor(
                rows=[
                    {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                    {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
                ],
                columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
            )
        )

        self.mock_connector = mock.patch(
            "snowflake.connector.connect", return_value=self.ctx
        ).start()

        self.mock_param = mock.patch(
            GET_UI_PARAMETERS,
            return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
        ).start()

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

        self.mock_streamlit_exists = mock_streamlit_exists = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists",
            lambda _, **kwargs: False,
        ).start()

        self.project_directory = project_directory
        self.alter_snowflake_yml = alter_snowflake_yml
        self.runner = runner

    def _assert_that_exactly_those_files_were_put_to_stage(
        self,
        put_files: List[str],
        stage_name: str = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}",
        project_root: Path = Path("."),
    ):
        assert self.mock_put.call_count == len(put_files)

        for file in put_files:  # This seems repeatable, extract it to a function
            if isinstance(file, dict):
                local = file["local"]
                stage = f"{stage_name}{file['stage']}/{local.name}"
            else:
                local = file
                stage = f"{stage_name}/{file}"

            self.mock_put.assert_any_call(
                local_file_name=(project_root / local).absolute(),
                stage_location=stage,
                overwrite=True,
            )
