from typing import List, Optional

from snowflake.cli.api.output.types import CommandResult
from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
    assert_that_result_is_successful_and_output_json_equals,
    assert_that_result_is_error,
)


class StreamlitTestSteps:
    def __init__(self, setup):
        self.setup = setup

    def list_streamlit_should_return_empty_list(self):
        self.assert_that_only_those_entities_are_listed([], None)

    def deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file(
        self,
    ):
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "deploy",
            ]
        )

        assert_that_result_failed_with_message_containing(
            result,
            "Multiple entities of type streamlit found. Please provide entity id for the operation.",
        )

    def deploy_with_entity_id_specified_should_succeed(self, entity_id: str, database):
        self.assert_that_only_those_entities_are_listed([], entity_id.upper())
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "deploy",
                entity_id,
            ]
        )

        self.assert_proper_url_is_returned(result, entity_id, database)

    def another_deploy_without_replace_flag_should_end_with_error(
        self, entity_id: str, database
    ):
        self.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.{entity_id.upper()}"], entity_id.upper()
        )
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "deploy",
                entity_id,
            ]
        )

        assert_that_result_is_error(result, 1)

    def another_deploy_with_replace_flag_should_succeed(self, entity_id: str, database):
        self.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.{entity_id.upper()}"], entity_id.upper()
        )
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "deploy",
                entity_id,
                "--replace",
            ]
        )

        self.assert_proper_url_is_returned(result, entity_id, database)

    def streamlit_describe_should_show_proper_streamlit(self, entity_id: str, database):
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "describe",
                entity_id,
            ]
        )

        assert len(result.json) == 1
        assert result.json[0]["name"] == entity_id.upper()

    def get_url_should_give_proper_url(self, entity_id: str, database: str):
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "get-url",
                entity_id,
            ]
        )

        assert_that_result_is_successful_and_output_json_equals(
            result, {"message": create_expected_url(entity_id, database)}
        )

    def execute_should_run_streamlit(self, entity_id: str, database: str):
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "execute",
                entity_id,
            ]
        )

        assert_that_result_is_successful_and_output_json_equals(
            result, {"message": f"Streamlit {entity_id} executed."}
        )

    def drop_should_succeed(self, entity_id: str, database: str):
        result = self.setup.runner.invoke_with_connection_json(
            [
                "streamlit",
                "drop",
                entity_id,
            ]
        )

        assert_that_result_is_successful_and_output_json_equals(
            result, [{"status": f"{entity_id.upper()} successfully dropped."}]
        )

    def assert_that_only_those_files_were_uploaded(
        self, uploaded_files: List[str], stage_name: str
    ):
        assert set(uploaded_files) == set(self.get_actual_file_staged_in_db(stage_name))

    def assert_that_only_those_entities_are_listed(
        self, entities: List[str], name: Optional[str]
    ):
        assert set(entities) == set(self.get_streamlits_in_db(name))

    def get_actual_file_staged_in_db(self, stage_name: str):
        return [
            file["name"]
            for file in self.setup.query_files_uploaded_in_this_test_case(stage_name)
        ]

    def get_streamlits_in_db(self, name: Optional[str]) -> List:
        query = "SHOW STREAMLITS"

        if name:
            query += f" LIKE '{name}%';"
        return [
            f"{s['database_name']}.{s['schema_name']}.{s['name']}"
            for s in self.setup.sql_test_helper.execute_single_sql(query)
        ]

    def assert_proper_url_is_returned(
        self, result: CommandResult, entity_id: str, database: str
    ):
        assert_that_result_is_successful_and_output_json_equals(
            result,
            {
                "message": f"Streamlit successfully deployed and available under {create_expected_url(entity_id, database)}",
            },
        )


def create_expected_url(entity_id: str, database: str):
    return f"https://app.snowflake.com/SFENGINEERING/snowcli_it/#/streamlit-apps/{database}.PUBLIC.{entity_id.upper()}"
