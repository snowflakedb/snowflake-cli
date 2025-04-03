# # Copyright (c) 2024 Snowflake Inc.
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# # http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# import shutil
# from textwrap import dedent
# from unittest import mock
#
# import pytest
#
# from tests.streamlit.streamlit_test_class import StreamlitTestClass
#
# STREAMLIT_NAME = "test_streamlit"
# TEST_WAREHOUSE = "test_warehouse"
#
# @pytest.mark.skip
# class TestStreamlitCommands(StreamlitTestClass):
#     def test_list_streamlit(self, setup):
#         result = self.runner.invoke(["object", "list", "streamlit"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_query() == "show streamlits like '%%'"
#
#     def test_describe_streamlit(self, setup):
#         result = self.runner.invoke(["object", "describe", "streamlit", STREAMLIT_NAME])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             f"describe streamlit IDENTIFIER('{STREAMLIT_NAME}')",
#         ]
#
#     @mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
#     def test_deploy_only_streamlit_file(self, mock_typer, setup):
#         with self.project_directory("example_streamlit") as tmp_dir:
#             (tmp_dir / "environment.yml").unlink()
#             shutil.rmtree(tmp_dir / "pages")
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#         self.mock_create_stage.assert_called_once
#         self.mock_put.assert_called_once_with(
#             local_file_name=(tmp_dir / "streamlit_app.py").resolve(),
#             stage_location="/test_streamlit/",
#             overwrite=True,
#             auto_compress=False,
#         )
#
#         mock_typer.launch.assert_not_called()
#
#     @mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
#     def test_deploy_only_streamlit_file_no_stage(self, mock_typer, setup):
#         with self.project_directory("example_streamlit_no_stage") as tmp_dir:
#             (tmp_dir / "environment.yml").unlink()
#             shutil.rmtree(tmp_dir / "pages")
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse;
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self.mock_put.assert_called_once_with(
#             local_file_name=(tmp_dir / "streamlit_app.py").resolve(),
#             stage_location="/test_streamlit/",
#             overwrite=True,
#             auto_compress=False,
#         )
#         mock_typer.launch.assert_not_called()
#
#     def test_deploy_with_empty_pages(self, setup):
#         with self.project_directory("streamlit_empty_pages") as tmp_dir:
#             (tmp_dir / "pages").mkdir(parents=True, exist_ok=True)
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse;
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml"]
#         )
#
#     @mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
#     def test_deploy_only_streamlit_file_replace(self, mock_typer, setup):
#         with self.project_directory("example_streamlit") as tmp_dir:
#             (tmp_dir / "environment.yml").unlink()
#             shutil.rmtree(tmp_dir / "pages")
#             result = self.runner.invoke(["streamlit", "deploy", "--replace"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#         self.mock_create_stage.assert_called_once()
#         self.mock_put.assert_called_once_with(
#             local_file_name=(tmp_dir / "streamlit_app.py").resolve(),
#             stage_location="/test_streamlit/",
#             overwrite=True,
#             auto_compress=False,
#         )
#         mock_typer.launch.assert_not_called()
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit_v2", "example_streamlit"]
#     )
#     @mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
#     def test_deploy_launch_browser(self, mock_typer, project_name, setup):
#         with self.project_directory(project_name):
#             result = self.runner.invoke(["streamlit", "deploy", "--open"])
#
#         assert result.exit_code == 0, result.output
#
#         mock_typer.launch.assert_called_once_with(
#             f"https://snowsight.domain/test.region.aws/my_account/#/streamlit-apps/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}"
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit_v2", "example_streamlit"]
#     )
#     def test_deploy_streamlit_and_environment_files(self, project_name, setup):
#         with self.project_directory(project_name) as tmp_dir:
#             shutil.rmtree(tmp_dir / "pages")
#             if project_name == "example_streamlit_v2":
#                 self.alter_snowflake_yml(
#                     tmp_dir / "snowflake.yml",
#                     parameter_path="entities.test_streamlit.artifacts",
#                     value=["streamlit_app.py", "environment.yml"],
#                 )
#
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             f"select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["environment.yml", "streamlit_app.py"]
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit_v2", "example_streamlit"]
#     )
#     def test_deploy_streamlit_and_pages_files(self, project_name, setup):
#         with self.project_directory(project_name) as tmp_dir:
#             (tmp_dir / "environment.yml").unlink()
#             if project_name == "example_streamlit_v2":
#                 self.alter_snowflake_yml(
#                     tmp_dir / "snowflake.yml",
#                     parameter_path="entities.test_streamlit.artifacts",
#                     value=["streamlit_app.py", "pages/"],
#                 )
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             f"select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["pages/my_page.py", "streamlit_app.py"]
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["streamlit_full_definition_v2", "streamlit_full_definition"]
#     )
#     def test_deploy_all_streamlit_files(self, project_name, setup):
#         with self.project_directory(project_name) as tmp_dir:
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse;
#                 """
#             ).strip(),
#             f"select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             [
#                 "streamlit_app.py",
#                 "environment.yml",
#                 "pages/my_page.py",
#                 "utils/utils.py",
#                 "extra_file.py",
#             ]
#         )
#
#     @pytest.mark.parametrize(
#         "project_name, merge_definition",
#         [
#             (
#                 "example_streamlit_v2",
#                 {
#                     "entities": {
#                         "test_streamlit": {
#                             "stage": "streamlit_stage",
#                             "artifacts": [
#                                 "streamlit_app.py",
#                                 "environment.yml",
#                                 "pages/my_page.py",
#                             ],
#                         }
#                     }
#                 },
#             ),
#             ("example_streamlit", {"streamlit": {"stage": "streamlit_stage"}}),
#         ],
#     )
#     def test_deploy_put_files_on_stage(self, project_name, merge_definition, setup):
#         with self.project_directory(
#             project_name,
#             merge_project_definition=merge_definition,
#         ) as tmp_dir:
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                     CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                     ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}'
#                     MAIN_FILE = 'streamlit_app.py'
#                     QUERY_WAREHOUSE = test_warehouse
#                     TITLE = 'My Fancy Streamlit';
#                     """
#             ).strip(),
#             f"select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#             STREAMLIT_NAME,
#         )
#
#     @pytest.mark.parametrize(
#         "project_name",
#         ["example_streamlit_no_defaults", "example_streamlit_no_defaults_v2"],
#     )
#     def test_deploy_all_streamlit_files_not_defaults(self, project_name, setup):
#         with self.project_directory(project_name) as tmp_dir:
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}'
#                 MAIN_FILE = 'main.py'
#                 QUERY_WAREHOUSE = streamlit_warehouse;
#                 """
#             ).strip(),
#             f"select system$get_snowsight_host()",
#         ]
#
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             [
#                 "main.py",
#                 "streamlit_environment.yml",
#                 "streamlit_pages/first_page.py",
#             ],
#             STREAMLIT_NAME,
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit", "example_streamlit_v2"]
#     )
#     @pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
#     @pytest.mark.parametrize("enable_streamlit_no_checkouts", [True, False])
#     def test_deploy_streamlit_main_and_pages_files_experimental(
#         self,
#         os_agnostic_snapshot,
#         enable_streamlit_versioned_stage,
#         enable_streamlit_no_checkouts,
#         project_name,
#         setup,
#     ):
#         with (
#             mock.patch(
#                 "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
#                 return_value=enable_streamlit_versioned_stage,
#             ),
#             mock.patch(
#                 "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled",
#                 return_value=enable_streamlit_no_checkouts,
#             ),
#         ):
#             with self.project_directory(project_name) as tmp_dir:
#                 if project_name == "example_streamlit_v2":
#                     self.alter_snowflake_yml(
#                         tmp_dir / "snowflake.yml",
#                         parameter_path="entities.test_streamlit.artifacts",
#                         value=[
#                             "streamlit_app.py",
#                             "environment.yml",
#                             "pages/my_page.py",
#                         ],
#                     )
#                 result = self.runner.invoke(["streamlit", "deploy", "--experimental"])
#
#         if enable_streamlit_versioned_stage:
#             root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
#             post_create_command = f"ALTER STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}') ADD LIVE VERSION FROM LAST;"
#         else:
#             root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
#             if enable_streamlit_no_checkouts:
#                 post_create_command = None
#             else:
#                 post_create_command = f"ALTER STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}') CHECKOUT;"
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             cmd
#             for cmd in [
#                 dedent(
#                     f"""
#                    CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                    MAIN_FILE = 'streamlit_app.py'
#                    QUERY_WAREHOUSE = test_warehouse
#                    TITLE = 'My Fancy Streamlit';
#                    """
#                 ).strip(),
#                 post_create_command,
#                 "select system$get_snowsight_host()",
#             ]
#             if cmd is not None
#         ]
#
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#         )
#
#     @pytest.mark.skip(
#         reason="It doesn't test our logic, but if our mocks return Programming error. Potentialy we should delete"
#     )
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit", "example_streamlit_v2"]
#     )
#     def test_deploy_streamlit_main_and_pages_files_experimental_double_deploy(
#         self, mock_cursor, project_name, setup
#     ):
#         with self.project_directory(project_name) as pdir:
#             if project_name == "example_streamlit_v2":
#                 self.alter_snowflake_yml(
#                     pdir / "snowflake.yml",
#                     parameter_path="entities.test_streamlit.artifacts",
#                     value=["streamlit_app.py", "environment.yml"],
#                 )
#             result1 = self.runner.invoke(["streamlit", "deploy", "--experimental"])
#
#         assert result1.exit_code == 0, result1.output
#
#         # Reset to a fresh cursor, and clear the list of queries,
#         # keeping the same connection context
#         self.ctx.cs = mock_cursor(
#             rows=[
#                 {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
#                 {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
#             ],
#             columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
#         )
#         self.ctx.queries = []
#
#         with self.project_directory(project_name) as tmp_dir:
#             if project_name == "example_streamlit_v2":
#                 self.alter_snowflake_yml(
#                     tmp_dir / "snowflake.yml",
#                     parameter_path="entities.test_streamlit.artifacts",
#                     value=["streamlit_app.py", "environment.yml"],
#                 )
#             result2 = self.runner.invoke(["streamlit", "deploy", "--experimental"])
#
#         root_path = (
#             f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
#         )
#         assert result2.exit_code == 0, result2.output
#         # Same as normal, except no ALTER query
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit_no_stage", "example_streamlit_no_stage_v2"]
#     )
#     @pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
#     def test_deploy_streamlit_main_and_pages_files_experimental_no_stage(
#         self, enable_streamlit_versioned_stage, project_name, setup
#     ):
#         with mock.patch(
#             "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
#             return_value=enable_streamlit_versioned_stage,
#         ):
#             with self.project_directory(project_name) as tmp_dir:
#
#                 result = self.runner.invoke(["streamlit", "deploy", "--experimental"])
#
#         if enable_streamlit_versioned_stage:
#             root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
#             post_create_command = f"ALTER STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}') ADD LIVE VERSION FROM LAST;"
#         else:
#             root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
#             post_create_command = f"ALTER STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}') CHECKOUT;"
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse;
#                 """
#             ).strip(),
#             post_create_command,
#             f"select system$get_snowsight_host()",
#         ]
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#         )
#
#     @pytest.mark.parametrize(
#         "project_name", ["example_streamlit", "example_streamlit_v2"]
#     )
#     def test_deploy_streamlit_main_and_pages_files_experimental_replace(
#         self, project_name, setup
#     ):
#
#         with self.project_directory(project_name) as tmp_dir:
#             if project_name == "example_streamlit_v2":
#                 self.alter_snowflake_yml(
#                     tmp_dir / "snowflake.yml",
#                     parameter_path="entities.test_streamlit.artifacts",
#                     value=["streamlit_app.py", "environment.yml", "pages/"],
#                 )
#             result = self.runner.invoke(
#                 ["streamlit", "deploy", "--experimental", "--replace"]
#             )
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = test_warehouse
#                 TITLE = 'My Fancy Streamlit';
#                 """
#             ).strip(),
#             f"ALTER STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}') CHECKOUT;",
#             f"select system$get_snowsight_host()",
#         ]
#
#         root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#         )
#
#     def test_share_streamlit(self, setup):
#         role = "other_role"
#
#         result = self.runner.invoke(["streamlit", "share", STREAMLIT_NAME, role])
#
#         assert result.exit_code == 0, result.output
#         assert (
#             self.ctx.get_query()
#             == f"grant usage on streamlit IDENTIFIER('{STREAMLIT_NAME}') to role {role}"
#         )
#
#     def test_drop_streamlit(self, setup):
#
#         result = self.runner.invoke(["object", "drop", "streamlit", STREAMLIT_NAME])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_query() == f"drop streamlit IDENTIFIER('{STREAMLIT_NAME}')"
#
#     def test_get_streamlit_url(self, setup):
#         result = self.runner.invoke(["streamlit", "get-url", STREAMLIT_NAME])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == ["select system$get_snowsight_host()"]
#
#     @pytest.mark.parametrize(
#         "command, parameters",
#         [
#             ("list", []),
#             ("list", ["--like", "PATTERN"]),
#             ("describe", ["NAME"]),
#             ("drop", ["NAME"]),
#         ],
#     )
#     def test_command_aliases(self, command, parameters, setup):
#         result = self.runner.invoke(["object", command, "streamlit", *parameters])
#         assert result.exit_code == 0, result.output
#
#         result = self.runner.invoke(
#             ["streamlit", command, *parameters], catch_exceptions=False
#         )
#         assert result.exit_code == 0, result.output
#
#         queries = self.ctx.get_queries()
#         assert queries[0] == queries[1]
#
#     @pytest.mark.parametrize("entity_id", ["app_1", "app_2"])
#     def test_selecting_streamlit_from_pdf(self, entity_id, setup):
#
#         with self.project_directory("example_streamlit_multiple_v2"):
#             result = self.runner.invoke(["streamlit", "deploy", entity_id, "--replace"])
#
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#             CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{entity_id}')
#             ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/None'
#             MAIN_FILE = 'streamlit_app.py'
#             QUERY_WAREHOUSE = 'streamlit';"""
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", f"{entity_id}.py"], streamlit_name=entity_id
#         )
#
#     def test_multiple_streamlit_raise_error_if_multiple_entities(
#         self, os_agnostic_snapshot, setup
#     ):
#         with self.project_directory("example_streamlit_multiple_v2"):
#             result = self.runner.invoke(["streamlit", "deploy"])
#
#         assert result.exit_code == 2, result.output
#         assert result.output.strip() == os_agnostic_snapshot
#
#     def test_deploy_streamlit_with_comment_v2(self, setup):
#         with self.project_directory("example_streamlit_with_comment_v2") as tmp_dir:
#             result = self.runner.invoke(["streamlit", "deploy", "--replace"])
#
#         root_path = f"@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli"
#         assert result.exit_code == 0, result.output
#         assert self.ctx.get_queries() == [
#             dedent(
#                 f"""
#                 CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.test_streamlit_deploy_snowcli')
#                 ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli'
#                 MAIN_FILE = 'streamlit_app.py'
#                 QUERY_WAREHOUSE = xsmall
#                 TITLE = 'My Streamlit App with Comment'
#                 COMMENT = 'This is a test comment';
#                 """
#             ).strip(),
#             "select system$get_snowsight_host()",
#         ]
#         self.mock_create_stage.assert_called_once()
#         self._assert_that_exactly_those_files_were_put_to_stage(
#             ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
#             streamlit_name="test_streamlit_deploy_snowcli",
#         )
#
#     def test_execute_streamlit(self, setup):
#         result = self.runner.invoke(["streamlit", "execute", STREAMLIT_NAME])
#
#         assert result.exit_code == 0, result.output
#         assert result.output == f"Streamlit {STREAMLIT_NAME} executed.\n"
#         assert self.ctx.get_queries() == [
#             "EXECUTE STREAMLIT IDENTIFIER('test_streamlit')()"
#         ]
