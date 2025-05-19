# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid
from pathlib import Path
from textwrap import dedent

import pytest

from tests_common import skip_snowpark_on_newest_python
from tests_e2e.conftest import subprocess_check_output, subprocess_run


def subprocess_check_output_with(sql_stmt: str, config_path: Path, snowcli) -> str:
    return subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-q",
            sql_stmt,
            "--format",
            "JSON",
            "-c",
            "integration",
        ],
    )


def assert_snapshot_match_with_query_result(output: str, snapshot) -> bool:
    """
    This function is required to parse the result value in the output independently from the uuid-based object created per test run.
    E.g.
    With the default format:
    '''
    select codegen_nativeapp_af3d785601654fa0a9a105883b626866_app.ext_code_schema.py_echo_fn('test')
    +------------------------------------------------------------------------------+
    | CODEGEN_NATIVEAPP_AF3D785601654FA0A9A105883B626866_APP.EXT_CODE_SCHEMA.PY_EC |
    | HO_FN('TEST')                                                                |
    |------------------------------------------------------------------------------|
    | echo_fn: test                                                                |
    +------------------------------------------------------------------------------+
    '''
    If the --format is JSON, then the output is like such:
    '''
    [
        {
            "CODEGEN_NATIVEAPP_E4B7C94AE8F74AF39D387CD44C4854E3_APP.EXT_CODE_SCHEMA.SUM_INT_DEC(10)": 10
        }
    ]
    '''
    With forced --format JSON, we can fetch only the result value reliably and compare against the snapshot.
    """
    myjson = json.loads(output)[0]
    return snapshot.assert_match(myjson.values())


@pytest.mark.e2e
@skip_snowpark_on_newest_python
def test_full_lifecycle_with_codegen(
    snowcli, test_root_path, project_directory, snapshot
):
    config_path = test_root_path / "config" / "config.toml"
    # FYI: when testing locally and you want to quickly get this running without all the setup,
    # remove the e2e marker and reroute the config path to the config.toml on your filesystem.

    project_name = "nativeapp"
    base_name = f"codegen_nativeapp_{uuid.uuid4().hex}"
    package_name = f"{base_name}_pkg"
    app_name = f"{base_name}_app"
    snowflake_yml_contents = dedent(
        f"""
        definition_version: 1

        native_app:
            name: codegen_nativeapp
            package:
                name: {package_name}
            application:
                name: {app_name}
            artifacts:
                - src: root_files/README.md
                  dest: README.md
                - src: resources/*
                - src: root_files/_manifest.yml
                  dest: manifest.yml
                - src: root_files/setup_scripts/*
                  dest: setup_scripts/
                - src: python/user_gen/echo.py
                  dest: user_gen/echo.py
                - src: python/cli_gen/*
                  dest: cli_gen/
                  processors: 
                    - snowpark
            """
    )

    with project_directory(project_name) as project_dir:
        with open("snowflake.yml", "w") as f:
            # Redo snowflake.yml to add unique id to project name
            f.write(snowflake_yml_contents)

        try:
            # App Run includes bundle
            result = subprocess_run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "run",
                    "-c",
                    "integration",
                ],
            )

            assert result.returncode == 0

            app_name_and_schema = f"{app_name}.ext_code_schema"

            # Disable debug mode to call functions and procedures.
            # This ensures all usage permissions have been granted accordingly.
            result = subprocess_run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "sql",
                    "-q",
                    f"alter application {app_name} set debug_mode = false",
                    "-c",
                    "integration",
                ]
            )
            assert result.returncode == 0

            # Test ext code that user wrote manually

            output = subprocess_check_output_with(
                sql_stmt=f"call {app_name_and_schema}.py_echo_proc('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.py_echo_fn('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            # User wrote ext code using codegen feature
            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_1('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_2('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_4('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            output = subprocess_check_output_with(
                sql_stmt=f"call {app_name_and_schema}.add_sp(1, 2)",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            # code gen UDAF
            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.sum_int_dec(10)",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            # code gen UDTF
            output = subprocess_check_output_with(
                sql_stmt=f"select * from TABLE({app_name_and_schema}.alt_int(10))",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            # Bundle is idempotent if no changes made to source files.
            result = subprocess_run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "run",
                    "-c",
                    "integration",
                ]
            )

            assert result.returncode == 0

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_1('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

            # UDTF should exist as only its deploy/root file was de-annotated, but the source should be discovered always
            output = subprocess_check_output_with(
                sql_stmt=f"select * from TABLE({app_name_and_schema}.alt_int(10))",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            # code gen UDAF
            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.sum_int_dec(10)",
                config_path=config_path,
                snowcli=snowcli,
            )
            assert_snapshot_match_with_query_result(output, snapshot)

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = subprocess_run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "teardown",
                    "--force",
                    "-c",
                    "integration",
                ]
            )
            assert result.returncode == 0
