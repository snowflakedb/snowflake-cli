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

import subprocess
import uuid
from pathlib import Path
from textwrap import dedent


def subprocess_check_output_with(sql_stmt: str, config_path: Path, snowcli) -> str:
    return subprocess.check_output(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-q",
            sql_stmt,
            # "-c",
            # "integration",
        ],
        encoding="utf-8",
    )


# @pytest.mark.e2e
def test_full_lifecycle_with_codegen(
    snowcli, test_root_path, project_directory, snapshot
):
    # config_path = test_root_path/ "config"/ "config.toml"
    config_path = Path("/Users/bgoel/.snowflake/config.toml")

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
            result = subprocess.run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "run",
                    # "-c",
                    # "integration",
                ],
                encoding="utf-8",
                capture_output=True,
                text=True,
            )

            assert result.stderr == ""
            assert result.returncode == 0
            # assert_dir_snapshot(project_dir, snapshot)

            app_name_and_schema = f"{app_name}.ext_code_schema"

            # Disable debug mode to call functions and procedures
            result = subprocess.run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "sql",
                    "-q",
                    f"alter application {app_name} set debug_mode = false",
                    # "-c",
                    # "integration",
                ],
                encoding="utf-8",
            )
            assert result.returncode == 0

            # Test ext code that user wrote manually

            output = subprocess_check_output_with(
                sql_stmt=f"call {app_name_and_schema}.py_echo_proc('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.py_echo_fn('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            # User wrote ext code using codegen feature
            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_1('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_2('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_4('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            output = subprocess_check_output_with(
                sql_stmt=f"call {app_name_and_schema}.add_sp(1, 2)",
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
            snapshot.assert_match(output)

            # code gen UDTF
            output = subprocess_check_output_with(
                sql_stmt=f"select * from TABLE({app_name_and_schema}.alt_int(10))",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

            # Bundle is idempotent if no changes made to source files.
            result = subprocess.run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "run",
                    # "-c",
                    # "integration",
                ],
                encoding="utf-8",
                capture_output=True,
                text=True,
            )

            assert result.stderr == ""
            assert result.returncode == 0

            output = subprocess_check_output_with(
                sql_stmt=f"select {app_name_and_schema}.echo_fn_1('test')",
                config_path=config_path,
                snowcli=snowcli,
            )
            snapshot.assert_match(output)

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
            snapshot.assert_match(output)

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = subprocess.run(
                [
                    snowcli,
                    "--config-file",
                    config_path,
                    "app",
                    "teardown",
                    "--force",
                    #     "-c",
                    #     "integration",
                ],
                encoding="utf-8",
            )
            assert result.returncode == 0
