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

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppManager,
    generic_sql_error_handler,
)
from snowflake.connector import ProgrammingError


def execute_sql_script(manager, sql_script_path):
    with open(sql_script_path) as f:
        sql_script = f.read()
        try:
            manager._execute_queries(sql_script)  # noqa: SLF001
        except ProgrammingError as err:
            generic_sql_error_handler(err)


def execute_post_deploy_hooks(manager: NativeAppManager):
    post_deploy_script_hooks = manager.app_post_deploy_hooks
    if post_deploy_script_hooks:
        for hook in post_deploy_script_hooks:
            if hook.sql_script:
                cc.step(
                    f"Executing application post-deploy SQL script: {hook.sql_script}"
                )
                execute_sql_script(manager, hook.sql_script)
            else:
                raise ValueError(
                    f"Unsupported application post-deploy hook type: {hook}"
                )
