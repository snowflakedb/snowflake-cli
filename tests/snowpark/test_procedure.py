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
from pathlib import Path
from textwrap import dedent
from unittest import mock
from unittest.mock import call

import pytest
from snowflake.cli._plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
)
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.identifiers import FQN
from snowflake.connector import ProgrammingError

from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Requires further refactor to work on Windows", allow_module_level=True)


mock_session_has_warehouse = mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin.session_has_warehouse",
    lambda _: True,
)


@mock_session_has_warehouse
def test_deploy_function_no_procedure(runner, project_directory):
    with project_directory("empty_project"):
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
        )
    assert result.exit_code == 1
    assert (
        "No procedures or functions were specified in the project definition."
        in result.output
    )


@pytest.mark.parametrize(
    "project_name", ["snowpark_procedures", "snowpark_procedures_v2"]
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name) as tmp:
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_om_describe.return_value(
        [
            call(object_type=str(ObjectType.PROCEDURE), name="procedureName(string)"),
            call(object_type=str(ObjectType.PROCEDURE), name="test()"),
        ]
    )
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        _put_query(
            Path(tmp),
            "my_snowpark_project/app.py",
            "@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/",
        ),
        dedent(
            """\
            create or replace procedure IDENTIFIER('MockDatabase.MockSchema.procedureName')(name string)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='hello'
            packages=()
            """
        ).strip(),
        dedent(
            """\
            create or replace procedure IDENTIFIER('MockDatabase.MockSchema.test')()
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='test'
            packages=()
            """
        ).strip(),
    ]


@pytest.mark.parametrize(
    "project_name",
    ["snowpark_procedure_external_access", "snowpark_procedure_external_access_v2"],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_with_external_access(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    mock_om_show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]

    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name) as project_dir:
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_om_describe.assert_has_calls(
        [
            call(
                object_type=str(ObjectType.PROCEDURE),
                fqn=FQN.from_string("MockDatabase.MockSchema.procedureName(string)"),
            ),
        ]
    )
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.dev_deployment') comment='deployments managed by Snowflake CLI'",
        _put_query(
            Path(project_dir),
            "my_snowpark_project/app.py",
            "@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/",
        ),
        dedent(
            """\
            create or replace procedure IDENTIFIER('MockDatabase.MockSchema.procedureName')(name string)
            copy grants
            returns string
            language python
            runtime_version=3.10
            imports=('@MockDatabase.MockSchema.dev_deployment/my_snowpark_project/app.py')
            handler='app.hello'
            packages=()
            external_access_integrations=(external_1, external_2)
            secrets=('cred'=cred_name, 'other'=other_name)
            """
        ).strip(),
    ]


@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_procedure_secrets_without_external_access",
        "snowpark_procedure_secrets_without_external_access_v2",
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_secrets_without_external_access(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    os_agnostic_snapshot,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    mock_om_show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
        {"name": "external_2", "type": "EXTERNAL_ACCESS"},
    ]

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 1, result.output
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize(
    "project_name",
    ["snowpark_procedure_external_access", "snowpark_procedure_external_access_v2"],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_fails_if_integration_does_not_exists(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    os_agnostic_snapshot,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    mock_om_show.return_value = [
        {"name": "external_1", "type": "EXTERNAL_ACCESS"},
    ]

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 1, result.output
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize(
    "project_name", ["snowpark_procedures", "snowpark_procedures_v2"]
)
@mock.patch(
    "snowflake.cli._plugins.snowpark.commands._check_if_all_defined_integrations_exists"
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_fails_if_object_exists_and_no_replace(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    _,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
    os_agnostic_snapshot,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.return_value = mock_cursor(
        [
            ("packages", "[]"),
            ("handler", "hello"),
            ("returns", "string"),
        ],
        columns=["key", "value"],
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy"])

    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize(
    "project_name", ["snowpark_procedures", "snowpark_procedures_v2"]
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_replace_nothing_to_update(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
    caplog,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
                ("imports", "dev_deployment/my_snowpark_project/app.py"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "test"),
                ("returns", "string"),
                ("imports", "dev_deployment/my_snowpark_project/app.py"),
                ("runtime_version", "3.10"),
            ],
            columns=["key", "value"],
        ),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {
            "object": "MockDatabase.MockSchema.test()",
            "status": "packages updated",
            "type": "procedure",
        },
    ]


@pytest.mark.parametrize(
    "project_name", ["snowpark_procedures", "snowpark_procedures_v2"]
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_replace_updates_single_object(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
                ("imports", "dev_deployment/my_snowpark_project/app.py"),
            ],
            columns=["key", "value"],
        ),
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "foo"),
                ("returns", "string"),
                ("imports", "dev_deployment/my_snowpark_project/app.zip"),
            ],
            columns=["key", "value"],
        ),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {
            "object": "MockDatabase.MockSchema.test()",
            "status": "definition updated",
            "type": "procedure",
        },
    ]


@pytest.mark.parametrize(
    "project_name", ["snowpark_procedures", "snowpark_procedures_v2"]
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_replace_creates_missing_object(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_cursor,
    mock_ctx,
    project_directory,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = [
        mock_cursor(
            [
                ("packages", "[]"),
                ("handler", "hello"),
                ("returns", "string"),
                ("imports", "dev_deployment/my_snowpark_project/app.py"),
            ],
            columns=["key", "value"],
        ),
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ]
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy", "--replace", "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": "MockDatabase.MockSchema.procedureName(name string)",
            "status": "packages updated",
            "type": "procedure",
        },
        {
            "object": "MockDatabase.MockSchema.test()",
            "status": "created",
            "type": "procedure",
        },
    ]


@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_procedure_fully_qualified_name",
        "snowpark_procedure_fully_qualified_name_v2",
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_fully_qualified_name(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    os_agnostic_snapshot,
    project_name,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    number_of_procedures_in_projects = 6
    mock_om_describe.side_effect = [
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ] * number_of_procedures_in_projects
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy"])
        assert result.output == os_agnostic_snapshot(name="database error")


@pytest.mark.parametrize(
    "project_name,parameter_path",
    [
        ("snowpark_procedure_fully_qualified_name", "snowpark.procedures.5.name"),
        (
            "snowpark_procedure_fully_qualified_name_v2",
            "entities.custom_database_custom_schema_fqn_procedure_error.identifier.name",
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_fully_qualified_name_duplicated_schema(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    os_agnostic_snapshot,
    project_name,
    parameter_path,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    number_of_procedures_in_projects = 6
    mock_om_describe.side_effect = [
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ] * number_of_procedures_in_projects
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory(project_name) as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path=parameter_path,
            value="custom_schema.fqn_procedure_error",
        )
        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy"])
        assert result.output == os_agnostic_snapshot(name="schema error")


@pytest.mark.parametrize(
    "parameter_type,default_value",
    [
        ("string", None),
        ("string", ""),
        ("int", None),
        ("variant", None),
        ("bool", None),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch(
    "snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages"
)
@mock_session_has_warehouse
def test_deploy_procedure_with_empty_default_value(
    mock_download,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    parameter_type,
    default_value,
    enable_snowpark_glob_support_feature_flag,
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("snowpark_procedures") as project_dir:
        snowflake_yml = project_dir / "snowflake.yml"
        for param, value in [("type", parameter_type), ("default", default_value)]:
            alter_snowflake_yml(
                snowflake_yml,
                parameter_path=f"snowpark.procedures.0.signature.0.{param}",
                value=value,
            )

        result = runner.invoke(
            [
                "snowpark",
                "build",
                "--ignore-anaconda",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        result = runner.invoke(["snowpark", "deploy", "--format", "json"])

    default_value_json = default_value
    if default_value is None:
        default_value_json = "null"
    elif parameter_type == "string":
        default_value_json = f"'{default_value}'"

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {
            "object": f"MockDatabase.MockSchema.procedureName(name {parameter_type} default {default_value_json})",
            "status": "created",
            "type": "procedure",
        },
        {
            "object": "MockDatabase.MockSchema.test()",
            "status": "created",
            "type": "procedure",
        },
    ]


@mock.patch("snowflake.connector.connect")
def test_execute_procedure(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "snowpark",
            "execute",
            "procedure",
            "procedureName(42, 'string')",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call procedureName(42, 'string')"


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "command, parameters",
    [
        ("list", []),
        ("list", ["--like", "PATTERN"]),
        ("describe", ["NAME"]),
        ("drop", ["NAME"]),
    ],
)
def test_command_aliases(mock_connector, runner, mock_ctx, command, parameters):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", command, "procedure", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["snowpark", command, "procedure", *parameters], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]


@mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin.session_has_warehouse",
    lambda _: False,
)
def test_snowpark_fail_if_no_active_warehouse(runner, mock_ctx, project_directory):
    with project_directory("snowpark_procedures"):
        result = runner.invoke(["snowpark", "deploy"])
    assert result.exit_code == 1, result.output
    assert (
        "The command requires warehouse. No warehouse found in current connection."
        in result.output
    )


def _put_query(project_root: Path, source: str, dest: str):
    return dedent(
        f"put file://{project_root.resolve() / 'output' / 'bundle' / 'snowpark' / source} {dest} auto_compress=false parallel=4 overwrite=True"
    )
