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
import logging
import os
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest
import snowflake.cli._plugins.snowpark.models
import snowflake.cli._plugins.snowpark.package.utils
from snowflake.cli._plugins.connection.util import (
    LOCAL_DEPLOYMENT_REGION,
    get_context,
    get_host_region,
    guess_regioned_host_from_allowlist,
    make_snowsight_url,
)
from snowflake.cli._plugins.snowpark import package_utils
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
)
from snowflake.cli.api.project.util import identifier_for_url
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils import path_utils
from snowflake.connector import SnowflakeConnection

from tests.test_data import test_data


def test_prepare_app_zip(
    temp_dir,
    app_zip: str,
    temp_directory_for_app_zip: str,
):
    result = (
        snowflake.cli._plugins.snowpark.package.utils.prepare_app_zip(  # noqa: SLF001
            SecurePath(app_zip), SecurePath(temp_directory_for_app_zip)
        )
    )
    assert str(result.path) == os.path.join(
        temp_directory_for_app_zip, Path(app_zip).name
    )


def test_prepare_app_zip_if_exception_is_raised_if_no_source(
    temp_directory_for_app_zip,
):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli._plugins.snowpark.package.utils.prepare_app_zip(  # noqa: SLF001
            SecurePath("/non/existent/path"), SecurePath(temp_directory_for_app_zip)
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_prepare_app_zip_if_exception_is_raised_if_no_dst(app_zip):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli._plugins.snowpark.package.utils.prepare_app_zip(  # noqa: SLF001
            SecurePath(app_zip), SecurePath("/non/existent/path")
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_parse_requirements_with_correct_file(
    correct_requirements_snowflake_txt: str, temp_dir
):
    result = package_utils.parse_requirements(
        SecurePath(correct_requirements_snowflake_txt)
    )

    assert len(result) == len(test_data.requirements)


def test_parse_requirements_with_nonexistent_file(temp_dir):
    path = os.path.join(temp_dir, "non_existent.file")
    result = package_utils.parse_requirements(SecurePath(path))

    assert result == []


@pytest.mark.parametrize(
    "contents, expected",
    [
        (
            """pytest==1.0.0\nDjango==3.2.1\nawesome_lib==3.3.3""",
            ["pytest==1.0.0", "django==3.2.1", "awesome_lib==3.3.3"],
        ),
        ("""toml # some-comment""", ["toml"]),
        ("", []),
        ("""some-package==1.2.3#incorrect_comment""", ["some_package==1.2.3"]),
        ("""#only comment here""", []),
        (
            """pytest==1.0\n# comment\nawesome_lib==3.3.3""",
            ["pytest==1.0", "awesome_lib==3.3.3"],
        ),
    ],
)
@mock.patch("snowflake.cli._plugins.snowpark.package_utils.SecurePath.read_text")
def test_parse_requirements_corner_cases(
    mock_file, contents, expected, correct_requirements_snowflake_txt
):
    mock_file.return_value = contents
    result = [
        p.name_and_version
        for p in package_utils.parse_requirements(
            SecurePath(correct_requirements_snowflake_txt)
        )
    ]
    mock_file.assert_called_with(file_size_limit_mb=128)
    assert result == expected


def test_parse_requirements(correct_requirements_txt: str):
    result = package_utils.parse_requirements(SecurePath(correct_requirements_txt))
    result.sort(key=lambda r: r.name)

    assert len(result) == 3
    assert result[0].name == "pytest"
    assert result[0].specifier is True
    assert result[0].specs == [("==", "1.0.0")]
    assert result[1].name == "snowflake_connector_python"
    assert result[1].specifier is True
    assert result[1].specs == [("<", "3.3.3")]
    assert result[2].name == "snowflake_sqlalchemy"
    assert result[2].specifier is True
    assert result[2].specs == [(">=", "3.2.1")]


@patch("platform.system")
@pytest.mark.parametrize(
    "argument, expected",
    [
        ("C:\\Something\\Something Else", "C:\\Something\\Something Else"),
        (
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
        ),
    ],
)
def test_path_resolver(mock_system, argument, expected):
    mock_system.response_value = "Windows"

    assert path_utils.path_resolver(argument) == expected


@patch("snowflake.cli._plugins.snowpark.package_utils.pip_wheel")
def test_pip_fail_message(mock_installer, correct_requirements_txt, caplog):
    mock_installer.return_value = 42

    with caplog.at_level(logging.INFO, "snowflake.cli._plugins.snowpark.package_utils"):
        requirements = package_utils.parse_requirements(
            SecurePath(correct_requirements_txt)
        )
        package_utils.download_unavailable_packages(
            requirements=requirements,
            target_dir=SecurePath(".packages"),
            anaconda_packages=AnacondaPackages.empty(),
        )

    assert "pip failed with return code 42" in caplog.text


@pytest.mark.parametrize(
    "identifier, expected",
    [
        ("my_app", "MY_APP"),
        ('"My App"', "My%20App"),
        ("SYSTEM$GET", "SYSTEM%24GET"),
        ("mailorder_!@#$%^&*()/_app", "MAILORDER_!%40%23%24%25%5E%26*()%2F_APP"),
        ('"Mailorder *App* is /cool/"', "Mailorder%20*App*%20is%20%2Fcool%2F"),
    ],
)
def test_identifier_for_url(identifier, expected):
    assert identifier_for_url(identifier) == expected


@patch("snowflake.cli._plugins.connection.util.get_account")
@patch("snowflake.cli._plugins.connection.util.get_context")
@patch("snowflake.cli._plugins.connection.util.get_snowsight_host")
@pytest.mark.parametrize(
    "context, account, path, expected",
    [
        (
            "org",
            "account",
            "UNQUOTED",
            "https://app.snowflake.com/org/account/UNQUOTED",
        ),
        (
            "host",
            identifier_for_url("some$account"),
            identifier_for_url('"Quoted App Name"'),
            "https://app.snowflake.com/host/SOME%24ACCOUNT/Quoted%20App%20Name",
        ),
        (
            "a",
            "b",
            f"""/some/path/{identifier_for_url('"on the server"')}""",
            "https://app.snowflake.com/a/b/some/path/on%20the%20server",
        ),
        (
            "myorg",
            "myacct",
            "/#/apps/application/MAILORDER_CGORRIE",
            "https://app.snowflake.com/myorg/myacct/#/apps/application/MAILORDER_CGORRIE",
        ),
    ],
)
def test_make_snowsight_url(
    get_snowsight_host, get_context, get_account, context, account, path, expected
):
    get_snowsight_host.return_value = "https://app.snowflake.com"
    get_context.return_value = context
    get_account.return_value = account
    actual = make_snowsight_url(None, path)  # all uses of conn are mocked
    assert actual == expected


@pytest.mark.parametrize(
    "allowlist, expected",
    [
        (
            [
                {
                    "host": "myacct.x.y.z.snowflakecomputing.com",
                    "type": "SNOWFLAKE_DEPLOYMENT",
                },
                {
                    "host": "myacct.nonregioned.snowflakecomputing.com",
                    "type": "SNOWFLAKE_DEPLOYMENT",
                },
                {"type": "unrelated"},
            ],
            "myacct.x.y.z.snowflakecomputing.com",
        ),
        (
            [
                {"type": "unrelated"},
            ],
            None,
        ),
    ],
)
def test_guess_regioned_host_from_allowlist(allowlist, expected, mock_cursor):
    mock_conn = mock.MagicMock(spec=SnowflakeConnection)
    mock_conn.execute_string.return_value = (
        None,
        mock_cursor([{"SYSTEM$ALLOWLIST()": json.dumps(allowlist)}], []),
    )
    assert guess_regioned_host_from_allowlist(mock_conn) == expected


@patch("snowflake.cli._plugins.connection.util.is_regionless_redirect")
@patch("snowflake.cli._plugins.connection.util.guess_regioned_host_from_allowlist")
def test_get_context_non_regionless_uses_region(
    guess_regioned_host_from_allowlist, is_regionless_redirect
):
    mock_conn = mock.MagicMock(spec=SnowflakeConnection)
    mock_conn.host = "myacct.regionless.snowflakecomputing.com"
    guess_regioned_host_from_allowlist.return_value = (
        "myacct.x.y.z.snowflakecomputing.com"
    )
    is_regionless_redirect.return_value = False
    assert get_context(mock_conn) == "x.y.z"


@patch("snowflake.cli._plugins.connection.util.is_regionless_redirect")
@patch("snowflake.cli._plugins.connection.util.guess_regioned_host_from_allowlist")
def test_get_context_local_non_regionless_gets_local_region(
    guess_regioned_host_from_allowlist, is_regionless_redirect
):
    mock_conn = mock.MagicMock(spec=SnowflakeConnection)
    mock_conn.host = "some.dns.local"
    is_regionless_redirect.return_value = False
    assert get_host_region(mock_conn.host) == LOCAL_DEPLOYMENT_REGION
    assert get_context(mock_conn) == LOCAL_DEPLOYMENT_REGION
    guess_regioned_host_from_allowlist.assert_not_called()


@pytest.mark.parametrize(
    "host, expected",
    [
        ("some.dns.local", LOCAL_DEPLOYMENT_REGION),
        ("org-acct.mydns.snowflakecomputing.com", None),
        ("account.x.us-west-2.aws.snowflakecomputing.com", "x.us-west-2.aws"),
        ("naf_test_pc.us-west-2.snowflakecomputing.com", None),
        ("test_account.az.int.snowflakecomputing.com", None),
        ("frozenweb.prod3.external-zone.snowflakecomputing.com", None),
    ],
)
def test_get_host_region(host, expected):
    assert get_host_region(host) == expected
