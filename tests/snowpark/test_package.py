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

import logging
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
)
from snowflake.cli._plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
)

from tests.snowpark.mocks import mock_available_packages_sql_result  # noqa: F401


class TestPackage:
    @pytest.mark.parametrize(
        "argument",
        [
            "snowflake.core",
            "some-weird-package-we-dont-know",
            "package-with-non-pep-version",
        ],
    )
    def test_package_lookup(
        self,
        argument,
        monkeypatch,
        runner,
        os_agnostic_snapshot,
        mock_available_packages_sql_result,
    ) -> None:
        result = runner.invoke(["snowpark", "package", "lookup", argument])

        assert result.exit_code == 0
        assert result.output == os_agnostic_snapshot

    @patch(
        "snowflake.cli._plugins.snowpark.package.commands.download_unavailable_packages"
    )
    @patch(
        "snowflake.cli._plugins.snowpark.package_utils.pip_wheel",
    )
    @pytest.mark.parametrize(
        "extra_flags", [[], ["--skip-version-check"], ["--ignore-anaconda"]]
    )
    def test_package_create(
        self,
        mock_pip_wheel,
        mock_download,
        caplog,
        temp_dir,
        runner,
        extra_flags,
        mock_available_packages_sql_result,
    ) -> None:
        mock_pip_wheel.return_value = 9

        mock_download.return_value = DownloadUnavailablePackagesResult(
            succeeded=True,
            anaconda_packages=[Requirement.parse("in-anaconda-package>=2")],
        )

        with caplog.at_level(
            logging.DEBUG, logger="snowflake.cli._plugins.snowpark.package"
        ):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package"]
                + extra_flags
            )

        assert result.exit_code == 0, result.output
        assert "in-anaconda-package>=2" in result.output
        assert os.path.isfile("totally-awesome-package.zip"), result.output

    @mock.patch("snowflake.cli._plugins.snowpark.package.manager.StageManager")
    @mock.patch("snowflake.cli._plugins.snowpark.package.manager.FQN.from_string")
    @mock.patch("snowflake.connector.connect")
    def test_package_upload(
        self,
        mock_connector,
        _,
        mock_stage_manager,
        package_file: str,
        runner,
        mock_ctx,
        mock_cursor,
    ) -> None:
        ctx = mock_ctx()
        mock_connector.return_value = ctx
        mock_stage_manager().put.return_value = mock_cursor(
            rows=[("", "", "", "", "", "", "UPLOADED")], columns=[]
        )

        result = runner.invoke(
            ["snowpark", "package", "upload", "-f", package_file, "-s", "stageName"]
        )

        assert result.exit_code == 0
        assert ctx.get_query() == ""

    @mock.patch(
        "snowflake.cli._plugins.snowpark.package.manager.StageManager._execute_query"
    )
    def test_package_upload_to_path(
        self,
        mock_execute_queries,
        package_file: str,
        runner,
        mock_ctx,
        mock_cursor,
    ) -> None:
        mock_execute_queries.return_value = MagicMock()

        result = runner.invoke(
            [
                "snowpark",
                "package",
                "upload",
                "-f",
                package_file,
                "-s",
                "db.schema.stage/path/to/file",
            ]
        )

        assert result.exit_code == 0
        assert mock_execute_queries.call_count == 2
        create, put = mock_execute_queries.call_args_list
        assert (
            create.args[0] == "create stage if not exists IDENTIFIER('db.schema.stage')"
        )
        assert "db.schema.stage/path/to/file" in put.args[0]

    @patch(
        "snowflake.cli._plugins.snowpark.package.commands.AnacondaPackagesManager.find_packages_available_in_snowflake_anaconda"
    )
    def test_lookup_install_without_flags_does_not_warn(
        self, _, runner, mock_available_packages_sql_result
    ):
        result = runner.invoke(["snowpark", "package", "lookup", "foo"])
        assert (
            "is deprecated. Lookup command no longer checks for package in PyPi"
            not in result.output
        )

    @mock.patch(
        "snowflake.cli._plugins.snowpark.package.commands.download_unavailable_packages"
    )
    @mock.patch("snowflake.cli._plugins.snowpark.package.commands.zip_dir")
    @mock.patch(
        "snowflake.cli._plugins.snowpark.package.commands.get_package_name_from_pip_wheel"
    )
    def test_create_without_flags_does_not_warn(
        self,
        _mock_pip_wheel,
        _mock_zip,
        _mock_download,
        runner,
        mock_available_packages_sql_result,
    ):
        result = runner.invoke(["snowpark", "package", "create", "foo"])
        assert "is deprecated" not in result.output

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
