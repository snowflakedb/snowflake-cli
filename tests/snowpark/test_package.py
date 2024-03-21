import logging
import os
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    SplitRequirements,
)

from tests.test_data import test_data


class TestPackage:
    @pytest.mark.parametrize(
        "argument",
        ["snowflake-connector-python", "some-weird-package-we-dont-know"],
    )
    @patch("snowflake.cli.plugins.snowpark.package.anaconda.requests")
    def test_package_lookup(
        self, mock_requests, argument, monkeypatch, runner, snapshot
    ) -> None:
        mock_requests.get.return_value = self.mocked_anaconda_response(
            test_data.anaconda_response
        )

        result = runner.invoke(["snowpark", "package", "lookup", argument])

        assert result.exit_code == 0
        assert result.output == snapshot

    @patch("snowflake.cli.plugins.snowpark.package_utils.download_packages")
    @patch(
        "snowflake.cli.plugins.snowpark.package.anaconda.AnacondaChannel.parse_anaconda_packages"
    )
    def test_package_lookup_with_install_packages(
        self, mock_package, mock_download, runner, capfd, snapshot
    ) -> None:

        mock_package.return_value = SplitRequirements(
            [], [Requirement("some-other-package")]
        )
        mock_download.return_value = (
            True,
            SplitRequirements(
                [Requirement("snowflake-snowpark-python")],
                [Requirement("some-other-package")],
            ),
        )

        result = runner.invoke(["snowpark", "package", "lookup", "some-other-package"])
        assert result.exit_code == 0
        assert result.output == snapshot

    @patch("snowflake.cli.plugins.snowpark.package.commands.download_packages")
    @patch(
        "snowflake.cli.plugins.snowpark.package.commands.get_anaconda_from_snowflake"
    )
    @pytest.mark.parametrize(
        "extra_flags", [[], ["--skip-version-check"], ["--ignore-anaconda"]]
    )
    def test_package_create(
        self,
        mock_get_anaconda,
        mock_download,
        caplog,
        temp_dir,
        dot_packages_directory,
        runner,
        extra_flags,
    ) -> None:

        mock_anaconda = MagicMock(name="anaconda")
        mock_anaconda.is_package_available.return_value = False
        mock_get_anaconda.return_value = mock_anaconda
        mock_download.return_value = (
            True,
            SplitRequirements(
                [Requirement.parse("in-anaconda-package>=2")],
                [Requirement.parse("some-other-package")],
            ),
        )

        with caplog.at_level(
            logging.DEBUG, logger="snowflake.cli.plugins.snowpark.package"
        ):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package"]
                + extra_flags
            )

        assert result.exit_code == 0, result.output
        assert "in-anaconda-package>=2" in result.output
        assert "some-other-package" not in result.output
        assert os.path.isfile("totally-awesome-package.zip"), result.output

        zip_file = ZipFile("totally-awesome-package.zip", "r")
        assert (
            "totally-awesome-package/totally-awesome-module.py" in zip_file.namelist()
        )
        os.remove("totally-awesome-package.zip")

        if "--ignore-anaconda" in extra_flags:
            expected_anaconda = expected_get_anaconda = []
        else:
            expected_get_anaconda = [mock.call()]
            expected_anaconda = [
                mock.call.is_package_available(
                    Requirement.parse("totally-awesome-package"),
                    skip_version_check="--skip-version-check" in extra_flags,
                )
            ]
        assert mock_anaconda.mock_calls == expected_anaconda
        assert mock_get_anaconda.mock_calls == expected_get_anaconda

    @mock.patch("snowflake.cli.plugins.snowpark.package.manager.StageManager")
    @mock.patch("snowflake.connector.connect")
    def test_package_upload(
        self,
        mock_connector,
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
        "snowflake.cli.plugins.snowpark.package.manager.StageManager._execute_query"
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
        assert create.args[0] == "create stage if not exists db.schema.stage"
        assert "db.schema.stage/path/to/file" in put.args[0]

    @pytest.mark.parametrize(
        "flags",
        [
            ["--pypi-download"],
            ["-y"],
            ["--yes"],
            ["--pypi-download", "-y"],
        ],
    )
    @mock.patch(
        "snowflake.cli.plugins.snowpark.package.commands.get_anaconda_from_snowflake"
    )
    def test_lookup_install_flag_are_deprecated(self, _, flags, runner):
        result = runner.invoke(["snowpark", "package", "lookup", "foo", *flags])
        assert (
            "is deprecated. Lookup command no longer checks for package in PyPi"
            in result.output
        )

    @pytest.mark.parametrize(
        "flags",
        [
            ["--pypi-download"],
            ["-y"],
            ["--yes"],
            ["--pypi-download", "-y"],
        ],
    )
    @mock.patch(
        "snowflake.cli.plugins.snowpark.package.commands.get_anaconda_from_snowflake"
    )
    def test_create_install_flag_are_deprecated(self, _, flags, runner):
        result = runner.invoke(["snowpark", "package", "create", "foo", *flags])
        assert (
            "is deprecated. Create command always checks for package in PyPi."
            in result.output
        )

    @mock.patch(
        "snowflake.cli.plugins.snowpark.package.commands.get_anaconda_from_snowflake"
    )
    def test_lookup_install_with_out_flags_does_not_warn(self, _, runner):
        result = runner.invoke(["snowpark", "package", "lookup", "foo"])
        assert (
            "is deprecated. Lookup command no longer checks for package in PyPi"
            not in result.output
        )

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
