import json
import os
from unittest import mock

import pytest

from tests_e2e.conftest import subprocess_check_output, subprocess_run


@pytest.fixture()
def _assert_json_output_matches_snapshot(snapshot):
    def f(cmd):
        output = subprocess_check_output(cmd)
        parsed_json = json.loads(output)
        snapshot.assert_match(json.dumps(parsed_json))

    return f


@pytest.mark.e2e
@mock.patch.dict(os.environ, {}, clear=True)
def test_import_of_snowsql_connections(
    snowcli, test_root_path, empty_config_file, _assert_json_output_matches_snapshot
):
    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ],
    )

    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "config",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "overriding_config",
            "--format",
            "json",
        ],
    )

    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ]
    )


@pytest.mark.e2e
@mock.patch.dict(os.environ, {}, clear=True)
def test_import_error_on_conflict_with_existing_cli_connection(
    snowcli, test_root_path, config_file, snapshot
):
    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "integration_config",
            "--format",
            "json",
        ],
    )

    assert result.returncode == 1
    snapshot.assert_match(result.stderr)


@pytest.mark.e2e
def test_connection_imported_from_snowsql(
    snowcli, test_root_path, empty_config_file, _assert_json_output_matches_snapshot
):
    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "integration_config",
            "--format",
            "json",
        ],
    )

    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "connection",
            "test",
            "-c",
            "integration",
            "--format",
            "json",
        ],
    )
    assert result.returncode == 0
