import json
from typing import Optional

import pytest

from tests_e2e.conftest import subprocess_check_output, subprocess_run


@pytest.fixture()
def _assert_json_output_matches_snapshot(snapshot):
    def f(cmd, stdin: Optional[str] = None):
        output = subprocess_check_output(cmd, stdin)
        parsed_json = json.loads(output)
        snapshot.assert_match(json.dumps(parsed_json))

    return f


@pytest.mark.e2e
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

    result = subprocess_run(
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
        ],
    )
    assert result.returncode == 0

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
def test_import_prompt_for_different_default_connection_name_on_conflict(
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

    result = subprocess_run(
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
            "--default-connection-name",
            "snowsql2",
        ],
        stdin="default\n",
    )
    assert result.returncode == 0

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
def test_import_confirm_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
    _assert_json_output_matches_snapshot,
):
    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ],
    )

    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "config",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "overriding_config",
        ],
        stdin="y\n",
    )
    assert result.returncode == 0

    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ],
    )


@pytest.mark.e2e
def test_import_reject_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
    _assert_json_output_matches_snapshot,
):
    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ],
    )

    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "config",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "overriding_config",
        ],
        stdin="n\n",
    )
    assert result.returncode == 0

    _assert_json_output_matches_snapshot(
        [
            snowcli,
            "--config-file",
            example_connection_config_file,
            "connection",
            "list",
            "--format",
            "json",
        ],
    )


@pytest.mark.e2e
def test_connection_imported_from_snowsql(snowcli, test_root_path, empty_config_file):
    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            empty_config_file,
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            test_root_path / "config" / "snowsql" / "integration_config",
        ],
    )
    assert result.returncode == 0

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
