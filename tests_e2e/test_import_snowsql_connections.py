import json
from typing import Optional

import pytest

from tests_e2e.conftest import subprocess_check_output, subprocess_run


def _get_connections_list(snowcli, config_file) -> list:
    """Helper function to get connections list as parsed JSON."""
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "connection",
            "list",
            "--format",
            "json",
        ]
    )
    return json.loads(output)


def _find_connection(connections: list, name: str) -> Optional[dict]:
    """Helper function to find a connection by name."""
    for conn in connections:
        if conn["connection_name"] == name:
            return conn
    return None


@pytest.mark.e2e
def test_import_of_snowsql_connections(snowcli, test_root_path, empty_config_file):
    # Initially should have empty or minimal connections list
    initial_connections = _get_connections_list(snowcli, empty_config_file)
    initial_count = len(initial_connections)

    # Import snowsql connections
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

    # After import, should have multiple connections
    final_connections = _get_connections_list(snowcli, empty_config_file)

    # Should have more connections than initially
    assert len(final_connections) > initial_count

    # Check that expected connections exist
    connection_names = {conn["connection_name"] for conn in final_connections}
    expected_names = {"snowsql1", "snowsql2", "snowsql3", "example", "default"}
    assert expected_names.issubset(connection_names)

    # Check specific connection details
    snowsql1 = _find_connection(final_connections, "snowsql1")
    assert snowsql1 is not None
    assert snowsql1["parameters"]["account"] == "a1"
    assert snowsql1["parameters"]["user"] == "u1"
    assert snowsql1["parameters"]["host"] == "h1_override"  # From overriding config
    assert snowsql1["is_default"] is False

    snowsql2 = _find_connection(final_connections, "snowsql2")
    assert snowsql2 is not None
    assert snowsql2["parameters"]["account"] == "a2"
    assert snowsql2["parameters"]["port"] == 1234
    assert snowsql2["is_default"] is False

    default_conn = _find_connection(final_connections, "default")
    assert default_conn is not None
    assert default_conn["parameters"]["account"] == "default_connection_account"
    assert (
        default_conn["parameters"]["database"] == "default_connection_database_override"
    )  # From overriding config
    assert default_conn["is_default"] is True


@pytest.mark.e2e
def test_import_prompt_for_different_default_connection_name_on_conflict(
    snowcli, test_root_path, empty_config_file
):
    # Initially should have empty or minimal connections list
    initial_connections = _get_connections_list(snowcli, empty_config_file)
    initial_count = len(initial_connections)

    # Import with different default connection name
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

    # After import, snowsql2 should be the default
    final_connections = _get_connections_list(snowcli, empty_config_file)

    # Should have more connections than initially
    assert len(final_connections) > initial_count

    snowsql2 = _find_connection(final_connections, "snowsql2")
    assert snowsql2 is not None
    assert snowsql2["is_default"] is True

    default_conn = _find_connection(final_connections, "default")
    assert default_conn is not None
    assert default_conn["is_default"] is False


@pytest.mark.e2e
def test_import_confirm_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
):
    # Initially should have example and integration connections
    initial_connections = _get_connections_list(snowcli, example_connection_config_file)

    example_conn = _find_connection(initial_connections, "example")
    assert example_conn is not None
    assert example_conn["parameters"]["user"] == "u1"
    assert example_conn["parameters"]["authenticator"] == "SNOWFLAKE_JWT"

    integration_conn = _find_connection(initial_connections, "integration")
    assert integration_conn is not None

    # Import with confirmation (y)
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

    # After import, example connection should be overwritten with snowsql data
    final_connections = _get_connections_list(snowcli, example_connection_config_file)

    example_conn = _find_connection(final_connections, "example")
    assert example_conn is not None
    assert example_conn["parameters"]["account"] == "accountname"
    assert example_conn["parameters"]["user"] == "username"
    # Should not have the old JWT authenticator
    assert "authenticator" not in example_conn["parameters"]


@pytest.mark.e2e
def test_import_reject_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
):
    # Initially should have example and integration connections
    initial_connections = _get_connections_list(snowcli, example_connection_config_file)

    example_conn = _find_connection(initial_connections, "example")
    assert example_conn is not None
    original_user = example_conn["parameters"]["user"]
    original_auth = example_conn["parameters"]["authenticator"]

    # Import with rejection (n)
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

    # After import, example connection should remain unchanged
    final_connections = _get_connections_list(snowcli, example_connection_config_file)

    example_conn = _find_connection(final_connections, "example")
    assert example_conn is not None
    assert example_conn["parameters"]["user"] == original_user
    assert example_conn["parameters"]["authenticator"] == original_auth

    # But other connections should still be imported
    snowsql1 = _find_connection(final_connections, "snowsql1")
    assert snowsql1 is not None
    assert snowsql1["parameters"]["account"] == "a1"


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

    # Test that the imported integration connection works
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
