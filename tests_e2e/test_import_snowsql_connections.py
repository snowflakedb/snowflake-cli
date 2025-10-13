import json

import pytest

from tests_e2e.conftest import subprocess_check_output, subprocess_run


def _get_connections_list_output(snowcli, config_file) -> str:
    """Helper function to get connections list output as string."""
    return subprocess_check_output(
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


def _parse_connections(output: str) -> list:
    """Parse connection list JSON output."""
    return json.loads(output)


def _assert_connection_structure(connection: dict) -> None:
    """Assert that a connection has the expected structure."""
    assert "connection_name" in connection
    assert "parameters" in connection
    assert "is_default" in connection
    assert isinstance(connection["parameters"], dict)
    assert isinstance(connection["is_default"], bool)


def _assert_connections_present(connections: list, expected_names: set) -> None:
    """Assert that specific connections are present in the list."""
    actual_names = {conn["connection_name"] for conn in connections}
    assert expected_names.issubset(
        actual_names
    ), f"Expected connections {expected_names} not found. Got: {actual_names}"


def _assert_connection_parameters(
    connections: list, connection_name: str, expected_params: dict
) -> None:
    """Assert that a specific connection has expected parameters."""
    conn = next(
        (c for c in connections if c["connection_name"] == connection_name), None
    )
    assert conn is not None, f"Connection '{connection_name}' not found"

    # Check each expected parameter
    for key, value in expected_params.items():
        assert (
            key in conn["parameters"]
        ), f"Parameter '{key}' not found in connection '{connection_name}'"
        assert conn["parameters"][key] == value, (
            f"Parameter '{key}' mismatch in connection '{connection_name}': "
            f"expected {value}, got {conn['parameters'][key]}"
        )


def _assert_default_connection(connections: list, expected_name: str) -> None:
    """Assert which connection is marked as default."""
    default_connections = [c for c in connections if c["is_default"]]
    assert (
        len(default_connections) == 1
    ), f"Expected exactly one default connection, found {len(default_connections)}"
    assert default_connections[0]["connection_name"] == expected_name


@pytest.mark.e2e
def test_import_of_snowsql_connections(snowcli, test_root_path, empty_config_file):
    """Test connection import.

    Verifies that connections are imported from SnowSQL config files and
    appear in the connection list. Environment-based connections are not
    shown by default (matching legacy behavior).
    """
    # Initially should have empty or minimal connections list
    initial_output = _get_connections_list_output(snowcli, empty_config_file)
    initial_connections = _parse_connections(initial_output)

    # In isolated e2e tests, should start empty
    assert (
        len(initial_connections) == 0
    ), f"Expected no file-based connections initially, found: {initial_connections}"

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
    final_output = _get_connections_list_output(snowcli, empty_config_file)
    final_connections = _parse_connections(final_output)

    # Validate all connections have proper structure
    for conn in final_connections:
        _assert_connection_structure(conn)

    # Assert expected connections are present
    expected_connections = {"snowsql1", "snowsql2", "example", "snowsql3", "default"}
    _assert_connections_present(final_connections, expected_connections)

    # Validate default connection
    _assert_default_connection(final_connections, "default")

    # Validate specific connection parameters (from snowsql config files)
    _assert_connection_parameters(
        final_connections,
        "snowsql1",
        {
            "account": "a1",
            "user": "u1",
            "host": "h1_override",  # overridden in overriding_config
            "database": "d1",
            "schema": "public",
            "warehouse": "w1",
            "role": "r1",
        },
    )

    _assert_connection_parameters(
        final_connections,
        "default",
        {
            "account": "default_connection_account",
            "user": "default_connection_user",
            "host": "localhost",
            "database": "default_connection_database_override",  # overridden
            "schema": "public",
            "warehouse": "default_connection_warehouse",
            "role": "accountadmin",
        },
    )


@pytest.mark.e2e
def test_import_prompt_for_different_default_connection_name_on_conflict(
    snowcli, test_root_path, empty_config_file
):
    """Test importing with different default connection name."""
    # Initially should have empty or minimal connections list
    initial_output = _get_connections_list_output(snowcli, empty_config_file)
    initial_connections = _parse_connections(initial_output)

    assert len(initial_connections) == 0

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
    final_output = _get_connections_list_output(snowcli, empty_config_file)
    final_connections = _parse_connections(final_output)

    # Validate all connections have proper structure
    for conn in final_connections:
        _assert_connection_structure(conn)

    # Assert expected connections are present
    expected_connections = {"snowsql1", "snowsql2", "example", "snowsql3", "default"}
    _assert_connections_present(final_connections, expected_connections)

    # Validate that snowsql2 is the default (not "default")
    _assert_default_connection(final_connections, "snowsql2")

    # Validate snowsql2 parameters
    _assert_connection_parameters(
        final_connections,
        "snowsql2",
        {
            "account": "a2",
            "user": "u2",
            "host": "h2",
            "port": 1234,
            "database": "d2",
            "schema": "public",
            "warehouse": "w2",
            "role": "r2",
        },
    )


@pytest.mark.e2e
def test_import_confirm_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
):
    """Test import with confirmation on conflict."""
    # Initially should have example connection
    initial_output = _get_connections_list_output(
        snowcli, example_connection_config_file
    )
    initial_connections = _parse_connections(initial_output)

    # Should have the example connection
    _assert_connections_present(initial_connections, {"example"})

    # Import with confirmation (y) - this will overwrite "example" connection
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
    final_output = _get_connections_list_output(snowcli, example_connection_config_file)
    final_connections = _parse_connections(final_output)

    # Validate all connections have proper structure
    for conn in final_connections:
        _assert_connection_structure(conn)

    # Assert all expected connections are present (including overwritten example)
    expected_connections = {"example", "snowsql1", "snowsql2", "snowsql3", "default"}
    _assert_connections_present(final_connections, expected_connections)

    # Validate default connection
    _assert_default_connection(final_connections, "default")

    # Validate that "example" was overwritten with snowsql config values
    _assert_connection_parameters(
        final_connections,
        "example",
        {
            "account": "accountname",
            "user": "username",
        },
    )


@pytest.mark.e2e
def test_import_reject_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
):
    """Test import with rejection on conflict."""
    # Initially should have example connection
    initial_output = _get_connections_list_output(
        snowcli, example_connection_config_file
    )
    initial_connections = _parse_connections(initial_output)

    # Should have the example connection with original values
    _assert_connections_present(initial_connections, {"example"})

    # Get initial example connection parameters
    initial_example = next(
        c for c in initial_connections if c["connection_name"] == "example"
    )
    initial_example_params = initial_example["parameters"].copy()

    # Import with rejection (n) - should NOT overwrite "example" connection
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
    # But other connections should still be imported
    final_output = _get_connections_list_output(snowcli, example_connection_config_file)
    final_connections = _parse_connections(final_output)

    # Validate all connections have proper structure
    for conn in final_connections:
        _assert_connection_structure(conn)

    # Assert all expected connections are present
    expected_connections = {"example", "snowsql1", "snowsql2", "snowsql3", "default"}
    _assert_connections_present(final_connections, expected_connections)

    # Validate default connection
    _assert_default_connection(final_connections, "default")

    # Validate that "example" connection was NOT overwritten (kept original values)
    final_example = next(
        c for c in final_connections if c["connection_name"] == "example"
    )
    assert (
        final_example["parameters"] == initial_example_params
    ), "Example connection should not have been overwritten after rejection"


@pytest.mark.e2e
def test_connection_imported_from_snowsql(snowcli, test_root_path, empty_config_file):
    """Test that imported connection works."""
    # Always provide confirmation to avoid interactive abort.
    stdin = "y\n"

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
        stdin=stdin,
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
