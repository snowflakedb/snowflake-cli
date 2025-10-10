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


@pytest.mark.parametrize("config_mode", ["legacy", "config_ng"], indirect=True)
@pytest.mark.e2e
def test_import_of_snowsql_connections(
    snowcli, test_root_path, empty_config_file, snapshot, config_mode
):
    """Test connection import with both legacy and config_ng systems."""
    # Initially should have empty or minimal connections list
    initial_output = _get_connections_list_output(snowcli, empty_config_file)
    assert initial_output == snapshot

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
    assert final_output == snapshot


@pytest.mark.parametrize("config_mode", ["legacy", "config_ng"], indirect=True)
@pytest.mark.e2e
def test_import_prompt_for_different_default_connection_name_on_conflict(
    snowcli, test_root_path, empty_config_file, snapshot, config_mode
):
    """Test importing with different default connection name."""
    # Initially should have empty or minimal connections list
    initial_output = _get_connections_list_output(snowcli, empty_config_file)
    assert initial_output == snapshot

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
    assert final_output == snapshot


@pytest.mark.parametrize("config_mode", ["legacy", "config_ng"], indirect=True)
@pytest.mark.e2e
def test_import_confirm_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
    snapshot,
    config_mode,
):
    """Test import with confirmation on conflict."""
    # Initially should have example and integration connections
    initial_output = _get_connections_list_output(
        snowcli, example_connection_config_file
    )
    assert initial_output == snapshot

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
    final_output = _get_connections_list_output(snowcli, example_connection_config_file)
    assert final_output == snapshot


@pytest.mark.parametrize("config_mode", ["legacy", "config_ng"], indirect=True)
@pytest.mark.e2e
def test_import_reject_on_conflict_with_existing_cli_connection(
    snowcli,
    test_root_path,
    example_connection_config_file,
    snapshot,
    config_mode,
):
    """Test import with rejection on conflict."""
    # Initially should have example and integration connections
    initial_output = _get_connections_list_output(
        snowcli, example_connection_config_file
    )
    assert initial_output == snapshot

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
    # But other connections should still be imported
    final_output = _get_connections_list_output(snowcli, example_connection_config_file)
    assert final_output == snapshot


@pytest.mark.parametrize("config_mode", ["legacy", "config_ng"], indirect=True)
@pytest.mark.e2e
def test_connection_imported_from_snowsql(
    snowcli, test_root_path, empty_config_file, config_mode
):
    """Test that imported connection works."""
    # In config_ng, an INTEGRATION connection may already exist via env vars.
    # Confirm override explicitly to avoid interactive abort.
    stdin = "y\n" if config_mode == "config_ng" else None

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
