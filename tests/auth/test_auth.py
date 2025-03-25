from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli.api.config import get_connection_dict
from snowflake.cli.api.secure_utils import file_permissions_are_strict

EXECUTE_QUERY = "snowflake.cli._plugins.auth.keypair.manager.AuthManager.execute_query"
OBJECT_EXECUTE_QUERY = (
    "snowflake.cli._plugins.object.manager.ObjectManager.execute_query"
)
CONNECTION = "snowflake.cli._plugins.auth.keypair.manager.AuthManager._conn"
CONNECT = "snowflake.connector.connect"
SECURE_PATH = "snowflake.cli.api.secure_path.SecurePath"
UPDATE_CONNECTION = (
    "snowflake.cli._app.snow_connector.update_connection_details_with_private_key"
)

key_pair = "key_pair"
new_connection = "keypairconnection"
user_name = "test_user"


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["auth", "keypair", "setup", "--format", "JSON"],
            input=f"Y\n{new_connection}\n4096\n{tmp_dir}\n\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{new_connection}.p8"
        public_key_path = tmp_path / f"{new_connection}.pub"
        assert result.exit_code == 0, result.output
        assert result.output == dedent(
            f"""\
            Create a new connection? [Y/n]: Y
            Enter connection name: {new_connection}
            Enter key length [2048]: 4096
            Enter output path [~/.ssh]: {tmp_path.absolute()}
            Enter private key passphrase []: 
            {{
                "message": "Setup completed."
            }}
            """
        )
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY="
        )


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_with_password(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_dir],
            input=f"Y\n{new_connection}\n4096\n123\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{new_connection}.p8"
        public_key_path = tmp_path / f"{new_connection}.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert "BEGIN ENCRYPTED PRIVATE KEY" in private_key_path.read_text()
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY="
        )


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_no_prompts(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "auth",
                "keypair",
                "setup",
                "--key-length",
                "4096",
                "--output-path",
                tmp_dir,
                "--private-key-passphrase",
                "123",
            ],
            input=f"\n{new_connection}\n",
        )

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_connection_already_exists(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_dir], input="\ndefault\n"
        )

        assert result.exit_code == 1, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_create_output_directory_with_proper_privileges(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "some" / "subdirectory" / "location"
        result = runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_path],
            input=f"Y\n{new_connection}\n4096\n\n",
        )

        private_key_path = tmp_path / f"{new_connection}.p8"
        public_key_path = tmp_path / f"{new_connection}.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert file_permissions_are_strict(tmp_path)
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path.resolve())
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY="
        )


@pytest.mark.parametrize(
    "key_value, key_2_value",
    [
        ("KEY", None),
        (None, "KEY"),
        ("KEY", "KEY"),
    ],
)
@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_error_if_any_public_key_is_set(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
    key_value,
    key_2_value,
):
    mock_connect.return_value.user = user_name
    mock_object_execute_query.return_value = mock_cursor(
        rows=[
            {"property": "RSA_PUBLIC_KEY", "value": key_value},
            {"property": "RSA_PUBLIC_KEY_2", "value": key_2_value},
        ],
        columns=[],
    )

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "some" / "subdirectory" / "location"
        result = runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_path],
            input=f"Y\n{new_connection}\n4096\n\n",
        )

        assert result.exit_code == 1, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_setup_overwrite_connection(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    test_snowcli_config,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_dir], input="n\n\n\n"
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / "default.p8"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        connection_config = get_connection_dict("default")
        assert connection_config["authenticator"] == "SNOWFLAKE_JWT"
        assert connection_config["private_key_file"] == str(private_key_path.resolve())
        assert "password" not in connection_config.keys()


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_public_key_for_rotate(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{key_pair}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            ["auth", "keypair", "rotate", "--format", "JSON", "-c", key_pair],
            input=f"4096\n{tmp_dir}\n\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{key_pair}_1.p8"
        public_key_path = tmp_path / f"{key_pair}_1.pub"
        assert result.exit_code == 0, result.output
        assert result.output == dedent(
            f"""\
            Enter key length [2048]: 4096
            Enter output path [~/.ssh]: {tmp_path.absolute()}
            Enter private key passphrase []: 
            {{
                "message": "Rotate completed."
            }}
            """
        )
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY_2="
        )
        connection_config = get_connection_dict(key_pair)
        assert connection_config["authenticator"] == "SNOWFLAKE_JWT"
        assert connection_config["private_key_file"] == str(private_key_path.resolve())
        assert "password" not in connection_config.keys()


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_with_password(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_public_key_for_rotate(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{key_pair}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            ["auth", "keypair", "rotate", "--output-path", tmp_dir, "-c", key_pair],
            input=f"4096\n123\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{key_pair}_1.p8"
        public_key_path = tmp_path / f"{key_pair}_1.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert "BEGIN ENCRYPTED PRIVATE KEY" in private_key_path.read_text()
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY_2="
        )


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_no_prompts(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_public_key_for_rotate(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{key_pair}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            [
                "auth",
                "keypair",
                "rotate",
                "--key-length",
                "4096",
                "--output-path",
                tmp_dir,
                "--private-key-passphrase",
                "123",
                "-c",
                key_pair,
            ],
        )

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_create_output_directory_with_proper_privileges(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_public_key_for_rotate(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "some" / "subdirectory" / "location"
        runner.invoke(
            ["auth", "keypair", "setup", "--output-path", tmp_path],
            input=f"Y\n{key_pair}\n4096\n\n",
        )

        result = runner.invoke(
            ["auth", "keypair", "rotate", "--output-path", tmp_path, "-c", key_pair],
            input=f"4096\n\n",
        )

        private_key_path = tmp_path / f"{key_pair}_1.p8"
        public_key_path = tmp_path / f"{key_pair}_1.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert file_permissions_are_strict(tmp_path)
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY_2="
        )


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_no_public_key_set(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_empty_public_keys(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{new_connection}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            [
                "auth",
                "keypair",
                "rotate",
                "--output-path",
                tmp_dir,
                "-c",
                new_connection,
            ],
        )

        assert result.exit_code == 1, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_only_public_key_set(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
):
    _mock_user_and_public_key_for_rotate(
        mock_connect, mock_object_execute_query, mock_cursor
    )

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{key_pair}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            ["auth", "keypair", "rotate", "--output-path", tmp_dir, "-c", key_pair],
            input=f"4096\n\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{key_pair}_1.p8"
        public_key_path = tmp_path / f"{key_pair}_1.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY_2="
        )


@pytest.mark.parametrize(
    "key_value, key_2_value",
    [
        (None, "KEY"),
        ("KEY", "KEY"),
    ],
)
@mock.patch(EXECUTE_QUERY)
@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_rotate_other_public_key_set_options(
    mock_connect,
    mock_object_execute_query,
    mock_execute_query,
    runner,
    mock_cursor,
    os_agnostic_snapshot,
    key_value,
    key_2_value,
):
    mock_connect.return_value.user = user_name
    mock_object_execute_query.side_effect = [
        mock_cursor(
            rows=[
                {"property": "RSA_PUBLIC_KEY", "value": None},
                {"property": "RSA_PUBLIC_KEY_2", "value": None},
            ],
            columns=[],
        ),
        mock_cursor(
            rows=[
                {"property": "RSA_PUBLIC_KEY", "value": key_value},
                {"property": "RSA_PUBLIC_KEY_2", "value": key_2_value},
            ],
            columns=[],
        ),
    ]

    with TemporaryDirectory() as tmp_dir:
        runner.invoke(
            ["auth", "keypair", "setup"],
            input=f"Y\n{key_pair}\n4096\n{tmp_dir}\n\n",
        )

        result = runner.invoke(
            ["auth", "keypair", "rotate", "--output-path", tmp_dir, "-c", key_pair],
            input=f"4096\n\n",
        )

        tmp_path = Path(tmp_dir)
        private_key_path = tmp_path / f"{key_pair}.p8"
        public_key_path = tmp_path / f"{key_pair}.pub"
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        assert private_key_path.exists()
        assert file_permissions_are_strict(private_key_path)
        assert public_key_path.exists()
        assert file_permissions_are_strict(public_key_path)
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY="
        )
        assert _call_contains(
            mock_execute_query, f"ALTER USER {user_name} SET RSA_PUBLIC_KEY_2="
        )


@mock.patch(UPDATE_CONNECTION)
@mock.patch(SECURE_PATH)
@mock.patch(CONNECT)
def test_status(
    mock_connect,
    mock_secure_path,
    mock_update_connection,
    runner,
    config_file,
    os_agnostic_snapshot,
):
    mock_connect.return_value.user = user_name
    mock_secure_path.exists.return_value = True

    connection = dedent(
        """\
    [connections.default]
    authenticator = "SNOWFLAKE_JWT"
    private_key_file = "key.p8"
    """
    )

    with config_file(connection) as config:
        result = runner.invoke_with_config_file(config, ["auth", "keypair", "status"])

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot


@mock.patch(SECURE_PATH)
@mock.patch(CONNECT)
def test_status_no_authenticator(
    mock_connect, mock_secure_path, runner, config_file, os_agnostic_snapshot
):
    mock_connect.return_value.user = user_name
    mock_secure_path.exists.return_value = True

    connection = dedent(
        """\
    [connections.default]
    private_key_file = "key.p8"
    """
    )

    with config_file(connection) as config:
        result = runner.invoke_with_config_file(config, ["auth", "keypair", "status"])

        assert result.exit_code == 1, result.output
        assert result.output == os_agnostic_snapshot


def test_status_no_private_key_in_connection(runner, os_agnostic_snapshot):
    result = runner.invoke(["auth", "keypair", "status"])

    assert result.exit_code == 1, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch(OBJECT_EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_list(
    mock_connect, mock_execute_query, runner, mock_cursor, os_agnostic_snapshot
):
    mock_connect.return_value.user = user_name
    mock_execute_query.return_value = mock_cursor(
        rows=[
            {
                "property": "NAME",
                "value": "test_user",
                "default": None,
                "description": "Name",
            },
            {
                "property": "RSA_PUBLIC_KEY",
                "value": "-----BEGIN PUBLIC KEY-----",
                "default": None,
                "description": "RSA public key of the user",
            },
            {
                "property": "RSA_PUBLIC_KEY_FP",
                "value": "SHA256",
                "default": None,
                "description": "Fingerprint of user's RSA public key.",
            },
            {
                "property": "RSA_PUBLIC_KEY_LAST_SET_TIME",
                "value": "2025-02-17 12:53:51.212",
                "default": None,
                "description": "The timestamp",
            },
            {
                "property": "RSA_PUBLIC_KEY_2",
                "value": None,
                "default": None,
                "description": "Second RSA public key of the user",
            },
            {
                "property": "RSA_PUBLIC_KEY_2_FP",
                "value": None,
                "default": None,
                "description": "Fingerprint of user's second RSA public key.",
            },
            {
                "property": "RSA_PUBLIC_KEY_2_LAST_SET_TIME",
                "value": None,
                "default": None,
                "description": "The timestamp",
            },
            {
                "property": "COMMENT",
                "value": None,
                "default": None,
                "description": "user comment",
            },
        ],
        columns=[],
    )

    result = runner.invoke(["auth", "keypair", "list"])

    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize("key", ["RSA_PUBLIC_KEY", "RSA_PUBLIC_KEY_2"])
@mock.patch(EXECUTE_QUERY)
@mock.patch(CONNECT)
def test_remove(
    mock_connect, mock_execute_query, runner, mock_cursor, os_agnostic_snapshot, key
):
    mock_connect.return_value.user = user_name
    mock_execute_query.return_value = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    result = runner.invoke(["auth", "keypair", "remove", "--key-id", key])

    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot
    mock_execute_query.assert_called_once_with(f"ALTER USER test_user UNSET {key}")


def _mock_user_and_empty_public_keys(
    mock_connect, mock_object_execute_query, mock_cursor
):
    mock_connect.return_value.user = user_name
    mock_object_execute_query.return_value = mock_cursor(
        rows=[
            {"property": "RSA_PUBLIC_KEY", "value": None},
            {"property": "RSA_PUBLIC_KEY_2", "value": None},
        ],
        columns=[],
    )


def _mock_user_and_public_key_for_rotate(
    mock_connect, mock_object_execute_query, mock_cursor
):
    mock_connect.return_value.user = user_name
    mock_object_execute_query.side_effect = [
        mock_cursor(
            rows=[
                {"property": "RSA_PUBLIC_KEY", "value": None},
                {"property": "RSA_PUBLIC_KEY_2", "value": None},
            ],
            columns=[],
        ),
        mock_cursor(
            rows=[
                {"property": "RSA_PUBLIC_KEY", "value": "KEY"},
                {"property": "RSA_PUBLIC_KEY_2", "value": None},
            ],
            columns=[],
        ),
    ]


def _call_contains(mock, search_string: str) -> bool:
    for call in mock.mock_calls:
        if search_string in str(call):
            return True
    return False
