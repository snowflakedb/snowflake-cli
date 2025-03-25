from enum import Enum
from typing import Dict, List, Optional, Tuple

from click import ClickException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api import exceptions
from snowflake.cli.api.cli_global_context import (
    _CliGlobalContextAccess,
    get_cli_context,
)
from snowflake.cli.api.config import (
    connection_exists,
    get_connection_dict,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor


class PublicKeyProperty(Enum):
    RSA_PUBLIC_KEY = "RSA_PUBLIC_KEY"
    RSA_PUBLIC_KEY_2 = "RSA_PUBLIC_KEY_2"


class AuthManager(SqlExecutionMixin):
    def setup(
        self,
        connection_name: Optional[str],
        key_length: int,
        output_path: SecurePath,
        private_key_passphrase: SecretType,
    ):
        # When the user provide new connection name
        if connection_name and connection_exists(connection_name):
            raise ClickException(
                f"Connection with name {connection_name} already exists."
            )

        cli_context = get_cli_context()
        # When the use not provide connection name, so we overwrite the current connection
        if not connection_name:
            connection_name = cli_context.connection_context.connection_name

        key_name = AuthManager._get_free_key_name(output_path, connection_name)  # type: ignore[arg-type]
        self._generate_key_pair_and_set_public_key(
            user=cli_context.connection.user,
            key_length=key_length,
            output_path=output_path,
            key_name=key_name,  # type: ignore[arg-type]
            private_key_passphrase=private_key_passphrase,
        )

        self._create_or_update_connection(
            current_connection=cli_context.connection_context.connection_name,
            connection_name=connection_name,  # type: ignore[arg-type]
            private_key_path=self._get_private_key_path(
                output_path=output_path, key_name=key_name  # type: ignore[arg-type]
            ),
        )

    def rotate(
        self,
        key_length: int,
        output_path: SecurePath,
        private_key_passphrase: SecretType,
    ):
        cli_context = get_cli_context()
        connection_name = cli_context.connection_context.connection_name

        self._ensure_connection_has_private_key(
            cli_context.connection_context.connection_name
        )

        public_key, public_key_2 = self._get_public_keys()

        if not public_key and not public_key_2:
            raise ClickException("No public key found. Use the setup command first.")

        if public_key_2:
            self.set_public_key(
                cli_context.connection.user,
                PublicKeyProperty.RSA_PUBLIC_KEY,
                public_key_2,
            )

        key_name = AuthManager._get_free_key_name(output_path, connection_name)
        public_key = self._generate_keys_and_return_public_key(
            key_length=key_length,
            output_path=output_path,
            key_name=key_name,
            private_key_passphrase=private_key_passphrase,
        )
        self.set_public_key(
            cli_context.connection.user, PublicKeyProperty.RSA_PUBLIC_KEY_2, public_key
        )
        self._create_or_update_connection(
            current_connection=cli_context.connection_context.connection_name,
            connection_name=connection_name,
            private_key_path=self._get_private_key_path(
                output_path=output_path, key_name=key_name
            ),
        )

    def _generate_key_pair_and_set_public_key(
        self,
        user: str,
        key_length: int,
        output_path: SecurePath,
        key_name: str,
        private_key_passphrase: SecretType,
    ):
        public_key_exists, public_key_2_exists = self._get_public_keys()

        if public_key_exists or public_key_2_exists:
            raise exceptions.CouldNotSetKeyPairError()

        if not output_path.exists():
            output_path.mkdir(parents=True)

        public_key = self._generate_keys_and_return_public_key(
            key_length=key_length,
            output_path=output_path,
            key_name=key_name,  # type: ignore[arg-type]
            private_key_passphrase=private_key_passphrase,
        )
        self.set_public_key(user, PublicKeyProperty.RSA_PUBLIC_KEY, public_key)

    def list_keys(self) -> List[Dict]:
        key_properties = [
            "RSA_PUBLIC_KEY",
            "RSA_PUBLIC_KEY_FP",
            "RSA_PUBLIC_KEY_LAST_SET_TIME",
            "RSA_PUBLIC_KEY_2",
            "RSA_PUBLIC_KEY_2_FP",
            "RSA_PUBLIC_KEY_2_LAST_SET_TIME",
        ]
        cursor = ObjectManager(connection=self._conn).describe(
            object_type=ObjectType.USER.value.sf_name,
            fqn=FQN.from_string(self._conn.user),
            cursor_class=DictCursor,
        )
        only_public_key_properties = [
            p for p in cursor.fetchall() if p.get("property") in key_properties
        ]
        return only_public_key_properties

    def set_public_key(
        self, user: str, public_key_property: PublicKeyProperty, public_key: str
    ) -> SnowflakeCursor:
        return self.execute_query(
            f"ALTER USER {user} SET {public_key_property.value}='{public_key}'"
        )

    def remove_public_key(
        self, public_key_property: PublicKeyProperty
    ) -> SnowflakeCursor:
        cli_context = get_cli_context()
        return self.execute_query(
            f"ALTER USER {cli_context.connection.user} UNSET {public_key_property.value}"
        )

    def status(self):
        cli_context = get_cli_context()
        self._ensure_connection_has_private_key(
            cli_context.connection_context.connection_name
        )
        cli_console.step("Private key set for connection - OK")
        self._check_connection(cli_context)
        cli_console.step("Test connection - OK")

    def extend_connection_add(
        self,
        connection_name: str,
        connection_options: Dict,
        key_length: int,
        output_path: SecurePath,
        private_key_passphrase: SecretType,
    ) -> Dict:
        key_name = AuthManager._get_free_key_name(output_path, connection_name)

        self._generate_key_pair_and_set_public_key(
            user=connection_options["user"],
            key_length=key_length,
            output_path=output_path,
            key_name=key_name,
            private_key_passphrase=private_key_passphrase,
        )

        connection_options["authenticator"] = "SNOWFLAKE_JWT"
        connection_options["private_key_file"] = str(
            self._get_private_key_path(output_path=output_path, key_name=key_name).path
        )
        if connection_options.get("password"):
            del connection_options["password"]

        return connection_options

    @staticmethod
    def _ensure_connection_has_private_key(connection_name: str) -> None:
        connection = get_connection_dict(connection_name)
        if not connection.get("private_key_file") and not connection.get(
            "private_key_path"
        ):
            raise ClickException(
                f"The private key is not set in {connection_name} connection."
            )

    @staticmethod
    def _check_connection(cli_context: _CliGlobalContextAccess) -> None:
        cli_context.connection

    def _get_public_keys(self) -> Tuple[str, str]:
        keys = self.list_keys()
        public_key = ""
        public_key_2 = ""
        for p in keys:
            if (
                p.get("property") == PublicKeyProperty.RSA_PUBLIC_KEY.value
                and p.get("value") != "null"
            ):
                public_key = p.get("value")  # type: ignore
            if (
                p.get("property") == PublicKeyProperty.RSA_PUBLIC_KEY_2.value
                and p.get("value") != "null"
            ):
                public_key_2 = p.get("value")  # type: ignore
        return public_key, public_key_2

    @staticmethod
    def _generate_keys_and_return_public_key(
        key_length: int,
        output_path: SecurePath,
        key_name: str,
        private_key_passphrase: SecretType,
    ) -> str:
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_length,
        )

        if private_key_passphrase:
            pem = SecretType(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.BestAvailableEncryption(
                        private_key_passphrase.value.encode("utf-8")
                    ),
                )
            )
            cli_console.message(
                "Set the `PRIVATE_KEY_PASSPHRASE` environment variable before using the connection."
            )
        else:
            pem = SecretType(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        with AuthManager._get_private_key_path(output_path, key_name).open(
            mode="wb"
        ) as file:
            file.write(pem.value)

        public_key = private_key.public_key()
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        with AuthManager._get_public_key_path(output_path, key_name).open(
            mode="wb"
        ) as file:
            file.write(public_pem)

        return public_pem.decode("utf-8")

    @staticmethod
    def _get_free_key_name(output_path: SecurePath, key_name: str) -> str:
        new_private_key = f"{key_name}.p8"
        new_public_key = f"{key_name}.pub"
        new_key_name = key_name
        counter = 1

        while (
            (output_path / new_private_key).exists()
            and (output_path / new_public_key).exists()
            and counter <= 100
        ):
            new_key_name = f"{key_name}_{counter}"
            new_private_key = f"{new_key_name}.p8"
            new_public_key = f"{new_key_name}.pub"
            counter += 1

        if counter == 100:
            raise ClickException(
                "Too many key pairs with the same name in the output directory."
            )

        return new_key_name

    @staticmethod
    def _get_private_key_path(output_path: SecurePath, key_name: str) -> SecurePath:
        return (output_path / f"{key_name}.p8").resolve()

    @staticmethod
    def _get_public_key_path(output_path: SecurePath, key_name: str) -> SecurePath:
        return (output_path / f"{key_name}.pub").resolve()

    @staticmethod
    def _create_or_update_connection(
        current_connection: Optional[str],
        connection_name: str,
        private_key_path: SecurePath,
    ):
        connection = get_connection_dict(current_connection)
        connection.pop("password", None)
        connection["authenticator"] = "SNOWFLAKE_JWT"
        connection["private_key_file"] = str(private_key_path.path)

        set_config_value(["connections", connection_name], value=connection)
