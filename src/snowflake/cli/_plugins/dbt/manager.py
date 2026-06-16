# Copyright (c) 2025 Snowflake Inc.
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

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, TypedDict

import yaml
from snowflake.cli._plugins.dbt.constants import (
    ENV_FILENAME,
    PROFILES_FILENAME,
    SUPPORTED_DBT_VERSIONS_QUERY,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, ObjectType
from snowflake.cli.api.exceptions import CliArgumentError, CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

DBT_ENV_SECRET_PREFIX = "DBT_ENV_SECRET_"
_ENV_VAR_KEY_PREFIX = "DBT_"
_ENV_VAR_KEY_RE = re.compile(r"^[A-Za-z0-9_]+$")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")
# A single-quoted SQL string literal (with '' escaping a quote), optionally
# surrounded by horizontal whitespace. Group 1 is the still-escaped contents.
_ENV_VAR_SQL_LITERAL_RE = re.compile(r"[ \t]*'((?:[^']|'')*)'[ \t]*")


def _reject_control_chars(value: Optional[str], flag_name: str) -> Optional[str]:
    if value is not None and _CONTROL_CHAR_RE.search(value):
        raise CliError(
            f"{flag_name} must not contain control characters "
            f"(newlines, tabs, etc.)"
        )
    return value


def _collect_shell_env_vars() -> tuple[Dict[str, str], int, int]:
    """Collect DBT_* environment variables from os.environ for --use-shell-env-vars.

    Returns (forwarded vars sorted by key, count of dropped DBT_ENV_SECRET_*
    keys, count of skipped malformed keys). Only fully-uppercase, DBT_-prefixed
    keys with valid characters and control-char-free values are forwarded —
    matching exactly what the server accepts in ENV_VARS=(), so a malformed
    shell var never triggers a server-side rejection of the whole run.
    Secret-prefixed keys are detected case-insensitively and dropped (never
    sent), so a secret cannot leak into query history. Sorting makes the
    resulting SQL text deterministic across shells.
    """
    forwarded: Dict[str, str] = {}
    dropped_secret_count = 0
    skipped_count = 0
    for key, value in os.environ.items():
        upper = key.upper()
        if not upper.startswith(_ENV_VAR_KEY_PREFIX):
            continue
        if upper.startswith(DBT_ENV_SECRET_PREFIX):
            dropped_secret_count += 1
            continue
        if (
            key == upper
            and _ENV_VAR_KEY_RE.match(key)
            and not _CONTROL_CHAR_RE.search(value)
        ):
            forwarded[key] = value
        else:
            skipped_count += 1
    return dict(sorted(forwarded.items())), dropped_secret_count, skipped_count


class _NoDuplicatesSafeLoader(yaml.SafeLoader):
    """yaml.SafeLoader that rejects duplicate mapping keys.

    PyYAML's default behavior is silent last-wins, but the server-side SQL
    parser rejects duplicates outright. Match that here so the user gets a
    clear local error instead of a server round-trip.
    """


def _no_duplicates_constructor(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                f"duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_NoDuplicatesSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _no_duplicates_constructor,
)


class DBTObjectEditableAttributes(TypedDict):
    default_target: Optional[str]
    default_env: Optional[str]
    external_access_integrations: Optional[List[str]]
    dbt_version: Optional[str]


@dataclass
class DBTDeployAttributes:
    """Attributes for deploying a DBT project."""

    default_target: Optional[str] = None
    unset_default_target: bool = False
    default_env: Optional[str] = None
    unset_default_env: bool = False
    external_access_integrations: Optional[List[str]] = None
    install_local_deps: bool = False
    dbt_version: Optional[str] = None


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT PROJECTS"
        return self.execute_query(query)

    @staticmethod
    def exists(name: FQN) -> bool:
        return ObjectManager().object_exists(
            object_type=ObjectType.DBT_PROJECT.value.cli_name, fqn=name
        )

    @staticmethod
    def describe(name: FQN) -> SnowflakeCursor:
        return ObjectManager().describe(
            object_type=ObjectType.DBT_PROJECT.value.cli_name, fqn=name
        )

    @staticmethod
    def get_dbt_object_attributes(name: FQN) -> Optional[DBTObjectEditableAttributes]:
        """Get editable attributes of an existing DBT project, or None if it doesn't exist."""
        try:
            cursor = DBTManager().describe(name)
        except ProgrammingError as exc:
            if "DBT PROJECT" in exc.msg and "does not exist" in exc.msg:
                return None
            raise exc

        rows = list(cursor)
        if not rows:
            return None

        row = rows[0]
        # Convert row to dict using column names
        columns = [desc[0].lower() for desc in cursor.description]
        row_dict = dict(zip(columns, row))

        external_access_integrations = row_dict.get("external_access_integrations")
        if external_access_integrations:
            if isinstance(external_access_integrations, str):
                external_access_integrations = [
                    x.strip()
                    for x in external_access_integrations.strip("[]").split(",")
                    if x.strip()
                ]
            elif not isinstance(external_access_integrations, list):
                external_access_integrations = None
        else:
            external_access_integrations = None

        return DBTObjectEditableAttributes(
            default_target=row_dict.get("default_target"),
            default_env=row_dict.get("default_environment"),
            external_access_integrations=external_access_integrations,
            dbt_version=row_dict.get("dbt_version"),
        )

    def _get_supported_dbt_versions(self) -> List[str]:
        try:
            row = self.execute_query(SUPPORTED_DBT_VERSIONS_QUERY).fetchone()
        except ProgrammingError as exc:
            raise CliError(
                "Could not fetch supported dbt versions from server. "
                "Ensure your Snowflake account supports SYSTEM$SUPPORTED_DBT_VERSIONS()."
            ) from exc
        if row is None or row[0] is None:
            raise CliError("Could not fetch supported dbt versions from server.")
        try:
            entries = json.loads(row[0])
        except (json.JSONDecodeError, TypeError) as exc:
            raise CliError(
                "Could not parse supported dbt versions from server."
            ) from exc
        try:
            versions = [e["dbt_version"] for e in entries]
        except (KeyError, TypeError) as exc:
            raise CliError(
                "Could not parse supported dbt versions from server."
            ) from exc
        if not versions:
            raise CliError("Server returned no supported dbt versions.")
        return versions

    def _validate_dbt_version(self, dbt_version: str) -> None:
        supported = self._get_supported_dbt_versions()
        if dbt_version not in supported:
            raise CliArgumentError(
                f"Invalid value '{dbt_version}' for --dbt-version. "
                f"Supported versions: {', '.join(supported)}."
            )

    def deploy(
        self,
        fqn: FQN,
        path: SecurePath,
        profiles_path: SecurePath,
        force: bool,
        attrs: DBTDeployAttributes,
        env_file_path: Optional[SecurePath] = None,
    ) -> SnowflakeCursor:
        dbt_project_path = path / "dbt_project.yml"
        if not dbt_project_path.exists():
            raise CliError(
                f"dbt_project.yml does not exist in directory {path.path.absolute()}."
            )

        with dbt_project_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            dbt_project = yaml.safe_load(fd)
            try:
                profile = dbt_project["profile"]
            except KeyError:
                raise CliError("`profile` is not defined in dbt_project.yml")

        self._validate_profiles(profiles_path, profile, attrs.default_target)

        if attrs.dbt_version:
            self._validate_dbt_version(attrs.dbt_version)

        # env.yml comes from --env-file-dir if given, else the source dir
        # (env.yml is optional, so it may be absent in either case).
        env_source_path = env_file_path if env_file_path is not None else path
        env_file = env_source_path / ENV_FILENAME
        if env_file_path is not None and not env_file.exists():
            raise CliError(
                f"{ENV_FILENAME} does not exist in directory {env_file_path.path.absolute()}."
            )
        # Parse/validate before any network call so duplicate keys / invalid
        # YAML fail fast, whether env.yml came from --env-file-dir or the
        # source directory.
        env_yml_content = (
            self._validate_and_parse_env_file(env_file) if env_file.exists() else None
        )

        with cli_console.phase("Creating temporary stage"):
            stage_manager = StageManager()
            stage_fqn = FQN.from_resource(ObjectType.DBT_PROJECT, fqn, "STAGE")
            stage_manager.create(stage_fqn, temporary=True)
            stage_name = stage_manager.get_standard_stage_prefix(stage_fqn)

        with cli_console.phase("Copying project files to stage"):
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                stage_manager.copy_to_tmp_dir(path.path, tmp_path)
                self._prepare_profiles_file(profiles_path.path, tmp_path)
                if env_yml_content is not None:
                    self._write_env_file(env_yml_content, tmp_path)
                result_count = len(
                    list(
                        stage_manager.put_recursive(
                            path.path, stage_name, temp_directory=tmp_path
                        )
                    )
                )
                cli_console.step(f"Copied {result_count} files")

        with cli_console.phase("Creating DBT project"):
            if force is True:
                return self._deploy_create_or_replace(fqn, stage_name, attrs)
            else:
                dbt_object_attributes = self.get_dbt_object_attributes(fqn)
                if dbt_object_attributes is not None:
                    return self._deploy_alter(
                        fqn, stage_name, dbt_object_attributes, attrs
                    )
                else:
                    return self._deploy_create(fqn, stage_name, attrs)

    def _deploy_alter(
        self,
        fqn: FQN,
        stage_name: str,
        dbt_object_attributes: DBTObjectEditableAttributes,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        set_properties = []
        unset_properties = []

        current_default_target = dbt_object_attributes.get("default_target")
        if attrs.unset_default_target and current_default_target is not None:
            unset_properties.append("DEFAULT_TARGET")
        elif attrs.default_target and (
            current_default_target is None
            or current_default_target.lower() != attrs.default_target.lower()
        ):
            set_properties.append(f"DEFAULT_TARGET='{attrs.default_target}'")

        # Always issue SET/UNSET when the user asks; the server treats
        # UNSET-on-null as a no-op.
        if attrs.unset_default_env:
            unset_properties.append("DEFAULT_ENVIRONMENT")
        elif attrs.default_env:
            set_properties.append(
                f"DEFAULT_ENVIRONMENT={to_string_literal(attrs.default_env)}"
            )

        # Comparing dbt_version to existing project's dbt_version might be ambiguous
        # if previously project was locked to just minor version and now user wants to
        # lock it to a patch as well. If target version is provided, it's better to just
        # apply it.
        if attrs.dbt_version:
            set_properties.append(f"DBT_VERSION={to_string_literal(attrs.dbt_version)}")

        current_external_access_integrations = dbt_object_attributes.get(
            "external_access_integrations"
        )
        if self._should_update_external_access_integrations(
            current_external_access_integrations,
            attrs.external_access_integrations,
            attrs.install_local_deps,
        ):
            if attrs.external_access_integrations:
                integrations_str = ", ".join(sorted(attrs.external_access_integrations))
                set_properties.append(
                    f"EXTERNAL_ACCESS_INTEGRATIONS=({integrations_str})"
                )
            elif attrs.install_local_deps:
                set_properties.append("EXTERNAL_ACCESS_INTEGRATIONS=()")

        if set_properties or unset_properties:
            self._execute_property_updates(fqn, set_properties, unset_properties)

        query = f"ALTER DBT PROJECT {fqn} ADD VERSION"
        query += f"\nFROM {stage_name}"
        result = self.execute_query(query)

        return result

    @staticmethod
    def _should_update_external_access_integrations(
        current: Optional[List[str]],
        requested: Optional[List[str]],
        install_local_deps: bool,
    ) -> bool:
        if requested is not None:
            current_set = set(current) if current else set()
            requested_set = set(requested)
            return current_set != requested_set
        elif install_local_deps:
            current_set = set(current) if current else set()
            return current_set != set()
        return False

    def _execute_property_updates(
        self, fqn: FQN, set_clauses: List[str], unset_properties: List[str]
    ) -> None:
        if set_clauses:
            query = f"ALTER DBT PROJECT {fqn} SET {', '.join(set_clauses)}"
            self.execute_query(query)

        for property_name in unset_properties:
            query = f"ALTER DBT PROJECT {fqn} UNSET {property_name}"
            self.execute_query(query)

    def _deploy_create(
        self,
        fqn: FQN,
        stage_name: str,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        query = f"CREATE DBT PROJECT {fqn}"
        query += f"\nFROM {stage_name}"
        if attrs.default_target:
            query += f" DEFAULT_TARGET='{attrs.default_target}'"
        if attrs.default_env:
            query += f" DEFAULT_ENVIRONMENT={to_string_literal(attrs.default_env)}"
        if attrs.dbt_version:
            query += f" DBT_VERSION={to_string_literal(attrs.dbt_version)}"
        query = self._handle_external_access_integrations_query(
            query, attrs.external_access_integrations, attrs.install_local_deps
        )
        return self.execute_query(query)

    @staticmethod
    def _handle_external_access_integrations_query(
        query: str,
        external_access_integrations: Optional[List[str]],
        install_local_deps: bool,
    ) -> str:
        # Providing external access integrations will trigger installation of local deps as well
        if external_access_integrations:
            integrations_str = ", ".join(external_access_integrations)
            query += f"\nEXTERNAL_ACCESS_INTEGRATIONS = ({integrations_str})"
        elif install_local_deps:
            query += f"\nEXTERNAL_ACCESS_INTEGRATIONS = ()"
        return query

    def _deploy_create_or_replace(
        self,
        fqn: FQN,
        stage_name: str,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        query = f"CREATE OR REPLACE DBT PROJECT {fqn}"
        query += f"\nFROM {stage_name}"
        if attrs.default_target:
            query += f" DEFAULT_TARGET='{attrs.default_target}'"
        if attrs.default_env:
            query += f" DEFAULT_ENVIRONMENT={to_string_literal(attrs.default_env)}"
        if attrs.dbt_version:
            query += f" DBT_VERSION={to_string_literal(attrs.dbt_version)}"
        query = self._handle_external_access_integrations_query(
            query, attrs.external_access_integrations, attrs.install_local_deps
        )
        return self.execute_query(query)

    def _validate_profiles(
        self,
        profiles_path: SecurePath,
        profile_name: str,
        default_target: str | None = None,
    ) -> None:
        """
        Validates that:
         * profiles.yml exists
         * contain profile specified in dbt_project.yml
         * default_target (if specified) exists in the profile's outputs
        """
        profiles_file = profiles_path / PROFILES_FILENAME
        if not profiles_file.exists():
            raise CliError(
                f"{PROFILES_FILENAME} does not exist in directory {profiles_path.path.absolute()}."
            )
        with profiles_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            profiles = yaml.safe_load(fd)

        if profile_name not in profiles:
            raise CliError(
                f"Profile {profile_name} is not defined in {PROFILES_FILENAME}."
            )

        errors = defaultdict(list)
        profile = profiles[profile_name]
        target_name = default_target or profile.get("target")
        available_targets = set(profile["outputs"].keys())
        if target_name in available_targets:
            target = profile["outputs"][target_name]
            target_errors = self._validate_target(target_name, target)
            if target_errors:
                errors[profile_name].extend(target_errors)
        else:
            available_targets_str = ", ".join(sorted(available_targets))
            errors[profile_name].append(
                f"Target '{target_name}' is not defined in profile '{profile_name}'. "
                f"Available targets: {available_targets_str}"
            )

        if errors:
            message = f"Found following errors in {PROFILES_FILENAME}. Please fix them before proceeding:"
            for target, issues in errors.items():
                message += f"\n{target}"
                message += "\n * " + "\n * ".join(issues)
            raise CliError(message)

    def _validate_target(
        self, target_name: str, target_details: Dict[str, str]
    ) -> List[str]:
        errors = []
        required_fields = {
            "database",
            "role",
            "schema",
            "type",
        }
        if missing_keys := required_fields - set(target_details.keys()):
            errors.append(
                f"Missing required fields: {', '.join(sorted(missing_keys))} in target {target_name}"
            )
        if role := target_details.get("role"):
            if not self._validate_role(role_name=role):
                errors.append(f"Role '{role}' does not exist or is not accessible.")
        return errors

    def _validate_role(self, role_name: str) -> bool:
        try:
            with self.use_role(role_name):
                self.execute_query("select 1")
            return True
        except ProgrammingError:
            return False

    @staticmethod
    def _prepare_profiles_file(profiles_path: Path, tmp_path: Path):
        # We need to copy profiles.yml file (not symlink) in order to redact
        # any comments without changing original file. This can be achieved
        # with pyyaml, which looses comments while reading a yaml file
        source_profiles_file = SecurePath(profiles_path / PROFILES_FILENAME)
        target_profiles_file = SecurePath(tmp_path / PROFILES_FILENAME)
        if target_profiles_file.exists():
            target_profiles_file.unlink()
        with source_profiles_file.open(
            read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
        ) as sfd, target_profiles_file.open(mode="w") as tfd:
            yaml.safe_dump(yaml.safe_load(sfd), tfd)

    @staticmethod
    def _validate_and_parse_env_file(env_file: SecurePath) -> Optional[dict]:
        """Parse and validate env.yml, rejecting duplicate keys and invalid YAML."""
        with env_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as sfd:
            try:
                return yaml.load(sfd, Loader=_NoDuplicatesSafeLoader)
            except yaml.constructor.ConstructorError as e:
                raise CliError(f"Failed to parse {ENV_FILENAME}: {e.problem}")
            except yaml.YAMLError as e:
                raise CliError(f"{ENV_FILENAME} is not valid YAML: {e}")

    @staticmethod
    def _write_env_file(content: dict, tmp_path: Path):
        """Write the parsed env.yml into the staging dir (comments already dropped)."""
        target_env_file = SecurePath(tmp_path / ENV_FILENAME)
        if target_env_file.exists():
            target_env_file.unlink()
        with target_env_file.open(mode="w") as tfd:
            yaml.safe_dump(content, tfd)

    def execute(
        self,
        dbt_command: str,
        name: FQN,
        run_async: bool,
        dbt_version: Optional[str] = None,
        environment: Optional[str] = None,
        env_vars: Optional[str] = None,
        *dbt_cli_args,
        use_shell_env_vars: bool = False,
    ) -> SnowflakeCursor:
        if dbt_cli_args:
            processed_args = self._process_dbt_args(dbt_cli_args)
            dbt_command = f"{dbt_command} {processed_args}".strip()
        query = f"EXECUTE DBT PROJECT {name}"
        if dbt_version:
            query += f" dbt_version={to_string_literal(dbt_version)}"
        if environment:
            query += f" ENVIRONMENT={to_string_literal(environment)}"

        merged: Dict[str, str] = {}
        if use_shell_env_vars:
            shell_vars, dropped_secret_count, skipped_count = _collect_shell_env_vars()
            if dropped_secret_count:
                cli_console.message(
                    f"--use-shell-env-vars: dropped {dropped_secret_count} "
                    f"{DBT_ENV_SECRET_PREFIX}* environment variable(s) from "
                    "shell. To forward secrets, use the secrets: block in "
                    "env.yml referencing a Snowflake SECRET object, or pass "
                    "them explicitly via --env-vars."
                )
            if skipped_count:
                cli_console.message(
                    f"--use-shell-env-vars: skipped {skipped_count} DBT_* shell "
                    "environment variable(s) that can't be forwarded; keys "
                    "must be uppercase and contain only letters, digits, and "
                    "underscores (e.g. export DBT_FOO, not DBT_Foo)."
                )
            if shell_vars:
                cli_console.warning(
                    f"--use-shell-env-vars: forwarded {len(shell_vars)} shell "
                    "environment variable(s) into query text. Never put "
                    "credentials, tokens, passwords, or other confidential "
                    "data in shell environment variables with the DBT_ prefix."
                )
            elif not dropped_secret_count and not skipped_count:
                cli_console.message(
                    "--use-shell-env-vars: no DBT_* environment variables "
                    "found in the shell. Make sure the variables are exported "
                    "(see your shell's documentation for how to export "
                    "environment variables)."
                )
            merged.update(shell_vars)
        if env_vars:
            merged.update(self._parse_env_vars(env_vars))
        env_vars_clause = self._format_env_vars_clause_from_dict(merged)
        if env_vars_clause:
            query += env_vars_clause
        query += f" args={to_string_literal(dbt_command)}"
        return self.execute_query(query, _exec_async=run_async)

    @staticmethod
    def _format_env_vars_clause_from_dict(pairs: Dict[str, str]) -> str:
        if not pairs:
            return ""
        secret_keys = [k for k in pairs if k.startswith(DBT_ENV_SECRET_PREFIX)]
        if secret_keys:
            cli_console.warning(
                f"--env-vars contains key(s) with the {DBT_ENV_SECRET_PREFIX} prefix "
                f"({', '.join(secret_keys)}); these values will appear in the SQL "
                f"text and query history. To avoid that, use the secrets: block "
                f"in env.yml referencing a Snowflake SECRET object."
            )
        items = ", ".join(
            f"{to_string_literal(k)}={to_string_literal(v)}" for k, v in pairs.items()
        )
        return f" ENV_VARS=({items})"

    @staticmethod
    def _parse_env_vars(raw: str) -> Dict[str, str]:
        """Parse --env-vars in the SQL string-literal form.

        Example: "('DBT_FOO'='1', 'DBT_BAR'='2')". Keys and values are
        single-quoted string literals, with a doubled single quote ('')
        escaping a literal quote, mirroring Snowflake string-literal syntax.
        This is the same form the CLI emits into the ENV_VARS=(...) clause, so
        a clause copied from a query can be passed straight back in. Keys must
        be uppercase, start with 'DBT_', and contain only ASCII letters,
        digits, and underscores.
        """
        s = raw.strip()
        if not (s.startswith("(") and s.endswith(")")):
            raise CliError(
                "--env-vars must be wrapped in parentheses, "
                "e.g. \"('DBT_FOO'='1', 'DBT_BAR'='2')\""
            )
        inner = s[1:-1]
        n = len(inner)

        def read_literal(pos: int):
            """Match a single-quoted literal at pos; return (unescaped, next_pos)."""
            m = _ENV_VAR_SQL_LITERAL_RE.match(inner, pos)
            if m:
                return m.group(1).replace("''", "'"), m.end()
            rest = inner[pos:].lstrip(" \t")
            if rest.startswith("'"):
                raise CliError(f"--env-vars: unterminated string literal in {raw!r}")
            hint = (
                " (use single quotes for SQL string literals)"
                if rest.startswith('"')
                else ""
            )
            raise CliError(
                f"--env-vars: expected a single-quoted string literal "
                f"in {raw!r}{hint}"
            )

        result: Dict[str, str] = {}
        if not inner.strip(" \t"):  # "()" — no pairs
            return result
        pos = 0
        while True:
            key, pos = read_literal(pos)
            if pos >= n or inner[pos] != "=":
                raise CliError(
                    f"--env-vars: expected '=' after key {key!r} in {raw!r}"
                )
            value, pos = read_literal(pos + 1)
            DBTManager._validate_env_var(key, value)
            if key in result:
                raise CliError(f"--env-vars: duplicate key {key!r}")
            result[key] = value
            if pos >= n:
                break
            if inner[pos] != ",":
                raise CliError(
                    f"--env-vars: expected ',' between pairs in {raw!r}"
                )
            pos += 1
        return result

    @staticmethod
    def _validate_env_var(key: str, value: str) -> None:
        if not key:
            raise CliError("--env-vars key must not be empty")
        if not _ENV_VAR_KEY_RE.match(key):
            raise CliError(
                f"--env-vars key {key!r} must contain only ASCII letters, "
                f"digits, and underscores"
            )
        if not key.startswith(_ENV_VAR_KEY_PREFIX):
            raise CliError(
                f"--env-vars key {key!r} must start with {_ENV_VAR_KEY_PREFIX!r}"
            )
        if key != key.upper():
            raise CliError(
                f"--env-vars key {key!r} must be uppercase (e.g. {key.upper()!r})"
            )
        if _CONTROL_CHAR_RE.search(value):
            raise CliError(
                f"--env-vars value for {key!r} must not contain control "
                f"characters (newlines, tabs, etc.)"
            )

    @staticmethod
    def _process_dbt_args(dbt_cli_args: tuple) -> str:
        """
        Process dbt CLI arguments, handling special cases like --vars flag.
        """
        if not dbt_cli_args:
            return ""

        processed_args = []
        i = 0
        while i < len(dbt_cli_args):
            arg = dbt_cli_args[i]
            if arg == "--vars" and i + 1 < len(dbt_cli_args):
                vars_value = dbt_cli_args[i + 1]
                processed_args.append("--vars")
                processed_args.append(f"'{vars_value}'")
                i += 2
            else:
                processed_args.append(arg)
                i += 1
        return " ".join(processed_args)
