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


from snowflake.cli.api.exceptions import CliError


class AccountMismatchError(CliError):
    """The active connection's account does not match the manifest target's
    ``account_identifier``.

    A dedicated ``CliError`` subclass so callers (e.g. ``apply``) can catch
    *only* the account-guard failure and surface it as a structured
    ``target_mismatch`` status, while letting real manifest errors (missing
    file, malformed YAML, unknown target) propagate with their own message.
    """


class ManifestNotFoundError(Exception):
    """Manifest file does not exist."""


class InvalidManifestError(Exception):
    """Manifest file is not valid (empty, wrong type, wrong version)."""


class ManifestConfigurationError(Exception):
    """Manifest is valid but has configuration issues (target not found, config doesn't exist)."""
