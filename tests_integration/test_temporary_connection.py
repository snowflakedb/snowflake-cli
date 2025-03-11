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
import os

import pytest

from snowflake.cli.api.secure_path import SecurePath


@pytest.mark.integration
def test_temporary_connection(runner, snapshot):
    with SecurePath.temporary_directory() as tmp_path:
        if os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE", None):
            private_key_path = os.environ.get(
                "SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE"
            )
        else:
            private_key_secure_path = tmp_path / "private_key.p8"
            with private_key_secure_path.open(mode="w") as f:
                f.write(
                    os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW")
                )
            private_key_path = private_key_secure_path.path
        result = runner.invoke(
            [
                "sql",
                "-q",
                "select 1",
                "--temporary-connection",
                "--host",
                os.environ["SNOWFLAKE_CONNECTIONS_INTEGRATION_HOST"],
                "--authenticator",
                "SNOWFLAKE_JWT",
                "--account",
                os.environ["SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT"],
                "--user",
                os.environ["SNOWFLAKE_CONNECTIONS_INTEGRATION_USER"],
                "--private-key-file",
                private_key_path,
            ]
        )
        assert result.exit_code == 0, result.output
        assert result.output == snapshot
