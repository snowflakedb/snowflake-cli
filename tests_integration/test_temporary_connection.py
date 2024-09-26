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
import tempfile
import pytest
import os
from unittest import mock


@pytest.mark.integration
def test_temporary_connection(runner, _temporary_connection_env, snapshot):
    with tempfile.TemporaryDirectory() as tmp_dir:
        if os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE", None):
            private_key_path = os.environ.get(
                "SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE"
            )
        else:
            private_key_path = str(os.path.join(tmp_dir, "private_key.p8"))
            with open(private_key_path, "w") as f:
                f.write(
                    os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW")
                )
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
        assert result.exit_code == 0
        assert result.output == snapshot


@pytest.fixture
def _temporary_connection_env():
    private_key_file = os.environ.get(
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE",
        os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_PATH", None),
    )
    private_key_raw = os.environ.get(
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW", None
    )

    if not private_key_file and not private_key_raw:
        pytest.fail(
            "One of SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE or SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW must be set"
        )

    env = {
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT": os.environ[
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT"
        ],
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_USER": os.environ[
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_USER"
        ],
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_HOST": os.environ[
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_HOST"
        ],
    }
    if private_key_file:
        env["SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE"] = private_key_file
    if private_key_raw:
        env["SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW"] = private_key_raw

    with mock.patch.dict(os.environ, env, clear=True):
        yield env
