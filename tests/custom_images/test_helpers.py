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

import json
from unittest import mock

FULL_PACKAGE_LIST = [
    {"name": "snowflake-ml-python", "version": "1.0"},
    {"name": "ray", "version": "2.0"},
    {"name": "ipykernel", "version": "6.0"},
    {"name": "sqlparse", "version": "0.5"},
    {"name": "jinja2", "version": "3.0"},
    {"name": "notebook", "version": "7.0"},
    {"name": "ipython", "version": "8.0"},
    {"name": "psutil", "version": "5.0"},
    {"name": "snowflake-snowpark-python", "version": "1.0"},
    {"name": "jupyter-server", "version": "2.0"},
    {"name": "lightgbm-ray", "version": "0.1"},
    {"name": "xgboost-ray", "version": "0.1"},
    {"name": "snowflake", "version": "1.0"},
    {"name": "snowflake.core", "version": "1.0"},
    {"name": "snowflake-connector-python", "version": "3.0"},
]


def make_docker_inspect_response(
    entrypoint: list[str] | None = None,
    env_vars: list[str] | None = None,
) -> str:
    """Helper to create a mock docker inspect JSON response."""
    return json.dumps(
        [
            {
                "Config": {
                    "Entrypoint": entrypoint,
                    "Env": env_vars or [],
                    "Labels": {},
                }
            }
        ]
    )


def make_pip_list_response(packages: list[dict]) -> str:
    """Helper to create a mock pip list JSON response."""
    return json.dumps(packages)


def create_mock_side_effect(
    inspect_response: str | None = None,
    pip_list_response: str | None = None,
    pip_check_result: tuple[int, str] = (0, ""),
    grype_result: tuple[int, str] = (0, ""),
    grype_error: Exception | None = None,
):
    """Build a subprocess.run side_effect that routes docker/grype commands."""
    if inspect_response is None:
        inspect_response = make_docker_inspect_response()
    if pip_list_response is None:
        pip_list_response = make_pip_list_response([])

    def side_effect(*args, **kwargs):
        cmd = args[0]
        cmd_str = " ".join(cmd)
        if cmd[0] == "docker":
            if "inspect" in cmd:
                return mock.Mock(returncode=0, stdout=inspect_response, stderr="")
            elif "run" in cmd:
                if "pip list" in cmd_str:
                    return mock.Mock(returncode=0, stdout=pip_list_response, stderr="")
                elif "pip check" in cmd_str:
                    return mock.Mock(
                        returncode=pip_check_result[0],
                        stdout=pip_check_result[1],
                        stderr="",
                    )
        elif cmd[0] == "grype":
            if grype_error:
                raise grype_error
            return mock.Mock(
                returncode=grype_result[0], stdout=grype_result[1], stderr=""
            )
        return mock.Mock(returncode=0, stdout="", stderr="")

    return side_effect
