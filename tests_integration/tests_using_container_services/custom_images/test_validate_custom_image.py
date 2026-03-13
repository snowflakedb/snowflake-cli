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

"""Integration tests for custom-image validate command. Requires Docker."""

import subprocess
from pathlib import Path

import pytest

DOCKER_TEST_DIR = Path(__file__).parent / "docker"


def _docker_available():
    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=10
            ).returncode
            == 0
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _grype_available():
    try:
        return (
            subprocess.run(
                ["grype", "version"], capture_output=True, timeout=10
            ).returncode
            == 0
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _build_image(dockerfile_dir: Path, tag: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            tag,
            str(dockerfile_dir),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to build {tag}: {result.stderr}")
    return tag


def _remove_image(tag: str):
    subprocess.run(["docker", "rmi", "-f", tag], capture_output=True, timeout=60)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _docker_available(), reason="Docker not available"),
]


@pytest.fixture(scope="module")
def valid_image():
    """Build once, reuse across tests, cleanup after all tests."""
    tag = _build_image(DOCKER_TEST_DIR / "test_valid_image", "snowflake-cli-test:valid")
    yield tag
    _remove_image(tag)


@pytest.fixture(scope="module")
def invalid_image():
    tag = _build_image(
        DOCKER_TEST_DIR / "test_invalid_image", "snowflake-cli-test:invalid"
    )
    yield tag
    _remove_image(tag)


def test_valid_image(runner, valid_image):
    result = runner.invoke(["custom-image", "validate", valid_image])

    assert "[PASS] image_exists" in result.output
    assert "[PASS] entrypoint" in result.output
    assert "[PASS] env_DASHBOARD_PORT" in result.output


def test_invalid_image(runner, invalid_image):
    result = runner.invoke(["custom-image", "validate", invalid_image])

    assert result.exit_code == 1
    assert "[FAIL] entrypoint" in result.output
    # Note: env_DASHBOARD_PORT check doesn't run because validation stops early on entrypoint failure


def test_nonexistent_image(runner):
    result = runner.invoke(["custom-image", "validate", "nonexistent:image"])

    assert result.exit_code == 1
    assert "[FAIL] image_exists" in result.output


@pytest.mark.skipif(not _grype_available(), reason="Grype not available")
def test_vulnerability_scan(runner, valid_image):
    result = runner.invoke(
        ["custom-image", "validate", valid_image, "--scan-vulnerabilities"]
    )

    assert (
        "[PASS] vulnerability_scan" in result.output
        or "[FAIL] vulnerability_scan" in result.output
    )
