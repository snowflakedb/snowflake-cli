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

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.upgrade.repo import (
    MANIFEST_NAME,
    POINTER_NAME,
    HttpRepo,
)
from snowflake.cli._plugins.upgrade.rollout import (
    HOLD,
    IMMEDIATE,
    STAGED_RAMP_HOURS,
    PublishedRelease,
    Rollout,
    RolloutPolicy,
    in_slice,
    parse_rollout,
)
from snowflake.cli._plugins.upgrade.trust import MANIFEST_SIG_NAME, sign_manifest
from snowflake.cli.api.exceptions import CliError

from tests.upgrade.manifest_signing import generate_test_rsa_keypair

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
RELEASED_AT_NOW = "2026-09-28T12:00:00Z"
# 9.6h / 96h = 0.1
RELEASED_AT_10PCT = "2026-09-28T02:24:00Z"
STAGED_NOW = Rollout(
    policy=RolloutPolicy.STAGED,
    released_at=datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc),
)
STAGED_10 = Rollout(
    policy=RolloutPolicy.STAGED,
    released_at=datetime(2026, 9, 28, 2, 24, tzinfo=timezone.utc),
)
STAGED_DONE = Rollout(
    policy=RolloutPolicy.STAGED,
    released_at=NOW - timedelta(hours=STAGED_RAMP_HOURS),
)


@pytest.fixture
def repo_keys():
    return generate_test_rsa_keypair()


def _in(score, rollout) -> bool:
    return in_slice(score, rollout, now=NOW)


def test_in_slice_staged_at_publish_includes_nobody():
    assert _in(0.0, STAGED_NOW) is False
    assert _in(0.999, STAGED_NOW) is False


def test_in_slice_staged_mid_ramp():
    assert _in(0.099, STAGED_10) is True
    assert _in(0.1, STAGED_10) is False


def test_in_slice_staged_is_continuous_not_percent_steps():
    # 0.5h / 96h ≈ 0.005208. Percent buckets would still be at 0%.
    rollout = Rollout(
        policy=RolloutPolicy.STAGED,
        released_at=NOW - timedelta(hours=0.5),
    )
    assert _in(0.005, rollout) is True
    assert _in(0.006, rollout) is False


def test_in_slice_staged_after_ramp_includes_everyone():
    assert _in(0.0, STAGED_DONE) is True
    assert _in(0.999999, STAGED_DONE) is True


def test_in_slice_immediate_includes_everyone():
    assert _in(0.999, IMMEDIATE) is True
    assert _in(0.0, IMMEDIATE) is True


def test_in_slice_hold_includes_nobody():
    assert _in(0.0, HOLD) is False
    assert _in(0.999, HOLD) is False


def test_in_slice_future_released_at_is_nobody():
    future = Rollout(
        policy=RolloutPolicy.STAGED,
        released_at=NOW + timedelta(hours=1),
    )
    assert _in(0.0, future) is False


@pytest.mark.parametrize(
    "score,rollout,expected",
    [
        (0.099, STAGED_10, True),
        (0.1, STAGED_10, False),
        (0.999, STAGED_DONE, True),
        (0.0, STAGED_NOW, False),
        (-0.1, IMMEDIATE, False),
        (1.0, IMMEDIATE, False),
        (True, IMMEDIATE, False),
    ],
)
def test_in_slice_table(score, rollout, expected):
    assert _in(score, rollout) is expected


@pytest.mark.parametrize(
    "manifest,expected",
    [
        (
            {"rollout": {"policy": "staged", "released_at": RELEASED_AT_NOW}},
            STAGED_NOW,
        ),
        (
            {
                "rollout": {
                    "policy": "staged",
                    "released_at": RELEASED_AT_NOW,
                    "fraction": 100,
                }
            },
            STAGED_NOW,
        ),
        (
            {"rollout": {"policy": "immediate", "fraction": 1}},
            IMMEDIATE,
        ),
        (
            {"rollout": {"policy": "immediate"}},
            IMMEDIATE,
        ),
        (
            {"rollout": {"policy": "hold", "released_at": RELEASED_AT_NOW}},
            HOLD,
        ),
        ({}, HOLD),
        ({"packages": {}}, HOLD),
        ({"rollout": {"policy": "mystery", "released_at": RELEASED_AT_NOW}}, HOLD),
        ({"rollout": {"policy": "staged"}}, HOLD),
        ({"rollout": {"policy": "staged", "released_at": "not-a-time"}}, HOLD),
        ({"rollout": {"policy": "staged", "released_at": True}}, HOLD),
        (
            {"rollout": {"policy": "STAGED", "released_at": RELEASED_AT_NOW}},
            STAGED_NOW,
        ),
        (None, HOLD),
        ([], HOLD),
        ("staged", HOLD),
        ({"rollout": None}, HOLD),
        ({"rollout": []}, HOLD),
        ({"rollout": "staged"}, HOLD),
    ],
)
def test_parse_rollout_table(manifest, expected):
    assert parse_rollout(manifest) == expected


def test_parse_rollout_never_raises_on_garbage():
    parse_rollout(object())
    parse_rollout({"rollout": {"policy": "staged", "released_at": 1}})


def test_leftover_fraction_does_not_override_ramp():
    rollout = parse_rollout(
        {
            "rollout": {
                "policy": "staged",
                "released_at": RELEASED_AT_NOW,
                "fraction": 100,
            }
        }
    )
    assert _in(0.0, rollout) is False


def _serve_pointer_and_manifest(
    httpserver: HTTPServer,
    version: str,
    repo_keys: tuple[bytes, bytes],
    *,
    manifest: dict,
    serve_sig: bool = True,
    tarball_name: Optional[str] = None,
) -> None:
    body = (json.dumps(manifest) + "\n").encode()
    httpserver.expect_request(f"/{POINTER_NAME}").respond_with_data(f"{version}\n")
    httpserver.expect_request(f"/{version}/{MANIFEST_NAME}").respond_with_data(
        body, content_type="application/json"
    )
    if serve_sig:
        private_pem, _ = repo_keys
        httpserver.expect_request(f"/{version}/{MANIFEST_SIG_NAME}").respond_with_data(
            sign_manifest(body, private_pem),
            content_type="application/octet-stream",
        )
    else:
        httpserver.expect_request(f"/{version}/{MANIFEST_SIG_NAME}").respond_with_data(
            b"", status=404
        )
    if tarball_name is not None:
        httpserver.expect_request(f"/{version}/{tarball_name}").respond_with_data(
            b"should-not-be-fetched"
        )


def _repo(httpserver: HTTPServer, repo_keys: tuple[bytes, bytes]) -> HttpRepo:
    _, public_pem = repo_keys
    return HttpRepo(
        base_url=httpserver.url_for("/").rstrip("/"), public_key_pem=public_pem
    )


def test_latest_release_reads_rollout_from_signed_manifest(httpserver, repo_keys):
    version = "3.13.1"
    _serve_pointer_and_manifest(
        httpserver,
        version,
        repo_keys,
        manifest={"rollout": {"policy": "staged", "released_at": RELEASED_AT_10PCT}},
        tarball_name="snowflake-cli-3.13.1-linux-amd64.tar.gz",
    )
    release = _repo(httpserver, repo_keys).latest_release()
    assert release == PublishedRelease(version=version, rollout=STAGED_10)
    assert _in(0.099, release.rollout) is True
    assert _in(0.1, release.rollout) is False


def test_latest_release_missing_rollout_is_hold(httpserver, repo_keys):
    _serve_pointer_and_manifest(
        httpserver, "3.13.1", repo_keys, manifest={"packages": {}}
    )
    release = _repo(httpserver, repo_keys).latest_release()
    assert release.version == "3.13.1"
    assert release.rollout == HOLD
    assert _in(0.0, release.rollout) is False


def test_latest_release_unknown_policy_is_hold(httpserver, repo_keys):
    _serve_pointer_and_manifest(
        httpserver,
        "3.13.1",
        repo_keys,
        manifest={"rollout": {"policy": "canary", "released_at": RELEASED_AT_NOW}},
    )
    assert _repo(httpserver, repo_keys).latest_release().rollout == HOLD


def test_latest_release_immediate_ignores_fraction(httpserver, repo_keys):
    _serve_pointer_and_manifest(
        httpserver,
        "3.13.1",
        repo_keys,
        manifest={"rollout": {"policy": "immediate", "fraction": 0}},
    )
    rollout = _repo(httpserver, repo_keys).latest_release().rollout
    assert rollout == IMMEDIATE
    assert _in(0.999, rollout) is True


def test_latest_release_missing_signature_fails_closed(httpserver, repo_keys):
    _serve_pointer_and_manifest(
        httpserver,
        "3.13.1",
        repo_keys,
        manifest={"rollout": {"policy": "staged", "released_at": RELEASED_AT_NOW}},
        serve_sig=False,
    )
    with pytest.raises(CliError, match="Missing signature for manifest.json"):
        _repo(httpserver, repo_keys).latest_release()


def test_latest_version_still_returns_pointer_only(httpserver, repo_keys):
    httpserver.expect_request(f"/{POINTER_NAME}").respond_with_data("3.13.1\n")
    assert _repo(httpserver, repo_keys).latest_version() == "3.13.1"
