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

import shutil
import subprocess
from pathlib import Path

import pytest
from snowflake.cli._plugins.upgrade.trust import (
    DEFAULT_PUBLIC_KEY_PEM,
    INVALID_SIGNATURE_MESSAGE,
    sign_manifest,
    verify_manifest_signature,
)
from snowflake.cli.api.exceptions import CliError

from tests.upgrade.manifest_signing import generate_test_rsa_keypair

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_SH = REPO_ROOT / "scripts" / "packaging" / "snowflake-managed" / "install.sh"
INSTALL_PS1 = REPO_ROOT / "scripts" / "packaging" / "snowflake-managed" / "install.ps1"
PUB_PEM = (
    REPO_ROOT
    / "src"
    / "snowflake"
    / "cli"
    / "_plugins"
    / "upgrade"
    / "managed_manifest.pub.pem"
)


def test_sign_and_verify_roundtrip():
    private_pem, public_pem = generate_test_rsa_keypair()
    body = b'{"packages": {}}\n'
    signature = sign_manifest(body, private_pem)
    verify_manifest_signature(body, signature, public_key_pem=public_pem)


def test_tampered_manifest_is_rejected():
    private_pem, public_pem = generate_test_rsa_keypair()
    body = b'{"packages": {}}\n'
    signature = sign_manifest(body, private_pem)
    with pytest.raises(CliError, match="Invalid signature"):
        verify_manifest_signature(
            b'{"packages": {"evil": {}}}\n',
            signature,
            public_key_pem=public_pem,
        )


def test_empty_signature_is_rejected():
    with pytest.raises(CliError, match="unsigned snowflake-managed package"):
        verify_manifest_signature(b"{}", b"")


def test_openssl_signature_matches_cryptography(tmp_path: Path):
    openssl_bin = shutil.which("openssl")
    if openssl_bin is None:
        pytest.skip("openssl is not installed")
    assert openssl_bin is not None
    private_pem, public_pem = generate_test_rsa_keypair()
    body = tmp_path / "manifest.json"
    body.write_bytes(b'{"packages": {}}\n')
    key_path = tmp_path / "key.pem"
    key_path.write_bytes(private_pem)
    sig_path = tmp_path / "manifest.json.sig"
    subprocess.run(
        [
            openssl_bin,
            "dgst",
            "-sha256",
            "-sign",
            str(key_path),
            "-out",
            str(sig_path),
            str(body),
        ],
        check=True,
        capture_output=True,
    )
    verify_manifest_signature(
        body.read_bytes(), sig_path.read_bytes(), public_key_pem=public_pem
    )


def test_embedded_pubkey_matches_install_scripts_and_pem_file():
    pem_text = DEFAULT_PUBLIC_KEY_PEM.decode()
    assert PUB_PEM.read_text(encoding="utf-8") == pem_text
    sh = INSTALL_SH.read_text(encoding="utf-8")
    ps1 = INSTALL_PS1.read_text(encoding="utf-8")
    for line in pem_text.strip().splitlines():
        assert line in sh
    assert "Test-ManifestSignature" in ps1
    assert "ManifestModulusB64" in ps1
    assert "mecfO6u7BbYw16AHXhDyMOGlJop5LZ08eZrsVswXlps" in ps1
    assert INVALID_SIGNATURE_MESSAGE.split(".")[0] in sh
