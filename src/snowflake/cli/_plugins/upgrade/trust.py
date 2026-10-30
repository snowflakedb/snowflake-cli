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

"""Detached RSA-SHA256 signature of snowflake-managed manifest.json.

TLS is not the update trust root. Releng signs the exact bytes of each
per-version manifest.json; clients verify that signature against the
embedded public key, then check the SHA-256 hashes inside the manifest.
Keep this PEM in sync with install.sh and install.ps1.
"""

from __future__ import annotations

from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from snowflake.cli.api.exceptions import CliError

MANIFEST_SIG_NAME = "manifest.json.sig"

# RSA-4096. Private key is Jenkins credential snowflake-cli-managed-manifest-key.
DEFAULT_PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAmecfO6u7BbYw16AHXhDy
MOGlJop5LZ08eZrsVswXlps+19CtIEJQQJZPjTKDAqDay0+TpNfj0ErberXW4pRl
WTHV1IsRrMBZQwj+Rx7wfcIBCnctMvXVinCTQxDCG6QVEIvyntSi/shju7ktWeON
GT4deCy7ZnTnE2abtK8sre+sbNAy5UbPiiAWULcoXmmpI/upQhGWDYsugMGdUQAm
aA3u/QzoH/PwH8pClZWUJfbid05U51rUV6WzScb6PZP0PK9JYcSVTHZscmGCYkY3
LrlCNZ+zMy1JCnp2B0cWzwaHoHFBmAACUMJLHz1NtQyRbHcLekKSgr5CIMMNIZmV
rDYYTrGxnqfph6RMCVBd1RuzcXSVZhxTI2o93r0GR/7J+tr1GmY003dfuuWlHsBa
+zKA8NyJDCnq7atW96AnnH78VQ+PuwMxEzmkgbi50lmAtGZuBpRtQn5ySWnaVKG1
S2qBnPgiPWLJ7r7cViK0ZL5Mk8z0hkLXBDlop9MVea9LV9x1evFDkgoC49Gn5g4Z
yqwTODl1tnE7/i47SKfqZl6lTnhbYLlUYYGt1pFmOp8w98ycCNynSFwAKKPr48FJ
uUCg4ucODyWnSw5d7xVnaUbqiHCj7QgPGWR5ssyFnlYHJD8os319nd/0xgvopeeX
nPViy3zvEUsQyDblLYH2jQMCAwEAAQ==
-----END PUBLIC KEY-----
"""

MISSING_SIGNATURE_MESSAGE = (
    "Missing signature for manifest.json. "
    "Refusing to install unsigned snowflake-managed package."
)
INVALID_SIGNATURE_MESSAGE = "Invalid signature on manifest.json. Refusing to install."


def verify_manifest_signature(
    manifest_bytes: bytes,
    signature: bytes,
    *,
    public_key_pem: Optional[bytes] = None,
) -> None:
    if not manifest_bytes:
        raise CliError("Empty manifest.json. Refusing to install.")
    if not signature:
        raise CliError(MISSING_SIGNATURE_MESSAGE)
    pem = public_key_pem if public_key_pem is not None else DEFAULT_PUBLIC_KEY_PEM
    try:
        key = serialization.load_pem_public_key(pem)
    except ValueError as exc:
        raise CliError("Invalid snowflake-managed manifest public key.") from exc
    if not isinstance(key, RSAPublicKey):
        raise CliError("Invalid snowflake-managed manifest public key.")
    try:
        key.verify(signature, manifest_bytes, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise CliError(INVALID_SIGNATURE_MESSAGE) from exc


def sign_manifest(manifest_bytes: bytes, private_key_pem: bytes) -> bytes:
    """RSA-SHA256 PKCS#1, same as ``openssl dgst -sha256 -sign``."""
    key = serialization.load_pem_private_key(private_key_pem, password=None)
    if not isinstance(key, RSAPrivateKey):
        raise ValueError("manifest signing key must be RSA")
    return key.sign(manifest_bytes, padding.PKCS1v15(), hashes.SHA256())
