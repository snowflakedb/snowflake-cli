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

import re

from snowflake.cli._plugins.feedback.constants import MAX_MESSAGE_BYTES, SECRET_MASK
from snowflake.cli.api.connector_adapter import connector_adapter

_ANSI_RE = re.compile(
    r"\x1b(?:"
    r"\[[0-9:;<=>?]*[ -/]*[@-~]|"  # CSI, including colon-separated params
    r"\].*?(?:\x07|\x1b\\)|"  # OSC
    r"[P_^].*?(?:\x07|\x1b\\)"  # DCS / APC / PM
    r")"
)
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
_EXTRA_SECRET_PATTERNS = (
    re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    re.compile(r"\b(?:sk-|gho_|ghs_|ghp_|ghu_|github_pat_|glpat-)[A-Za-z0-9_-]{8,}"),
    re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?"
        r"(?:-----END [A-Z ]*PRIVATE KEY-----|$)",
        re.IGNORECASE,
    ),
)


def prepare_message(message: str) -> tuple[str, bool]:
    """Strip, drop ANSI/controls, mask secrets, then cap UTF-8 size."""
    text = message.strip()
    text = _ANSI_RE.sub("", text)
    text = _CONTROL_RE.sub("", text)
    text = connector_adapter.mask_secrets(text, fallback=SECRET_MASK)
    text = _apply_extra_masks(text)
    truncated = False
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_MESSAGE_BYTES:
        text = encoded[:MAX_MESSAGE_BYTES].decode("utf-8", errors="ignore")
        truncated = True
    return text, truncated


def _apply_extra_masks(text: str) -> str:
    for pattern in _EXTRA_SECRET_PATTERNS:
        text = pattern.sub(SECRET_MASK, text)
    return text
