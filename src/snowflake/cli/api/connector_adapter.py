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

"""Adapter for connector symbols that v4 ships and v5 does not.

Builtin plugins may import this module at startup. Each method imports the
missing symbol itself, so a connector-5 install still starts ``snow``. Put the
next missing symbol here instead of in a plugin. Exception-shape differences
stay in :mod:`snowflake.cli.api.connector_errors`.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class ConnectorAdapter:
    """v4 connector calls with a defined result when v5 has no replacement."""

    def mask_secrets(self, text: str, *, fallback: str) -> str:
        """Mask ``text`` with ``snowflake.connector.secret_detector.SecretDetector``.

        Connector 5 has no such module. Return ``fallback`` then, and when the
        detector sets ``error_str``, so the raw text is not returned.
        """
        try:
            from snowflake.connector.secret_detector import SecretDetector
        except ImportError:
            log.debug("SecretDetector is unavailable; using the caller fallback")
            return fallback
        detected = SecretDetector.mask_secrets(text)
        if detected.error_str:
            return fallback
        if detected.masked_text is None:
            return text
        return detected.masked_text


connector_adapter = ConnectorAdapter()
