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

import logging
from typing import TypeAlias

from snowflake.cli._app.auth.errors import OidcProviderError
from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderType,
    OidcProviderTypeWithAuto,
    auto_detect_oidc_provider,
    get_active_oidc_provider,
)
from snowflake.cli.api.exceptions import CliError

logger = logging.getLogger(__name__)


Providers: TypeAlias = OidcProviderType | OidcProviderTypeWithAuto


class OidcManager:
    """
    Manages OIDC authentication.

    This class provides methods to read OIDC configurations for authentication.
    """

    def read_token(
        self,
        provider_type: Providers = OidcProviderTypeWithAuto.AUTO,
    ) -> str:
        """
        Reads OIDC token based on the specified provider type.

        Args:
            provider_type: Type of provider to read token from ("auto" for auto-detection)

        Returns:
            Token string or provider information

        Raises:
            CliError: If token reading fails
        """
        logger.info("Reading OIDC token with provider type: %s", provider_type)

        try:
            if provider_type == OidcProviderTypeWithAuto.AUTO:
                provider = auto_detect_oidc_provider()
            else:
                provider = get_active_oidc_provider(provider_type.value)
            return provider.get_token()
        except OidcProviderError as e:
            logger.error("OIDC provider error: %s", str(e))
            raise CliError(str(e))
