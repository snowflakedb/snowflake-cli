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

import importlib
import inspect
import logging
import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Type

import id as oidc_id
from snowflake.cli.api.exceptions import CliError

logger = logging.getLogger(__name__)

SNOWFLAKE_AUDIENCE_ENV = "SNOWFLAKE_AUDIENCE"


class OidcProviderType(Enum):
    """Enum for OIDC provider types."""

    GITHUB = "github"


class OidcTokenProvider(ABC):
    """
    Abstract base class for OIDC token providers.
    Each CI environment should implement this interface.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Returns the name of the CI provider (e.g., 'github', 'gitlab', 'azure-devops').
        """
        pass

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """
        Checks if this provider is available in the current environment.
        Should return True if the provider can detect credentials in the current context.
        """
        pass

    @abstractmethod
    def get_token(self) -> str:
        """
        Retrieves the OIDC token from the CI environment.

        Returns:
            The OIDC token string

        Raises:
            CliError: If token cannot be retrieved
        """
        pass


class GitHubOidcProvider(OidcTokenProvider):
    """
    OIDC token provider for GitHub Actions.
    """

    # Audience URL - configurable via environment variable
    AUDIENCE = os.getenv(SNOWFLAKE_AUDIENCE_ENV, "snowflakecomputing.com")

    @property
    def provider_name(self) -> str:
        return OidcProviderType.GITHUB.value

    @property
    def is_available(self) -> bool:
        """
        Checks if GitHub Actions environment is available.
        """
        logger.debug("Checking if GitHub Actions environment is available")

        # Check if we're in a GitHub Actions environment
        github_actions_env = os.getenv("GITHUB_ACTIONS")
        logger.debug("GITHUB_ACTIONS environment variable: %s", github_actions_env)

        is_github_actions = github_actions_env == "true"
        logger.debug("Running in GitHub Actions: %s", is_github_actions)
        return is_github_actions

    def get_token(self) -> str:
        """
        Retrieves the OIDC token from GitHub Actions.
        """
        logger.debug("Retrieving OIDC token from GitHub Actions")

        try:
            logger.debug("Detecting OIDC credentials for token retrieval")
            # Use configurable audience for workload identity
            token = oidc_id.detect_credential(self.AUDIENCE)
            if not token:
                logger.error("No OIDC credentials detected")
                raise CliError(
                    "No OIDC credentials detected. This command should be run in a GitHub Actions environment."
                )

            logger.info("Successfully retrieved OIDC token")
            return token
        except Exception as e:
            logger.error("Failed to detect OIDC credentials: %s", str(e))
            raise CliError("Failed to detect OIDC credentials: %s" % str(e))


class OidcProviderRegistry:
    """
    Registry for auto-discovering and managing OIDC token providers.
    """

    def __init__(self):
        self._providers: Dict[str, Type[OidcTokenProvider]] = {}
        self._auto_discover_providers()

    def _auto_discover_providers(self) -> None:
        """
        Auto-discovers all OIDC token providers in the current module.
        """
        logger.debug("Auto-discovering OIDC token providers")
        current_module = importlib.import_module(__name__)

        for name, obj in inspect.getmembers(current_module):
            if (
                inspect.isclass(obj)
                and issubclass(obj, OidcTokenProvider)
                and obj != OidcTokenProvider
            ):
                provider_instance = obj()
                provider_name = provider_instance.provider_name
                logger.debug("Discovered OIDC provider: %s (%s)", provider_name, name)
                self._providers[provider_name] = obj

        logger.info(
            "Auto-discovered %d OIDC provider(s): %s",
            len(self._providers),
            list(self._providers.keys()),
        )

    def register_provider(self, provider_class: Type[OidcTokenProvider]) -> None:
        """
        Manually register a provider class.
        """
        provider_instance = provider_class()
        self._providers[provider_instance.provider_name] = provider_class

    def get_provider(self, provider_name: str) -> Optional[OidcTokenProvider]:
        """
        Get a specific provider by name.
        """
        provider_class = self._providers.get(provider_name)
        if provider_class:
            return provider_class()
        return None

    def get_available_providers(self) -> List[OidcTokenProvider]:
        """
        Get all providers that are available in the current environment.
        """
        logger.debug("Checking for available OIDC providers")
        available = []
        for provider_class in self._providers.values():
            provider = provider_class()
            provider_name = provider.provider_name
            logger.debug("Checking availability of provider: %s", provider_name)
            if provider.is_available:
                logger.debug("Provider %s is available", provider_name)
                available.append(provider)
            else:
                logger.debug("Provider %s is not available", provider_name)

        available_names = [p.provider_name for p in available]
        logger.info(
            "Found %d available provider(s): %s", len(available), available_names
        )
        return available

    def auto_detect_provider(self) -> Optional[OidcTokenProvider]:
        """
        Auto-detect the first available provider in the current environment.
        """
        for provider in self.get_available_providers():
            return provider
        return None

    def list_provider_names(self) -> List[str]:
        """
        List all registered provider names.
        """
        return list(self._providers.keys())


# Global registry instance
_registry = OidcProviderRegistry()


def get_oidc_provider(provider_name: str) -> Optional[OidcTokenProvider]:
    """
    Get a specific OIDC provider by name.
    """
    return _registry.get_provider(provider_name)


def auto_detect_oidc_provider() -> Optional[OidcTokenProvider]:
    """
    Auto-detect the first available OIDC provider in the current environment.
    """
    return _registry.auto_detect_provider()


def list_oidc_providers() -> List[str]:
    """
    List all registered OIDC provider names.
    """
    return _registry.list_provider_names()
