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
from typing import Dict, List, Literal, Optional, Type

import id as oidc_id
from snowflake.cli._app.auth.errors import (
    OidcProviderAutoDetectionError,
    OidcProviderNotFoundError,
    OidcProviderUnavailableError,
    OidcTokenRetrievalError,
)

logger = logging.getLogger(__name__)


ACTIONS_ID_TOKEN_REQUEST_URL_ENV: Literal[
    "ACTIONS_ID_TOKEN_REQUEST_URL"
] = "ACTIONS_ID_TOKEN_REQUEST_URL"
GITHUB_ACTIONS_ENV: Literal["GITHUB_ACTIONS"] = "GITHUB_ACTIONS"
SNOWFLAKE_AUDIENCE_ENV: Literal["SNOWFLAKE_AUDIENCE"] = "SNOWFLAKE_AUDIENCE"


class OidcProviderType(Enum):
    """Enum for OIDC provider types."""

    GITHUB = "github"


class OidcProviderTypeWithAuto(Enum):
    """Extended version of OidcProviderType with AUTO."""

    AUTO = "auto"
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

    @property
    @abstractmethod
    def issuer(self) -> str:
        """
        Returns the OIDC issuer URL for this provider.

        Returns:
            The OIDC issuer URL
        """
        pass

    @abstractmethod
    def get_token(self) -> str:
        """
        Retrieves the OIDC token from the CI environment.

        Returns:
            The OIDC token string

        Raises:
            OidcProviderError: If token cannot be retrieved
        """
        pass


class GitHubOidcProvider(OidcTokenProvider):
    """
    OIDC token provider for GitHub Actions.
    """

    @property
    def _is_ci(self):
        logger.debug("Checking if GitHub Actions environment is available")

        # Check if we're in a GitHub Actions environment
        github_actions_env = os.getenv(GITHUB_ACTIONS_ENV)
        logger.debug(
            "%s environment variable: %s",
            GITHUB_ACTIONS_ENV,
            github_actions_env,
        )

        is_github_actions = github_actions_env == "true"
        logger.debug("Running in GitHub Actions: %s", is_github_actions)
        return is_github_actions

    @property
    def audience(self) -> str:
        """
        Returns the audience URL for GitHub OIDC.

        Returns:
            The audience URL, defaults to 'snowflakecomputing.com' if SNOWFLAKE_AUDIENCE environment variable is not set
        """
        return os.getenv(SNOWFLAKE_AUDIENCE_ENV, "snowflakecomputing.com")

    @property
    def issuer(self) -> str:
        """
        Returns the GitHub OIDC issuer URL.

        Returns:
            The GitHub OIDC issuer URL from ACTIONS_ID_TOKEN_REQUEST_URL environment variable,
            or the default GitHub issuer URL if the environment variable is not set
        """
        issuer_url = os.getenv(ACTIONS_ID_TOKEN_REQUEST_URL_ENV)
        if not issuer_url and self._is_ci:
            raise OidcTokenRetrievalError(
                "%s environment variable is not set. "
                "This variable is required for Github Actions OIDC authentication"
                % ACTIONS_ID_TOKEN_REQUEST_URL_ENV
            )
        return issuer_url or "https://token.actions.githubusercontent.com"

    @property
    def provider_name(self) -> str:
        return OidcProviderType.GITHUB.value

    @property
    def is_available(self) -> bool:
        """
        Checks if GitHub Actions environment is available.
        """
        return self._is_ci

    def get_token(self) -> str:
        """
        Retrieves the OIDC token from GitHub Actions.
        """
        logger.debug("Retrieving OIDC token from GitHub Actions")

        try:
            logger.debug("Detecting OIDC credentials for token retrieval")
            # Use configurable audience for workload identity
            token = oidc_id.detect_credential(self.audience)
            if not token:
                logger.error("No OIDC credentials detected")
                raise OidcTokenRetrievalError(
                    "No OIDC credentials detected. This command should be run in a GitHub Actions environment."
                )

            logger.info("Successfully retrieved OIDC token")
            return token
        except Exception as e:
            logger.error("Failed to detect OIDC credentials: %s", str(e))
            raise OidcTokenRetrievalError(
                "Failed to detect OIDC credentials: %s" % str(e)
            )


class OidcProviderRegistry:
    """
    Registry for managing OIDC token providers.
    Handles registration, storage, and retrieval of providers.
    """

    def __init__(self) -> None:
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

    def get_provider_class(
        self, provider_name: str
    ) -> Optional[Type[OidcTokenProvider]]:
        """
        Get a specific provider class by name.
        """
        return self._providers.get(provider_name)

    @property
    def provider_names(self) -> List[str]:
        """
        List all registered provider names.
        """
        return list(self._providers.keys())

    @property
    def all_providers(self) -> List[OidcTokenProvider]:
        """
        Get instances of all registered providers.
        """
        return [provider_class() for provider_class in self._providers.values()]


# Global registry instance
_registry = OidcProviderRegistry()


def get_oidc_provider(provider_name: str) -> OidcTokenProvider:
    """
    Get a specific OIDC provider by name without checking availability.

    Args:
        provider_name: Name of the provider to get

    Returns:
        The requested OIDC provider instance

    Raises:
        OidcProviderNotFoundError: If provider is unknown
    """
    provider = _registry.get_provider(provider_name)

    if not provider:
        providers_list = ", ".join(_registry.provider_names)
        raise OidcProviderNotFoundError(
            "Unknown provider '%s'. Available providers: %s"
            % (
                provider_name,
                providers_list,
            )
        )

    return provider


def get_active_oidc_provider(provider_name: str) -> OidcTokenProvider:
    """
    Get a specific OIDC provider by name and ensure it's available.

    Args:
        provider_name: Name of the provider to get

    Returns:
        The requested OIDC provider instance

    Raises:
        OidcProviderNotFoundError: If provider is unknown
        OidcProviderUnavailableError: If provider is not available
    """
    provider = get_oidc_provider(provider_name)

    if not provider.is_available:
        raise OidcProviderUnavailableError(
            "Provider '%s' is not available in the current environment." % provider_name
        )

    return provider


def get_oidc_provider_class(provider_name: str) -> Type[OidcTokenProvider]:
    """
    Get a specific OIDC provider class by name.

    Args:
        provider_name: Name of the provider to get

    Returns:
        The requested OIDC provider class

    Raises:
        OidcProviderNotFoundError: If provider is unknown
    """
    provider_class = _registry.get_provider_class(provider_name)

    if not provider_class:
        providers_list = ", ".join(_registry.provider_names)
        raise OidcProviderNotFoundError(
            "Unknown provider '%s'. Available providers: %s"
            % (
                provider_name,
                providers_list,
            )
        )

    return provider_class


def auto_detect_oidc_provider() -> OidcTokenProvider:
    """
    Auto-detect a single available OIDC provider in the current environment.

    Returns:
        The single available OIDC provider

    Raises:
        OidcProviderAutoDetectionError: If no providers are available or multiple providers are available
    """
    available = [
        provider for provider in _registry.all_providers if provider.is_available
    ]
    available_names = [p.provider_name for p in available]

    all_providers = _registry.provider_names
    match (len(available), all_providers):
        case (1, _):
            # Happy path - single provider found
            logger.info("Found 1 available provider: %s", available_names[0])
            return available[0]
        case (0, providers) if providers:
            # No providers available but some are registered
            providers_list = ", ".join(providers)
            msg = (
                "No OIDC provider detected in current environment. "
                "Available providers: %s. "
                "Use --type <provider> to specify a provider explicitly."
            ) % providers_list
            logger.info(msg)
            raise OidcProviderAutoDetectionError(msg)
        case (0, _):
            # No providers available and none are registered
            msg = "No OIDC providers are registered."
            logger.info(msg)
            raise OidcProviderAutoDetectionError(msg)
        case _:
            # Multiple providers available - raise error
            providers_list = ", ".join(available_names)
            msg = (
                "Multiple OIDC providers detected: %s. "
                "Please specify which provider to use with --type <provider>."
            ) % providers_list
            logger.info(msg)
            raise OidcProviderAutoDetectionError(msg)

    # This line should never be reached, but helps mypy understand all paths are covered
    raise OidcProviderAutoDetectionError(
        "Unexpected state in auto_detect_oidc_provider"
    )
