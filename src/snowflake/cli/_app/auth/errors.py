class OidcProviderError(Exception):
    """Base exception for OIDC provider related errors."""

    ...


class OidcProviderNotFoundError(OidcProviderError):
    """Exception raised when requested OIDC provider is not found or unknown."""

    ...


class OidcProviderUnavailableError(OidcProviderError):
    """Exception raised when OIDC provider is not available in current environment."""

    ...


class OidcProviderAutoDetectionError(OidcProviderError):
    """Exception raised when auto-detection of OIDC provider fails."""

    ...


class OidcTokenRetrievalError(OidcProviderError):
    """Exception raised when OIDC token cannot be retrieved."""

    ...
