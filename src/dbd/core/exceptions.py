from __future__ import annotations


class DbdError(Exception):
    """Base class for all dbd-data errors."""


class DbdAuthError(DbdError):
    """Raised when the anonymous auth handshake with DBD fails."""


class DbdForbiddenError(DbdError):
    """Raised when DBD returns 403 even after the Chrome-cookie fallback."""


class DbdTokenExpiredError(DbdError):
    """Raised when the cached access token has expired and must be refreshed."""


class ContractError(DbdError):
    """Raised when a DBD API response is missing fields this codebase depends on.

    This is the earliest possible signal that DBD changed a response shape,
    so it should fail loudly instead of silently writing nulls to MySQL.
    """
