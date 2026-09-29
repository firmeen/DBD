from __future__ import annotations


class DbdError(Exception):
    """Base class for all dbd-data errors."""


class DbdAuthError(DbdError):
    """Raised when the anonymous auth handshake with DBD fails."""

    def __init__(self, message: str = "DBD authentication failed", *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class DbdForbiddenError(DbdError):
    """Raised when DBD rejects an authenticated API request with HTTP 403."""

    status_code = 403


class DbdTokenExpiredError(DbdError):
    """Raised when the cached access token has expired and must be refreshed."""


class ContractError(DbdError):
    """Raised when a DBD API response is missing fields this codebase depends on.

    This is the earliest possible signal that DBD changed a response shape,
    so it should fail loudly instead of silently writing nulls to MySQL.
    """


class IdentityConflict(ContractError):
    """Raised when a response or existing projection disagrees with task identity.

    Identity conflicts are contract failures plus an operational master-state
    transition: while the task still owns its lease, the worker marks the company
    master row as `conflict` so future master scheduling excludes it until a human
    resolves the source identity discrepancy.
    """