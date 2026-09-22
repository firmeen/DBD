"""Human-first terminal presentation for dbd-data.

Runtime and storage layers return structured values. This package is the only
place that decides how those values are rendered for an operator.
"""

from .terminal import OutputOptions, TerminalPresenter

__all__ = ["OutputOptions", "TerminalPresenter"]
