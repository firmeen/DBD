"""Standalone Master -> Deep collector.

This package deliberately does not import the legacy campaign/fabric runtime.
"""

from .catalog import ACTIVE_ENDPOINT_MASK, ACTIVE_ENDPOINTS

__all__ = ["ACTIVE_ENDPOINTS", "ACTIVE_ENDPOINT_MASK"]
