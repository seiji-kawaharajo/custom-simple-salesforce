#!/usr/bin/env python
"""A Python library for interacting with the Salesforce API.

This package provides:
- Sf: A client for the Salesforce REST API.
- SfBulk: A client for the Salesforce Bulk API 2.0.
"""

from .bulk import SfBulk
from .client import Sf

__all__ = ["Sf", "SfBulk"]
