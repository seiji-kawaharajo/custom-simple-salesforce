"""Shared type definitions for the Salesforce Bulk API client."""

from typing import Any, Literal

type ResultType = list[dict[str, Any]] | list[list[str]] | str
"""Represents the parsed data type returned from the Salesforce API.

The concrete type depends on the `format_type` argument provided to
the result-fetching methods. It can be one of the following:

- `'dict'`: A list of dictionaries (`list[dict[str, Any]]`).
- `'reader'`: A list of lists representing CSV rows (`list[list[str]]`).
- `'csv'`: A raw CSV string (`str`).
"""

type FormatType = Literal["dict", "reader", "csv"]
"""Specifies the desired output format for query results."""
