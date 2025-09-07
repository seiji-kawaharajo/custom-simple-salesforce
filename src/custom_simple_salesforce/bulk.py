"""Salesforce Bulk API 2.0 Client.

This module provides the SfBulk class, a client for interacting with the
Salesforce Bulk API 2.0. It facilitates the creation, management, and execution
of bulk jobs for queries and DML operations through logically separated
'query' and 'ingest' handlers.
"""

import csv
import io
from copy import deepcopy
from time import sleep
from typing import Any, cast, get_args

import requests

from .bulk_job import SfBulkJob, SfBulkJobQuery
from .client import Sf
from .types import FormatType, ResultType


class SfBulk:
    """A client for the Salesforce Bulk API 2.0.

    This class provides functionalities to execute queries and DML operations
    via the Bulk API 2.0. Operations are grouped under the `query` and `ingest`
    attributes for logical separation.

    Attributes:
        bulk2_url (str): The base URL for Bulk API 2.0 endpoints.
        headers (dict[str, str]): The HTTP headers for authentication.
        query (Query): Handler for query operations.
        ingest (Ingest): Handler for ingest (CRUD) operations.
        _interval (int): The default waiting interval in seconds.
        _timeout (int): The request timeout in seconds.

    Args:
        sf (Sf): An authenticated Salesforce client instance.
        interval (int): The default interval in seconds for waiting job status.
        timeout (int): The timeout in seconds for API requests.

    """

    bulk2_url: str
    headers: dict[str, str]
    _interval: int
    _timeout: int

    class Query:
        """Handle Bulk API 2.0 Query operations."""

        def __init__(self, sf_bulk: "SfBulk") -> None:
            """Initialize the Query operations handler."""
            self._sf_bulk = sf_bulk

        def create(
            self,
            query: str,
            *,
            include_all: bool = False,
        ) -> SfBulkJobQuery:
            """Create a query job.

            Args:
                query: The SOQL query to be executed.
                include_all: If True, the operation is 'queryAll' to include
                    archived and deleted records. Defaults to False.

            Returns:
                An object to manage the created query job.

            """
            _operation = "queryAll" if include_all else "query"
            _response = self._sf_bulk._make_request(  # noqa: SLF001
                "POST",
                "query",
                json={
                    "operation": _operation,
                    "query": query,
                },
            )
            return SfBulkJobQuery(self._sf_bulk, cast("dict[str, Any]", _response.json()))

        def get_info(self, job_id: str) -> dict[str, Any]:
            """Get information about a specific query job.

            Args:
                job_id: The ID of the query job.

            Returns:
                A dictionary containing the job's information.

            """
            _response = self._sf_bulk._make_request("GET", f"query/{job_id}")  # noqa: SLF001
            return cast("dict[str, Any]", _response.json())

        def wait(
            self,
            job_id: str,
            interval: int | None = None,
        ) -> dict[str, Any]:
            """Wait a query job's status until it completes.

            Args:
                job_id: The ID of the query job to wait.
                interval: The waiting interval in seconds. If None,
                    the default is used.

            Returns:
                The final job information dictionary after completion.

            """
            final_interval = self._sf_bulk._get_final_interval(interval)  # noqa: SLF001
            while True:
                _job_info = self.get_info(job_id)
                if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                    break
                sleep(final_interval)
            return _job_info

        def get_results(
            self,
            job_id: str,
            format_type: FormatType = "dict",
        ) -> ResultType:
            """Get the results of a completed query job.

            Args:
                job_id: The ID of the query job.
                format_type: The desired output format.

            Returns:
                The query results in the specified format.

            """
            return self._sf_bulk._get_csv_results(  # noqa: SLF001
                f"query/{job_id}/results",
                format_type,
            )

    class Ingest:
        """Handle Bulk API 2.0 Ingest (CRUD) operations."""

        def __init__(self, sf_bulk: "SfBulk") -> None:
            """Initialize the Ingest operations handler."""
            self._sf_bulk = sf_bulk

        def create_insert(self, object_name: str) -> SfBulkJob:
            """Create an insert job.

            Args:
                object_name: The Salesforce object API name (e.g., 'Account').

            Returns:
                An object to manage the created insert job.

            """
            return self._sf_bulk.create_job(object_name, "insert")

        def create_update(self, object_name: str) -> SfBulkJob:
            """Create an update job.

            Args:
                object_name: The Salesforce object API name (e.g., 'Account').

            Returns:
                An object to manage the created update job.

            """
            return self._sf_bulk.create_job(object_name, "update")

        def create_upsert(
            self,
            object_name: str,
            external_id_field: str,
        ) -> SfBulkJob:
            """Create an upsert job.

            Args:
                object_name: The Salesforce object API name (e.g., 'Account').
                external_id_field: The API name of the external ID field.

            Returns:
                An object to manage the created upsert job.

            """
            return self._sf_bulk.create_job(object_name, "upsert", external_id_field)

        def create_delete(self, object_name: str) -> SfBulkJob:
            """Create a delete job (moves records to the recycle bin).

            Args:
                object_name: The Salesforce object API name (e.g., 'Account').

            Returns:
                An object to manage the created delete job.

            """
            return self._sf_bulk.create_job(object_name, "delete")

        def create_hard_delete(self, object_name: str) -> SfBulkJob:
            """Create a hard delete job (permanently deletes records).

            Args:
                object_name: The Salesforce object API name (e.g., 'Account').

            Returns:
                An object to manage the created hard delete job.

            """
            return self._sf_bulk.create_job(object_name, "hardDelete")

        def upload_data(self, job_id: str, csv_data: str) -> None:
            """Upload CSV data to a job.

            Args:
                job_id: The ID of the ingest job.
                csv_data: A string containing the data in CSV format.

            """
            self._sf_bulk._make_request(  # noqa: SLF001
                "PUT",
                f"ingest/{job_id}/batches",
                headers={"Content-Type": "text/csv"},
                data=csv_data.encode("utf-8"),
            )

        def complete_upload(self, job_id: str) -> None:
            """Signal that data upload is complete for a job.

            This moves the job from the 'Open' state to the 'UploadComplete' state,
            making it ready for processing.

            Args:
                job_id: The ID of the ingest job.

            """
            self._sf_bulk._make_request(  # noqa: SLF001
                "PATCH",
                f"ingest/{job_id}",
                json={"state": "UploadComplete"},
            )

        def get_info(self, job_id: str) -> dict[str, Any]:
            """Get information about a specific ingest job.

            Args:
                job_id: The ID of the ingest job.

            Returns:
                A dictionary containing the job's information.

            """
            _response = self._sf_bulk._make_request(  # noqa: SLF001
                "GET",
                f"ingest/{job_id}",
            )
            return cast("dict[str, Any]", _response.json())

        def wait(
            self,
            job_id: str,
            interval: int | None = None,
        ) -> dict[str, Any]:
            """Wait an ingest job's status until it completes.

            Args:
                job_id: The ID of the ingest job to wait.
                interval: The waiting interval in seconds. If None, the default is used.

            Returns:
                The final job information dictionary after completion.

            """
            final_interval = self._sf_bulk._get_final_interval(interval)  # noqa: SLF001
            while True:
                _job_info = self.get_info(job_id)
                if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                    break
                sleep(final_interval)
            return _job_info

        def get_successful_results(
            self,
            job_id: str,
            format_type: FormatType = "dict",
        ) -> ResultType:
            """Get the successful records from a completed ingest job.

            Args:
                job_id: The ID of the ingest job.
                format_type: The desired output format. Defaults to 'dict'.

            Returns:
                The query results in the specified format.

            """
            return self._sf_bulk._get_csv_results(  # noqa: SLF001
                f"ingest/{job_id}/successfulResults",
                format_type,
            )

        def get_failed_results(
            self,
            job_id: str,
            format_type: FormatType = "dict",
        ) -> ResultType:
            """Get the failed records from a completed ingest job.

            Args:
                job_id: The ID of the ingest job.
                format_type: The desired output format. Defaults to 'dict'.

            Returns:
                The query results in the specified format.

            """
            return self._sf_bulk._get_csv_results(  # noqa: SLF001
                f"ingest/{job_id}/failedResults",
                format_type,
            )

        def get_unprocessed_records(
            self,
            job_id: str,
            format_type: FormatType = "dict",
        ) -> ResultType:
            """Get unprocessed records from an ingest job.

            Args:
                job_id: The ID of the ingest job.
                format_type: The desired output format. Defaults to 'dict'.

            Returns:
                The query results in the specified format.

            """
            return self._sf_bulk._get_csv_results(  # noqa: SLF001
                f"ingest/{job_id}/unprocessedrecords",
                format_type,
            )

    def __init__(self: "SfBulk", sf: Sf, interval: int = 5, timeout: int = 30) -> None:
        """Initialize the SfBulk client."""
        self.bulk2_url = sf.bulk2_url
        self.headers = sf.headers
        self._interval = interval
        self._timeout = timeout

        # Instantiate nested handlers
        self.query = self.Query(self)
        self.ingest = self.Ingest(self)

    def _make_request(
        self: "SfBulk",
        method: str,
        endpoint: str,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> requests.Response:
        """Send an API request and check its status (internal helper)."""
        final_headers = deepcopy(self.headers)
        if headers:
            final_headers.update(headers)

        _response = requests.request(
            method,
            f"{self.bulk2_url}{endpoint}",
            headers=final_headers,
            timeout=self._timeout,
            **kwargs,
        )
        _response.raise_for_status()
        return _response

    def _get_csv_results(
        self,
        endpoint: str,
        format_type: FormatType,
    ) -> ResultType:
        """Retrieve and parse CSV results from a given endpoint (internal helper)."""
        allowed_formats = get_args(FormatType.__value__)
        if format_type not in allowed_formats:
            err_msg = (
                f"Unsupported format: '{format_type}'. "
                f"Allowed formats are: {', '.join(allowed_formats)}"
            )
            raise ValueError(err_msg)

        _response = self._make_request("GET", endpoint)
        _response.encoding = "utf-8"

        _csv_data_io = io.StringIO(_response.text)
        match format_type:
            case "dict":
                return list(csv.DictReader(_csv_data_io))
            case "reader":
                return list(csv.reader(_csv_data_io))
            case "csv":
                return _response.text
            case _:
                # This should be unreachable
                err_msg = "Internal error: Unsupported format."
                raise ValueError(err_msg)

    def _get_final_interval(self, interval: int | None) -> int:
        """Determine the final waiting interval (internal helper)."""
        return interval if interval is not None else self._interval

    def create_job(
        self: "SfBulk",
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> SfBulkJob:
        """Create a generic ingest job (internal helper)."""
        if operation == "upsert" and not external_id_field:
            error_msg = "The 'external_id_field' is required for 'upsert' operation."
            raise ValueError(error_msg)

        _payload = {"object": object_name, "operation": operation}
        if external_id_field:
            _payload["externalIdFieldName"] = external_id_field

        _response = self._make_request("POST", "ingest", json=_payload)
        return SfBulkJob(self, cast("dict[str, Any]", _response.json()))
