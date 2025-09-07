"""Salesforce Bulk API 2.0 Client.

This module provides the SfBulk class, a client for interacting with the
Salesforce Bulk API 2.0. It facilitates the creation, management, and execution
of bulk jobs for queries and DML operations (insert, update, upsert, delete).
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
    (insert, update, delete, etc.) via the Bulk API 2.0.

    Attributes:
        bulk2_url (str): The base URL for Bulk API 2.0 endpoints.
        headers (dict[str, str]): The HTTP headers for authentication.
        _interval (int): The default polling interval in seconds.
        _timeout (int): The request timeout in seconds.

    Args:
        sf (Sf): An authenticated Salesforce client instance.
        interval (int): The default interval in seconds for polling job status.
        timeout (int): The timeout in seconds for API requests.

    """

    bulk2_url: str
    headers: dict[str, str]
    _interval: int
    _timeout: int

    def __init__(self: "SfBulk", sf: Sf, interval: int = 5, timeout: int = 30) -> None:  # noqa: D107
        self.bulk2_url = sf.bulk2_url
        self.headers = sf.headers
        self._interval = interval
        self._timeout = timeout

    def _make_request(
        self: "SfBulk",
        method: str,
        endpoint: str,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> requests.Response:
        """Send an API request and check its status.

        Args:
            method: The HTTP method (e.g., 'GET', 'POST').
            endpoint: The API endpoint to append to the base bulk URL.
            headers: Optional additional headers for the request.
            **kwargs: Additional keyword arguments passed to `requests.request`.

        Returns:
            The response object from the API call.

        Raises:
            requests.exceptions.HTTPError: If the API request returns an error status code.

        """
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
        """Retrieve and parse CSV results from a given endpoint.

        Args:
            endpoint: The API endpoint for fetching CSV results.
            format_type: The desired output format.

        Returns:
            The parsed data in the specified format.

        Raises:
            ValueError: If an unsupported format_type is provided.

        """
        allowed_formats = get_args(FormatType.__value__)
        if format_type not in allowed_formats:
            err_msg = (
                f"Unsupported format: '{format_type}'. "
                f"Allowed formats are: {', '.join(allowed_formats)}"
            )
            raise ValueError(
                err_msg,
            )

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
        """Determine the final polling interval.

        It returns the provided interval, or the default if it's None.

        Args:
            interval: An optional interval in seconds.

        Returns:
            The determined interval in seconds.

        """
        return interval if interval is not None else self._interval

    # Query operations
    def create_job_query(
        self: "SfBulk",
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
        _operation = "query"
        if include_all:
            _operation += "All"

        _response = self._make_request(
            "POST",
            "query",
            json={
                "operation": _operation,
                "query": query,
            },
        )
        return SfBulkJobQuery(self, cast("dict[str, Any]", _response.json()))

    def get_job_query_info(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Get information about a specific query job.

        Args:
            job_id: The ID of the query job.

        Returns:
            A dictionary containing the job's information.

        """
        _response = self._make_request(
            "GET",
            f"query/{job_id}",
        )
        return cast("dict[str, Any]", _response.json())

    def poll_job_query(
        self: "SfBulk",
        job_id: str,
        interval: int | None = None,
    ) -> dict[str, Any]:
        """Poll a query job's status until it completes.

        Args:
            job_id: The ID of the query job to poll.
            interval: The polling interval in seconds. If None,
                the default is used.

        Returns:
            The final job information dictionary after completion.

        """
        while True:
            _job_info = self.get_job_query_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(self._get_final_interval(interval))

        return _job_info

    def get_job_query_results(
        self: "SfBulk",
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
        return self._get_csv_results(f"query/{job_id}/results", format_type)

    # CRUD operations
    def create_job_insert(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an insert job.

        Args:
            object_name: The Salesforce object API name (e.g., 'Account').

        Returns:
            SfBulkJob: An object to manage the created insert job.

        """
        return self.create_job(object_name, "insert")

    def create_job_update(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an update job.

        Args:
            object_name: The Salesforce object API name (e.g., 'Account').

        Returns:
            An object to manage the created update job.

        """
        return self.create_job(object_name, "update")

    def create_job_upsert(
        self: "SfBulk",
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
        return self.create_job(object_name, "upsert", external_id_field)

    def create_job_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a delete job (moves records to the recycle bin).

        Args:
            object_name: The Salesforce object API name (e.g., 'Account').

        Returns:
            An object to manage the created delete job.

        """
        return self.create_job(object_name, "delete")

    def create_job_hard_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a hard delete job (permanently deletes records).

        Args:
            object_name: The Salesforce object API name (e.g., 'Account').

        Returns:
            An object to manage the created hard delete job.

        """
        return self.create_job(object_name, "hardDelete")

    def create_job(
        self: "SfBulk",
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> SfBulkJob:
        """Create a job with a specified DML operation.

        Args:
            object_name: The Salesforce object API name.
            operation: The DML operation ('insert', 'update', 'upsert', etc.).
            external_id_field:
                The external ID field name, required
                for 'upsert' operations.

        Returns:
            An object to manage the created job.

        Raises:
            ValueError: If 'upsert' operation is specified without an
                `external_id_field`.

        """
        if operation == "upsert" and not external_id_field:
            error_msg = "The 'external_id_field' is required for the 'upsert' operation."
            raise ValueError(error_msg)

        _payload = {
            "object": object_name,
            "operation": operation,
        }

        if external_id_field:
            _payload["externalIdFieldName"] = external_id_field

        _response = self._make_request(
            "POST",
            "ingest",
            json=_payload,
        )

        return SfBulkJob(self, cast("dict[str, Any]", _response.json()))

    def upload_job_data(self: "SfBulk", job_id: str, csv_data: str) -> None:
        """Upload CSV data to a job.

        Args:
            job_id: The ID of the ingest job.
            csv_data: A string containing the data in CSV format.

        """
        self._make_request(
            "PUT",
            f"ingest/{job_id}/batches",
            headers={"Content-Type": "text/csv"},
            data=csv_data.encode("utf-8"),
        )

    def uploaded_job(self: "SfBulk", job_id: str) -> None:
        """Signal that data upload is complete for a job.

        This moves the job from the 'Open' state to the 'UploadComplete' state,
        making it ready for processing.

        Args:
            job_id: The ID of the ingest job.

        """
        self._make_request(
            "PATCH",
            f"ingest/{job_id}",
            json={"state": "UploadComplete"},
        )

    def get_job_info(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Get information about a specific ingest job.

        Args:
            job_id: The ID of the ingest job.

        Returns:
            A dictionary containing the job's information.

        """
        _response = self._make_request(
            "GET",
            f"ingest/{job_id}",
        )
        return cast("dict[str, Any]", _response.json())

    def poll_job(
        self: "SfBulk",
        job_id: str,
        interval: int | None = None,
    ) -> dict[str, Any]:
        """Poll an ingest job's status until it completes.

        Args:
            job_id: The ID of the ingest job to poll.
            interval:
                The polling interval in seconds. If None,
                the default is used.

        Returns:
            The final job information dictionary after completion.

        """
        while True:
            _job_info = self.get_job_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(self._get_final_interval(interval))

        return _job_info

    def get_ingest_successful_results(
        self: "SfBulk",
        job_id: str,
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get the successful records from a completed ingest job.

        Args:
            job_id: The ID of the ingest job.
            format_type:
                The desired output format ('dict', 'reader', 'csv').
                Defaults to 'dict'.

        Returns:
            The query results in the specified format.

        """
        return self._get_csv_results(f"ingest/{job_id}/successfulResults", format_type)

    def get_ingest_failed_results(
        self: "SfBulk",
        job_id: str,
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get the failed records from a completed ingest job.

        Args:
            job_id: The ID of the ingest job.
            format_type:
                The desired output format ('dict', 'reader', 'csv').
                Defaults to 'dict'.

        Returns:
            The query results in the specified format.

        """
        return self._get_csv_results(f"ingest/{job_id}/failedResults", format_type)

    def get_ingest_unprocessed_records(
        self: "SfBulk",
        job_id: str,
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get unprocessed records from an ingest job.

        These are records that were not processed because the job was aborted or
        a batch failed.

        Args:
            job_id: The ID of the ingest job.
            format_type:
                The desired output format ('dict', 'reader', 'csv').
                Defaults to 'dict'.

        Returns:
            The query results in the specified format.

        """
        return self._get_csv_results(f"ingest/{job_id}/unprocessedrecords", format_type)
