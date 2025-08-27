import io
from copy import deepcopy
from time import sleep
from typing import Any, cast

import pandas as pd
import requests

from .api import Sf
from .bulk_job import SfBulkJob, SfBulkJobQuery


class SfBulk:
    """Salesforce Bulk API client."""

    def __init__(self: "SfBulk", sf: Sf) -> None:
        """Initialize Bulk API client with Salesforce connection."""
        self.bulk2_url = sf.bulk2_url
        self.headers = sf.headers

    # Query operations
    def create_job_query_raw(
        self: "SfBulk",
        query: str,
        *,
        include_all: bool = False,
    ) -> dict[str, Any]:
        """Create a raw query job."""
        _operation = "query"
        if include_all:
            _operation += "All"

        _response = requests.post(
            f"{self.bulk2_url}query",
            headers=self.headers,
            json={"operation": _operation, "query": query},
            timeout=30,
        )
        _response.raise_for_status()

        return cast("dict[str, Any]", _response.json())

    def create_job_query(
        self: "SfBulk",
        query: str,
        *,
        include_all: bool = False,
    ) -> SfBulkJobQuery:
        """Create a query job."""
        return SfBulkJobQuery(
            self, self.create_job_query_raw(query, include_all=include_all)
        )

    def get_job_query_info(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Get query job information."""
        _response = requests.get(
            f"{self.bulk2_url}query/{job_id}",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        return cast("dict[str, Any]", _response.json())

    def poll_job_query(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Poll query job status until completion."""
        while True:
            _job_info = self.get_job_query_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(5)

        return _job_info

    def get_job_query_results(self: "SfBulk", job_id: str) -> pd.DataFrame:
        """Get query job results as DataFrame."""
        _response = requests.get(
            f"{self.bulk2_url}query/{job_id}/results",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        _response.encoding = "utf-8"
        return pd.read_csv(io.StringIO(_response.text))

    # CRUD operations
    def create_job_insert_raw(self: "SfBulk", object_name: str) -> dict[str, Any]:
        """Create a raw insert job."""
        return self.create_job_raw(object_name, "insert")

    def create_job_insert(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an insert job."""
        return self.create_job(object_name, "insert")

    def create_job_delete_raw(self: "SfBulk", object_name: str) -> dict[str, Any]:
        """Create a raw delete job."""
        return self.create_job_raw(object_name, "delete")

    def create_job_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a delete job."""
        return self.create_job(object_name, "delete")

    def create_job_hard_delete_raw(self: "SfBulk", object_name: str) -> dict[str, Any]:
        """Create a raw hard delete job."""
        return self.create_job_raw(object_name, "hardDelete")

    def create_job_hard_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a hard delete job."""
        return self.create_job(object_name, "hardDelete")

    def create_job_update_raw(self: "SfBulk", object_name: str) -> dict[str, Any]:
        """Create a raw update job."""
        return self.create_job_raw(object_name, "update")

    def create_job_update(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an update job."""
        return self.create_job(object_name, "update")

    def create_job_upsert_raw(
        self: "SfBulk",
        object_name: str,
        external_id_field: str,
    ) -> dict[str, Any]:
        """Create a raw upsert job."""
        return self.create_job_raw(object_name, "upsert", external_id_field)

    def create_job_upsert(
        self: "SfBulk", object_name: str, external_id_field: str
    ) -> SfBulkJob:
        """Create an upsert job."""
        return self.create_job(object_name, "upsert", external_id_field)

    def create_job_raw(
        self: "SfBulk",
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> dict[str, Any]:
        """Create a raw job with specified operation."""
        if operation == "upsert" and not external_id_field:
            error_msg = "operation が 'upsert' の場合、external_id_field は必須です。"
            raise ValueError(error_msg)

        _payload = {
            "object": object_name,
            "operation": operation,
        }

        if operation == "upsert" and external_id_field is not None:
            _payload["externalIdFieldName"] = external_id_field

        _response = requests.post(
            f"{self.bulk2_url}ingest",
            headers=self.headers,
            json=_payload,
            timeout=30,
        )
        _response.raise_for_status()

        return cast("dict[str, Any]", _response.json())

    def create_job(
        self: "SfBulk",
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> SfBulkJob:
        """Create a job with specified operation."""
        return SfBulkJob(
            self,
            self.create_job_raw(object_name, operation, external_id_field),
        )

    def upload_job_data(self: "SfBulk", job_id: str, csv_data: str) -> None:
        """Upload CSV data to a job."""
        _headers_csv = deepcopy(self.headers)
        _headers_csv["Content-Type"] = "text/csv"

        _response = requests.put(
            f"{self.bulk2_url}ingest/{job_id}/batches",
            data=csv_data.encode("utf-8"),
            headers=_headers_csv,
            timeout=30,
        )
        _response.raise_for_status()

    def uploaded_job(self: "SfBulk", job_id: str) -> None:
        """Mark job as uploaded."""
        _response = requests.patch(
            f"{self.bulk2_url}ingest/{job_id}",
            headers=self.headers,
            json={"state": "UploadComplete"},
            timeout=30,
        )
        _response.raise_for_status()

    def get_job_info(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Get job information."""
        _response = requests.get(
            f"{self.bulk2_url}ingest/{job_id}",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        return cast("dict[str, Any]", _response.json())

    def poll_job(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Poll job status until completion."""
        while True:
            _job_info = self.get_job_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(5)

        return _job_info

    def get_ingest_successful_results(self: "SfBulk", job_id: str) -> pd.DataFrame:
        """Get successful results from ingest job."""
        _response = requests.get(
            f"{self.bulk2_url}ingest/{job_id}/successfulResults",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        _response.encoding = "utf-8"
        return pd.read_csv(io.StringIO(_response.text))

    def get_ingest_failed_results(self: "SfBulk", job_id: str) -> pd.DataFrame:
        """Get failed results from ingest job."""
        _response = requests.get(
            f"{self.bulk2_url}ingest/{job_id}/failedResults",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        _response.encoding = "utf-8"
        return pd.read_csv(io.StringIO(_response.text))

    def get_ingest_unprocessed_records(self: "SfBulk", job_id: str) -> pd.DataFrame:
        """Get unprocessed records from ingest job."""
        _response = requests.get(
            f"{self.bulk2_url}ingest/{job_id}/unprocessedrecords",
            headers=self.headers,
            timeout=30,
        )
        _response.raise_for_status()
        _response.encoding = "utf-8"
        return pd.read_csv(io.StringIO(_response.text))
