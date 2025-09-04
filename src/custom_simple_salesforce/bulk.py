import csv
import io
from copy import deepcopy
from time import sleep
from typing import Any, cast

import requests

from .bulk_job import SfBulkJob, SfBulkJobQuery
from .client import Sf


class SfBulk:
    """Salesforce Bulk API 2.0クライアント。

    本クラスは、クエリやDML操作(挿入、更新、削除)を Bulk API 2.0経由で実行するための機能を提供します。
    """

    # 共通部
    def __init__(self: "SfBulk", sf: Sf, interval: int = 5, timeout: int = 30) -> None:
        """Initialize Bulk API client with Salesforce connection."""
        self.bulk2_url = sf.bulk2_url
        self.headers = sf.headers
        self._interval = interval
        self._timeout = timeout

    def _make_request(
        self: "SfBulk",
        method: str,
        endpoint: str,
        headers: dict | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> requests.Response:
        """APIリクエストを送信し、ステータスを確認するプライベートヘルパーメソッド。"""
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
        format_type: str,
    ) -> list[dict[str, Any]] | list[list[str]] | str:
        allowed_formats = ("dict", "reader", "csv")
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
                # ここには到達しないはず
                raise ValueError("Internal error: Unsupported format.")

    def _get_final_interval(self, interval: int) -> int:
        """ポーリング間隔の最終値を決定するヘルパーメソッド。"""
        return interval if interval is not None else self._interval

    # Query operations
    def create_job_query(
        self: "SfBulk",
        query: str,
        *,
        include_all: bool = False,
    ) -> SfBulkJobQuery:
        """Create a query job."""
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
        """Get query job information."""
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
        """Poll query job status until completion."""
        while True:
            _job_info = self.get_job_query_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(self._get_final_interval(interval))

        return _job_info

    def get_job_query_results(
        self: "SfBulk",
        job_id: str,
        format_type: str = "dict",
    ) -> Any:
        """Get query job results in a specified format."""
        return self._get_csv_results(f"query/{job_id}/results", format_type)

    # CRUD operations
    def create_job_insert(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an insert job."""
        return self.create_job(object_name, "insert")

    def create_job_update(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create an update job."""
        return self.create_job(object_name, "update")

    def create_job_upsert(
        self: "SfBulk",
        object_name: str,
        external_id_field: str,
    ) -> SfBulkJob:
        """Create an upsert job."""
        return self.create_job(object_name, "upsert", external_id_field)

    def create_job_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a delete job."""
        return self.create_job(object_name, "delete")

    def create_job_hard_delete(self: "SfBulk", object_name: str) -> SfBulkJob:
        """Create a hard delete job."""
        return self.create_job(object_name, "hardDelete")

    def create_job(
        self: "SfBulk",
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> SfBulkJob:
        """Create a job with specified operation."""
        if operation == "upsert" and not external_id_field:
            error_msg = "operation が 'upsert' の場合、external_id_field は必須です。"
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
        """Upload CSV data to a job."""
        self._make_request(
            "PUT",
            f"ingest/{job_id}/batches",
            headers={"Content-Type": "text/csv"},
            data=csv_data.encode("utf-8"),
        )

    def uploaded_job(self: "SfBulk", job_id: str) -> None:
        """Mark job as uploaded."""
        self._make_request(
            "PATCH",
            f"ingest/{job_id}",
            json={"state": "UploadComplete"},
        )

    def get_job_info(self: "SfBulk", job_id: str) -> dict[str, Any]:
        """Get job information."""
        _response = self._make_request(
            "GET",
            f"ingest/{job_id}",
        )
        return cast("dict[str, Any]", _response.json())

    def poll_job(
        self: "SfBulk",
        job_id: str,
        interval: int = None,
    ) -> dict[str, Any]:
        """Poll job status until completion."""
        while True:
            _job_info = self.get_job_info(job_id)

            if _job_info["state"] in ["Aborted", "JobComplete", "Failed"]:
                break

            sleep(self._get_final_interval(interval))

        return _job_info

    def get_ingest_successful_results(
        self: "SfBulk",
        job_id: str,
        format: str = "dict",
    ) -> Any:
        """Get successful results from ingest job."""
        return self._get_csv_results(f"ingest/{job_id}/successfulResults", format)

    def get_ingest_failed_results(
        self: "SfBulk",
        job_id: str,
        format: str = "dict",
    ) -> Any:
        """Get failed results from ingest job."""
        return self._get_csv_results(f"ingest/{job_id}/failedResults", format)

    def get_ingest_unprocessed_records(
        self: "SfBulk",
        job_id: str,
        format: str = "dict",
    ) -> Any:
        """Get unprocessed records from ingest job."""
        return self._get_csv_results(f"ingest/{job_id}/unprocessedrecords", format)
