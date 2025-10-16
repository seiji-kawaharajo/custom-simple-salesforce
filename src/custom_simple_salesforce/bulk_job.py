"""Salesforce Bulk Job Module.

This module provides classes for managing Salesforce Bulk API 2.0 jobs,
including query jobs and DML operation jobs. These classes act as wrappers
around a bulk client instance to simplify job-specific operations.
"""

from typing import TYPE_CHECKING, Any

from .types import FormatType, ResultType

if TYPE_CHECKING:
    from .bulk import SfBulk


class _SfBulkJobBase:
    """Base class for managing a Salesforce Bulk API job.

    This class provides common initialization for bulk jobs.
    It is not intended to be instantiated directly.
    """

    _sf_bulk: "SfBulk"
    id: str
    _info: dict[str, Any]

    def __init__(
        self: "_SfBulkJobBase",
        sf_bulk: "SfBulk",
        job_info: dict[str, Any],
    ) -> None:
        """Initialize common job attributes.

        Args:
            sf_bulk (SfBulk): The Bulk API client instance.
            job_info (dict): The initial job information from the API.

        """
        self._sf_bulk = sf_bulk
        self.id = job_info["id"]
        self._info = job_info

    @property
    def info(self: "_SfBulkJobBase") -> dict[str, Any]:
        """Get the last known job information (read-only).

        This property provides access to the job's state as of the last update
        (e.g., after initialization, `get_info()`, or `wait()`).
        To fetch the latest status from Salesforce, use the `get_info()` method.
        """
        return self._info


class SfBulkJobQuery(_SfBulkJobBase):
    """Manage a Salesforce Bulk API query job.

    This class simplifies waiting for job status and retrieving results for a
    specific query job.

    Attributes:
        _sf_bulk (SfBulk): The Bulk API client instance used for API communication.
        id (str): The unique ID for the Bulk API job.
        info (dict[str, Any]): A dictionary holding the latest metadata and
            status for the job, which is updated after waiting.

    Args:
        sf_bulk (SfBulk): The Bulk API client instance.
        job_info (dict): The initial job information from the API.

    """

    def get_info(self: "SfBulkJobQuery") -> dict[str, Any]:
        """Fetch the latest job information from Salesforce.

        Updates `self.info` with the latest status.

        Returns:
            The latest job information dictionary.

        """
        self._info = self._sf_bulk.query.get_info(self.id)
        return self._info

    def wait(self: "SfBulkJobQuery", interval: int | None = None) -> dict[str, Any]:
        """Wait the job status until it reaches a terminal state.

        The terminal states are 'JobComplete', 'Aborted', or 'Failed'.
        Updates `self.info` with the final job status.

        Args:
            interval: The waiting interval in seconds. If None,
                the client's default is used.

        Returns:
            The final job information dictionary.

        """
        self._info = self._sf_bulk.query.wait(self.id, interval=interval)
        return self._info

    def get_results(
        self: "SfBulkJobQuery",
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get the job results in the specified format.

        Args:
            format_type: The desired output format.

        Returns:
            The query results.

        """
        return self._sf_bulk.query.get_results(self.id, format_type=format_type)


class SfBulkJob(_SfBulkJobBase):
    """Manage a Salesforce Bulk API DML job (e.g., insert, update, delete).

    Supports uploading CSV data, marking the upload as complete, waiting its status, and
    retrieving successful or failed records.

    Attributes:
        _sf_bulk (SfBulk): The Bulk API client instance used for API communication.
        id (str): The unique ID for the Bulk API job.
        info (dict[str, Any]): A dictionary holding the latest metadata and
            status for the job, which is updated after waiting.

    Args:
        sf_bulk (SfBulk): The Bulk API client instance.
        job_info (dict): The initial job information from the API.

    """

    def upload_data(self: "SfBulkJob", csv_data: str) -> None:
        """Upload CSV data to the job.

        Args:
            csv_data: A string containing the data in CSV format.

        """
        self._sf_bulk.ingest.upload_data(job_id=self.id, csv_data=csv_data)

    def complete_upload(
        self: "SfBulkJob",
    ) -> None:  # メソッド名を 'close' から 'complete_upload' に変更
        """Mark the job's data upload as complete.

        This signals to Salesforce that all data has been uploaded and the job
        is ready for processing (transitions to 'UploadComplete' state).
        """
        self._sf_bulk.ingest.complete_upload(job_id=self.id)

    def get_info(self: "SfBulkJob") -> dict[str, Any]:
        """Fetch the latest job information from Salesforce.

        Updates `self.info` with the latest status.

        Returns:
            The latest job information dictionary.

        """
        self._info = self._sf_bulk.ingest.get_info(job_id=self.id)
        return self._info

    def wait(self: "SfBulkJob", interval: int | None = None) -> dict[str, Any]:
        """Wait the job status until it reaches a terminal state.

        The terminal states are 'JobComplete', 'Aborted', or 'Failed'.
        Updates `self.info` with the final job status.

        Args:
            interval: The waiting interval in seconds. If None,
                the client's default is used.

        Returns:
            The final job information dictionary.

        """
        self._info = self._sf_bulk.ingest.wait(job_id=self.id, interval=interval)
        return self._info

    def is_successful(self: "SfBulkJob") -> bool:
        """Check if the job completed successfully.

        Returns:
            True if the job state is 'JobComplete', False otherwise.

        """
        return self.info.get("state") == "JobComplete"

    def has_failed_records(self: "SfBulkJob") -> bool:
        """Check if the job has any failed records.

        Returns:
            True if the number of failed records is greater than 0.

        """
        return int(self.info.get("numberRecordsFailed", 0)) > 0

    def is_failed(self: "SfBulkJob") -> bool:
        """Check if the entire job failed.

        Returns:
            True if the job state is 'Failed', False otherwise.

        """
        return self.info.get("state") == "Failed"

    def is_aborted(self: "SfBulkJob") -> bool:
        """Check if the job was aborted.

        Returns:
            True if the job state is 'Aborted', False otherwise.

        """
        return self.info.get("state") == "Aborted"

    def get_successful_results(
        self: "SfBulkJob",
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get the successfully processed records.

        Args:
            format_type: The desired output format.

        Returns:
            The successful records in the specified format.

        """
        return self._sf_bulk.ingest.get_successful_results(
            job_id=self.id,
            format_type=format_type,
        )

    def get_failed_results(
        self: "SfBulkJob",
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get the records that failed to process.

        The results include error messages from Salesforce.

        Args:
            format_type: The desired output format.

        Returns:
            The failed records in the specified format.

        """
        return self._sf_bulk.ingest.get_failed_results(
            job_id=self.id,
            format_type=format_type,
        )

    def get_unprocessed_records(
        self: "SfBulkJob",
        format_type: FormatType = "dict",
    ) -> ResultType:
        """Get records that were not processed.

        This typically occurs if the job is aborted or a batch fails before
        these records could be processed.

        Args:
            format_type: The desired output format.

        Returns:
            The unprocessed records in the specified format.

        """
        return self._sf_bulk.ingest.get_unprocessed_records(
            job_id=self.id,
            format_type=format_type,
        )
