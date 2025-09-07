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
    info: dict[str, Any]

    def __init__(self: "_SfBulkJobBase", sf_bulk: "SfBulk", job_info: dict[str, Any]) -> None:
        """Initialize common job attributes.

        Args:
            sf_bulk (SfBulk): The Bulk API client instance.
            job_info (dict): The initial job information from the API.

        """
        self._sf_bulk = sf_bulk
        self.id = job_info["id"]
        self.info = job_info


class SfBulkJobQuery(_SfBulkJobBase):
    """Manage a Salesforce Bulk API query job.

    This class simplifies polling for job status and retrieving results for a
    specific query job.

    Attributes:
        _sf_bulk (SfBulk): The Bulk API client instance used for API communication.
        id (str): The unique ID for the Bulk API job.
        info (dict[str, Any]): A dictionary holding the latest metadata and
            status for the job, which is updated after polling.

    Args:
        sf_bulk (SfBulk): The Bulk API client instance.
        job_info (dict): The initial job information from the API.

    """

    def poll_status(self: "SfBulkJobQuery", interval: int | None = None) -> dict[str, Any]:
        """Poll the job status until it reaches a terminal state.

        The terminal states are 'JobComplete', 'Aborted', or 'Failed'.
        Updates `self.info` with the final job status.

        Args:
            interval: The polling interval in seconds. If None,
                the client's default is used.

        Returns:
            The final job information dictionary.

        """
        self.info = self._sf_bulk.poll_job_query(self.id, interval=interval)
        return self.info

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
        return self._sf_bulk.get_job_query_results(self.id, format_type=format_type)


class SfBulkJob(_SfBulkJobBase):
    """Manage a Salesforce Bulk API DML job (e.g., insert, update, delete).

    Supports uploading CSV data, closing the job, polling its status, and
    retrieving successful or failed records.

    Attributes:
        _sf_bulk (SfBulk): The Bulk API client instance used for API communication.
        id (str): The unique ID for the Bulk API job.
        info (dict[str, Any]): A dictionary holding the latest metadata and
            status for the job, which is updated after polling.

    Args:
        sf_bulk (SfBulk): The Bulk API client instance.
        job_info (dict): The initial job information from the API.

    """

    def upload_data(self: "SfBulkJob", csv_data: str) -> None:
        """Upload CSV data to the job.

        Args:
            csv_data: A string containing the data in CSV format.

        """
        self._sf_bulk.upload_job_data(job_id=self.id, csv_data=csv_data)

    def close(self: "SfBulkJob") -> None:
        """Close the job, marking it as ready for processing.

        This signals to Salesforce that all data has been uploaded.
        """
        self._sf_bulk.uploaded_job(job_id=self.id)

    def poll_status(self: "SfBulkJob", interval: int | None = None) -> dict[str, Any]:
        """Poll the job status until it reaches a terminal state.

        The terminal states are 'JobComplete', 'Aborted', or 'Failed'.
        Updates `self.info` with the final job status.

        Args:
            interval: The polling interval in seconds. If None,
                the client's default is used.

        Returns:
            The final job information dictionary.

        """
        self.info = self._sf_bulk.poll_job(job_id=self.id, interval=interval)
        return self.info

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

    def get_successful_results(self: "SfBulkJob", format_type: FormatType = "dict") -> ResultType:
        """Get the successfully processed records.

        Args:
            format_type: The desired output format.

        Returns:
            The successful records in the specified format.

        """
        return self._sf_bulk.get_ingest_successful_results(
            job_id=self.id,
            format_type=format_type,
        )

    def get_failed_results(self: "SfBulkJob", format_type: FormatType = "dict") -> ResultType:
        """Get the records that failed to process.

        The results include error messages from Salesforce.

        Args:
            format_type: The desired output format.

        Returns:
            The failed records in the specified format.

        """
        return self._sf_bulk.get_ingest_failed_results(job_id=self.id, format_type=format_type)

    def get_unprocessed_records(self: "SfBulkJob", format_type: FormatType = "dict") -> ResultType:
        """Get records that were not processed.

        This typically occurs if the job is aborted or a batch fails before
        these records could be processed.

        Args:
            format_type: The desired output format.

        Returns:
            The unprocessed records in the specified format.

        """
        return self._sf_bulk.get_ingest_unprocessed_records(
            job_id=self.id,
            format_type=format_type,
        )
