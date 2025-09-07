# ruff: noqa: INP001 D100
import logging
import os

from dotenv import load_dotenv

from custom_simple_salesforce import Sf, SfBulk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


load_dotenv()

# env
_ENV = os.environ.copy()

CLIENT_ID = _ENV.get("CLIENT_ID")
CLIENT_SECRET = _ENV.get("CLIENT_SECRET")
DOMAIN = _ENV.get("DOMAIN")


def main() -> None:
    """Bulk Sample."""
    settings = {
        "auth_method": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "domain": DOMAIN,
    }

    sf_client = Sf.connection(settings)
    logger.info("Connection successful!")

    # Bulk client
    bulk_client = SfBulk(sf_client)

    logger.info("Create query job")
    job = bulk_client.query.create("select id, Name from account")
    job.wait()

    logger.info("Result with format as dictionary")
    results_list_dict = job.get_results()
    for record in results_list_dict[:3]:
        logger.info(record)

    logger.info("Result with format as list csv")
    results_list_str = job.get_results(format_type="reader")
    for record in results_list_str[:3]:
        logger.info(record)

    logger.info("Result with format as raw csv data")
    results_csv = job.get_results(format_type="csv")
    logger.info("%s...", results_csv[:30])

    # create insert job
    insert_job = bulk_client.ingest.create_insert("Account")
    logger.info(insert_job.info)

    csv_data = """Name,Industry
    Test Account 1,Technology
    Test Account 2,Finance"""

    insert_job.upload_data(csv_data)
    insert_job.complete_upload()
    insert_job.wait()

    if insert_job.is_successful():
        logger.info("Job completed.")
        if insert_job.has_failed_records():
            logger.info("But some records failed.")
            failed_results = insert_job.get_failed_results()
            logger.info(failed_results)
            # Logging of failed records
        else:
            logger.info("All successful.")
            successful_results = insert_job.get_successful_results()
            logger.info(successful_results)
    elif insert_job.is_failed():
        logger.info("Job failed.")
        logger.info("Job Info", extra=insert_job.info)
    elif insert_job.is_aborted():
        logger.info("Job was interrupted.")


if __name__ == "__main__":
    main()
