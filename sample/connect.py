# ruff: noqa: INP001 D100
import logging
import os

from dotenv import load_dotenv

from custom_simple_salesforce import Sf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


load_dotenv()

# env
_ENV = os.environ.copy()

DOMAIN = _ENV.get("DOMAIN")
USERNAME = _ENV.get("USERNAME")
PASSWORD = _ENV.get("PASSWORD")
SECURITY_TOKEN = _ENV.get("SECURITY_TOKEN")

CLIENT_ID = _ENV.get("CLIENT_ID")
CLIENT_SECRET = _ENV.get("CLIENT_SECRET")

logger.info(os.environ.copy())


def main() -> None:
    id_json = {
        "auth_method": "password",
        "username": USERNAME,
        "password": PASSWORD,
        "security_token": SECURITY_TOKEN,
        "domain": "login",
    }

    sf_client = Sf.connection(id_json)
    logger.info("username + password dict 接続成功!")

    id_json_str = f"""{{
        "auth_method": "password",
        "username": {USERNAME},
        "password": {PASSWORD},
        "security_token": {SECURITY_TOKEN},
        "domain": "login",
    }}"""

    sf_client = Sf.connection(id_json_str)
    logger.info("username + password dict String 接続成功!")

    # https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_client_credentials_flow.htm&type=5
    credential_yaml = f"""
    auth_method: client_credentials
    client_id: {CLIENT_ID}
    client_secret: {CLIENT_SECRET}
    domain: {DOMAIN}
    """

    sf_client = Sf.connection(credential_yaml)
    logger.info("credential_yaml 接続成功!")

    credential_env = f"""
    AUTH_METHOD: client_credentials
    CLIENT_ID: {CLIENT_ID}
    CLIENT_SECRET: {CLIENT_SECRET}
    DOMAIN={DOMAIN}
    """

    sf_client = Sf.connection(credential_env)
    logger.info("credential_env 接続成功!")

    _response = sf_client.query("select id, Name from account limit 10")

    for record in _response["records"][:3]:
        logger.info(record)


if __name__ == "__main__":
    main()
