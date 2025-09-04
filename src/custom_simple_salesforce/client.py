"""A client module for managing connections to the Salesforce API.

This module abstracts password and client credentials authentication methods, making it
easier to manage connection settings. It extends the `simple-salesforce` library to
provide more flexible connection options.
"""

from typing import Any, Literal

import requests
import yaml
from pydantic import BaseModel, SecretStr, ValidationError
from simple_salesforce.api import Salesforce


class SalesforceBaseSettings(BaseModel):
    """Base settings for Salesforce API authentication."""

    auth_method: Literal["password", "client_credentials"]
    api_version: str = "64.0"


class PasswordAuthSettings(SalesforceBaseSettings):
    """Settings for Salesforce API authentication using a username and password."""

    username: str
    password: SecretStr
    security_token: SecretStr
    domain: Literal["login", "test"] = "login"


class ClientCredentialsSettings(SalesforceBaseSettings):
    """Settings for Salesforce API authentication using a client ID and client secret."""

    client_id: str
    client_secret: SecretStr
    domain: str = "login"


class Sf(Salesforce):
    """Salesforce client.

    This class extends the `simple-salesforce` Salesforce class to abstract
    authentication methods (password, client credentials) and simplify connection
    management. It can be used in the same way as the base class due to inheritance.
    """

    def __init__(self: "Sf", *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the Salesforce client.

        Args:
            *args: A list of variable-length positional arguments.
            **kwargs: A dictionary of arbitrary keyword arguments.

        """
        super().__init__(*args, **kwargs)

    @classmethod
    def _connect_with_password(cls: type["Sf"], config: dict[str, Any]) -> "Sf":
        """Connect to Salesforce using password authentication."""
        try:
            validated_settings = PasswordAuthSettings.model_validate(config)
        except ValidationError as e:
            error_msg = f"Failed to validate Salesforce settings: {e}"
            raise ValueError(error_msg) from e
        except (ValueError, TypeError) as e:
            error_msg = f"An unexpected error occurred during settings parsing: {e}"
            raise ValueError(error_msg) from e

        return cls(
            username=validated_settings.username,
            password=validated_settings.password.get_secret_value(),
            security_token=validated_settings.security_token.get_secret_value(),
            domain=validated_settings.domain,
            version=validated_settings.api_version,
        )

    @classmethod
    def _connect_with_client_credentials(cls: type["Sf"], config: dict[str, Any]) -> "Sf":
        """Connect to Salesforce using client credentials authentication."""
        try:
            validated_settings = ClientCredentialsSettings.model_validate(
                config,
            )
        except ValidationError as e:
            error_msg = f"Failed to validate Salesforce settings: {e}"
            raise ValueError(error_msg) from e
        except (ValueError, TypeError) as e:
            error_msg = f"An unexpected error occurred during settings parsing: {e}"
            raise ValueError(error_msg) from e

        match validated_settings.domain:
            case "login":
                _endpoint = "https://login.salesforce.com"
            case "test":
                _endpoint = "https://test.salesforce.com"
            case _:
                _endpoint = f"https://{validated_settings.domain}.my.salesforce.com"

        _response = requests.post(
            f"{_endpoint}/services/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": validated_settings.client_id,
                "client_secret": validated_settings.client_secret.get_secret_value(),
            },
            timeout=30,
        )
        _response.raise_for_status()

        _response_json = _response.json() or {}
        return cls(
            instance_url=_response_json.get("instance_url"),
            session_id=_response_json.get("access_token"),
            version=validated_settings.api_version,
        )

    @classmethod
    def connection(cls: type["Sf"], settings: str | dict[str, Any]) -> "Sf":
        """Establish a connection to Salesforce based on provided settings.

        This class method validates the provided settings and handles the connection
        using either "password" or "client_credentials" authentication methods.

        Args:
            settings: A settings string (in YAML format) or a dictionary
                      containing authentication details. Supported authentication
                      methods are "password" and "client_credentials".

        Returns:
            Sf: An instance of the connected Salesforce client.

        Raises:
            ValueError: If the settings are invalid, authentication fails, or
                        the authentication method is not supported.
            TypeError: If the settings are not a dictionary or a string.
            yaml.YAMLError: If the settings string is not a valid YAML format.
            requests.exceptions.HTTPError: If the HTTP request for client
                                          credentials fails.

        """
        config: dict[str, Any]
        if isinstance(settings, str):
            try:
                config = yaml.safe_load(settings) or {}
            except yaml.YAMLError as e:
                error_msg = f"Invalid settings string format: {e}"
                raise ValueError(error_msg) from e
        elif isinstance(settings, dict):
            config = settings
        else:
            error_msg = "Settings must be provided as a JSON string or a dictionary."
            raise TypeError(error_msg)

        config = {k.lower(): v for k, v in config.items()}

        auth_method = config.get("auth_method")

        match auth_method:
            case "password":
                return cls._connect_with_password(config)

            case "client_credentials":
                return cls._connect_with_client_credentials(config)
            case _:
                error_msg = f"Unexpected authentication method specified: {auth_method}"
                raise ValueError(error_msg)
