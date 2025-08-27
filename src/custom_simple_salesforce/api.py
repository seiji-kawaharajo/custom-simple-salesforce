from typing import Any, Dict, Literal, Union

import requests
import yaml
from pydantic import BaseModel, SecretStr, ValidationError
from simple_salesforce.api import Salesforce


class PasswordAuthSettings(BaseModel):
    auth_method: str
    username: str
    password: SecretStr
    security_token: SecretStr
    domain: Literal["login", "test"] = "login"
    api_version: str = "64.0"


class ClientCredentialsSettings(BaseModel):
    auth_method: str
    client_id: str
    client_secret: SecretStr
    domain: str = "login"
    api_version: str = "64.0"


class Sf(Salesforce):
    def __init__(self: "Sf", *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        super().__init__(*args, **kwargs)

    @classmethod
    def connection(cls: type["Sf"], settings: Union[str, Dict[str, Any]]):
        config: Dict[str, Any]
        if isinstance(settings, str):
            try:
                config = yaml.safe_load(settings) or {}
            except yaml.YAMLError as e:
                raise ValueError(f"設定文字列の形式が不正です: {e}") from e
        elif isinstance(settings, dict):
            config = settings
        else:
            raise TypeError("設定はJSON文字列または辞書形式で指定してください。")

        auth_method = config.get("auth_method")

        match auth_method:
            case "password":
                try:
                    validated_settings = PasswordAuthSettings.model_validate(config)
                except ValidationError as e:
                    error_msg = f"Salesforce設定のバリデーションに失敗しました: {e}"
                    raise ValueError(error_msg) from e
                except (ValueError, TypeError) as e:
                    error_msg = f"設定パース中に予期せぬエラーが発生しました: {e}"
                    raise ValueError(error_msg) from e

                return cls(
                    username=validated_settings.username,
                    password=validated_settings.password.get_secret_value(),
                    security_token=validated_settings.security_token.get_secret_value(),
                    domain=validated_settings.domain,
                    version=validated_settings.api_version,
                )

            case "client_credentials":
                try:
                    validated_settings = ClientCredentialsSettings.model_validate(
                        config
                    )
                except ValidationError as e:
                    error_msg = f"Salesforce設定のバリデーションに失敗しました: {e}"
                    raise ValueError(error_msg) from e
                except (ValueError, TypeError) as e:
                    error_msg = f"設定パース中に予期せぬエラーが発生しました: {e}"
                    raise ValueError(error_msg) from e

                match validated_settings.domain:
                    case "login":
                        _endpoint = "https://login.salesforce.com"
                    case "test":
                        _endpoint = "https://test.salesforce.com"
                    case _:
                        _endpoint = (
                            f"https://{validated_settings.domain}.my.salesforce.com"
                        )

                _response = requests.post(
                    f"{_endpoint}/services/oauth2/token",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={
                        "grant_type": "client_credentials",
                        "client_id": validated_settings.client_id,
                        "client_secret": validated_settings.client_secret.get_secret_value(),
                    },
                )
                _response.raise_for_status()

                _response_json = _response.json() or {}
                return cls(
                    instance_url=_response_json.get("instance_url"),
                    session_id=_response_json.get("access_token"),
                    version=validated_settings.api_version,
                )
