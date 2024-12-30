from functools import lru_cache
import pathlib
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from pydantic_settings.main import SettingsConfigDict
import yaml


class AppSettings(BaseSettings):
    # HACK: This is just so Pyright doesn't flag false errors
    FAL_KEY: Optional[str] = None
    KAFKA_BOOTSTRAP_SERVERS: Optional[str] = None
    MAX_CONCURRENT_REQUESTS: Optional[int] = None

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    services_config: dict = Field(
        default_factory=lambda: yaml.safe_load((pathlib.Path().cwd() / "dish_images_processor" / "config" / "services.yml").read_text())
    )

class ServiceSettings(BaseModel):
    app: AppSettings = Field(default_factory = lambda: AppSettings())
    endpoint: str
    arguments: dict

@lru_cache
def get_app_settings() -> AppSettings:
    return AppSettings()

@lru_cache
def get_services_settings() -> dict:
    services_settings_path = pathlib.Path().cwd() / "dish_images_processor" / "config" / "services.yml"
    try:
        return yaml.safe_load(services_settings_path.read_text())
    except FileNotFoundError:
        raise FileNotFoundError(f"Services configuration file not found at {services_settings_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in services configuration: {e}")

def get_settings(service_name: str) -> ServiceSettings:
    service_settings = get_services_settings().get(service_name)
    if service_settings is None:
        raise ValueError(f"Service {service_name} not found. Available services: {list(get_services_settings().keys())}")
    return ServiceSettings(**service_settings)
