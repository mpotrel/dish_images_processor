from functools import lru_cache
import pathlib

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from pydantic_settings.main import SettingsConfigDict
import yaml


class AppSettings(BaseSettings):
    FAL_KEY: str = Field(..., description="FAL API key")
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., description="Kafka bootstrap servers")
    MAX_CONCURRENT_REQUESTS: int = Field(..., gt=0, description="Maximum number of concurrent requests")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        validate_default=True
    )

    @field_validator('FAL_KEY', 'KAFKA_BOOTSTRAP_SERVERS', 'MAX_CONCURRENT_REQUESTS')
    @classmethod
    def validate_not_empty(cls, v: str | int) -> str | int:
        if isinstance(v, str) and not v.strip():
            raise ValueError("Field cannot be empty")
        if isinstance(v, int) and v <= 0:
            raise ValueError("MAX_CONCURRENT_REQUESTS must be greater than 0")
        return v

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
