from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    database_path: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()