from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    raw_taq_path: str
    output_path: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()