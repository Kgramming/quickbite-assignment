"""Application configuration using Pydantic Settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # API Configuration
    api_host: str = "127.0.0.1"
    api_port: int = 8002
    log_level: str = "info"

    # Database Configuration
    database_url: str = "postgresql+psycopg://quickbite_user:quickbite_password@127.0.0.1:5434/quickbite_db"
    database_echo: bool = False

    # RabbitMQ Configuration
    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "quickbite"
    rabbitmq_password: str = "quickbite_password"
    rabbitmq_vhost: str = "/"

    # Application
    environment: str = "development"
    debug: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
