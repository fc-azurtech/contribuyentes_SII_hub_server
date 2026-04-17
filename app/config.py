import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    app_name: str = os.getenv("APP_NAME", "Taxpayer Hub")
    app_host: str = os.getenv("APP_HOST", "0.0.0.0")
    app_port: int = int(os.getenv("APP_PORT", "8787"))
    app_secret_key: str = os.getenv("APP_SECRET_KEY", "change-me")

    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://taxpayer_hub:taxpayer_hub@localhost:5432/taxpayer_hub",
    )

    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "change-me")

    seed_api_client_name: str = os.getenv("SEED_API_CLIENT_NAME", "odoo-main")
    seed_api_client_key: str = os.getenv("SEED_API_CLIENT_KEY", "change-me")

    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_USERNAME", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    smtp_tls: bool = os.getenv("SMTP_TLS", "true").lower() in {"1", "true", "yes"}
    smtp_from: str = os.getenv("SMTP_FROM", "alerts@example.com")
    alert_email_to: str = os.getenv("ALERT_EMAIL_TO", "ops@example.com")

    sii_direcciones_url: str = os.getenv(
        "SII_DIRECCIONES_URL", "https://www.sii.cl/estadisticas/nominas/PUB_NOM_DIRECCIONES.zip"
    )
    sii_actecos_url: str = os.getenv(
        "SII_ACTECOS_URL", "https://www.sii.cl/estadisticas/nominas/PUB_NOM_ACTECOS.zip"
    )
    sii_base_contribuyentes_url: str = os.getenv("SII_BASE_CONTRIBUYENTES_URL", "")

    sync_weekday: str = os.getenv("SYNC_WEEKDAY", "sun")
    sync_hour: int = int(os.getenv("SYNC_HOUR", "3"))
    sync_minute: int = int(os.getenv("SYNC_MINUTE", "30"))
    sync_frequency: str = os.getenv("SYNC_FREQUENCY", "weekly")
    sync_weekdays: str = os.getenv("SYNC_WEEKDAYS", os.getenv("SYNC_WEEKDAY", "sun"))
    sync_months: str = os.getenv("SYNC_MONTHS", "")
    sync_day_of_month: int = int(os.getenv("SYNC_DAY_OF_MONTH", "1"))
    sync_yearly_month: int = int(os.getenv("SYNC_YEARLY_MONTH", "1"))
    sync_timezone: str = os.getenv("SYNC_TIMEZONE", "America/Santiago")
    sync_download_timeout: int = int(os.getenv("SYNC_DOWNLOAD_TIMEOUT", "180"))
    sync_download_retries: int = int(os.getenv("SYNC_DOWNLOAD_RETRIES", "3"))
    sync_download_backoff_seconds: int = int(os.getenv("SYNC_DOWNLOAD_BACKOFF_SECONDS", "3"))

    sii_auth_enabled: bool = os.getenv("SII_AUTH_ENABLED", "false").lower() in {"1", "true", "yes"}
    sii_auth_cert_mode: str = os.getenv("SII_AUTH_CERT_MODE", "pfx")
    sii_auth_pfx_path: str = os.getenv("SII_AUTH_PFX_PATH", "")
    sii_auth_pfx_password: str = os.getenv("SII_AUTH_PFX_PASSWORD", "")
    sii_auth_cert_path: str = os.getenv("SII_AUTH_CERT_PATH", "")
    sii_auth_key_path: str = os.getenv("SII_AUTH_KEY_PATH", "")
    sii_auth_query_url: str = os.getenv("SII_AUTH_QUERY_URL", "https://palena.sii.cl/cvc_cgi/dte/ce_consulta_rut")
    sii_auth_timeout: int = int(os.getenv("SII_AUTH_TIMEOUT", "30"))
    sii_auth_retries: int = int(os.getenv("SII_AUTH_RETRIES", "2"))
    sii_auth_backoff_seconds: int = int(os.getenv("SII_AUTH_BACKOFF_SECONDS", "2"))
    sii_auth_delay_ms: int = int(os.getenv("SII_AUTH_DELAY_MS", "250"))
    sii_auth_batch_size: int = int(os.getenv("SII_AUTH_BATCH_SIZE", "250"))


settings = Settings()
