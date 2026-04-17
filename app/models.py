from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class AdminUser(Base):
    __tablename__ = "admin_user"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ApiClient(Base):
    __tablename__ = "api_client"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class SystemSetting(Base):
    __tablename__ = "system_setting"

    key: Mapped[str] = mapped_column(String(120), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Taxpayer(Base):
    __tablename__ = "taxpayer"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rut_clean: Mapped[str] = mapped_column(String(16), unique=True, index=True, nullable=False)
    rut_formatted: Mapped[str] = mapped_column(String(24), nullable=False)
    legal_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    dte_email: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    address: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    city: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    parish: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="sii_weekly")
    is_override: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    activities: Mapped[list["TaxpayerActivity"]] = relationship(
        "TaxpayerActivity", back_populates="taxpayer", cascade="all, delete-orphan"
    )


class ActivityCatalog(Base):
    __tablename__ = "activity_catalog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")


class TaxpayerActivity(Base):
    __tablename__ = "taxpayer_activity"
    __table_args__ = (UniqueConstraint("taxpayer_id", "activity_id", name="uq_taxpayer_activity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    taxpayer_id: Mapped[int] = mapped_column(ForeignKey("taxpayer.id", ondelete="CASCADE"), nullable=False)
    activity_id: Mapped[int] = mapped_column(ForeignKey("activity_catalog.id", ondelete="CASCADE"), nullable=False)

    taxpayer: Mapped[Taxpayer] = relationship("Taxpayer", back_populates="activities")
    activity: Mapped[ActivityCatalog] = relationship("ActivityCatalog")


class SyncRun(Base):
    __tablename__ = "sync_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    stage: Mapped[str] = mapped_column(String(40), nullable=False, default="queued")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    total_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_percent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inserted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class StagingDirecciones(Base):
    __tablename__ = "stg_direcciones"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("sync_run.id", ondelete="CASCADE"), index=True, nullable=False)
    rut_clean: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    rut_formatted: Mapped[str] = mapped_column(String(24), nullable=False)
    vigencia: Mapped[str] = mapped_column(String(2), nullable=False, default="")
    tipo_direccion: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    legal_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    dte_email: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    address: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    city: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    parish: Mapped[str] = mapped_column(String(120), nullable=False, default="")


class StagingActecos(Base):
    __tablename__ = "stg_actecos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("sync_run.id", ondelete="CASCADE"), index=True, nullable=False)
    rut_clean: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")


class StagingNombresPJ(Base):
    __tablename__ = "stg_nombres_pj"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("sync_run.id", ondelete="CASCADE"), index=True, nullable=False)
    rut_clean: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    legal_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
