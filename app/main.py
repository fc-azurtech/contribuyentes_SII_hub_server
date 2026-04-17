import logging
import secrets
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session, selectinload
from starlette.middleware.sessions import SessionMiddleware

from .config import settings
from .db import Base, SessionLocal, engine
from .models import AdminUser, ApiClient, SyncRun, SystemSetting, Taxpayer, TaxpayerActivity
from .notifications import NotificationService
from .security import clean_rut, format_rut, hash_api_key, hash_password, verify_password
from .sync_service import SyncService


app = FastAPI(title=settings.app_name)
app.add_middleware(SessionMiddleware, secret_key=settings.app_secret_key)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
templates.env.globals["app_name"] = settings.app_name

scheduler = AsyncIOScheduler()
sync_lock = threading.Lock()
auth_email_lock = threading.Lock()
logger = logging.getLogger(__name__)

SYNC_WEEKDAY_VALUES = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
SYNC_MONTH_VALUES = {str(i) for i in range(1, 13)}


DEFAULT_SETTING_KEYS = {
    "sii_direcciones_url": settings.sii_direcciones_url,
    "sii_actecos_url": settings.sii_actecos_url,
    "sii_base_contribuyentes_url": settings.sii_base_contribuyentes_url,
    "sync_download_timeout": str(settings.sync_download_timeout),
    "sync_download_retries": str(settings.sync_download_retries),
    "sync_download_backoff_seconds": str(settings.sync_download_backoff_seconds),
    "sync_frequency": settings.sync_frequency,
    "sync_weekdays": settings.sync_weekdays,
    "sync_months": settings.sync_months,
    "sync_day_of_month": str(settings.sync_day_of_month),
    "sync_yearly_month": str(settings.sync_yearly_month),
    "sync_hour": str(settings.sync_hour),
    "sync_minute": str(settings.sync_minute),
    "sync_timezone": settings.sync_timezone,
    "sii_auth_enabled": "true" if settings.sii_auth_enabled else "false",
    "sii_auth_cert_mode": settings.sii_auth_cert_mode,
    "sii_auth_pfx_path": settings.sii_auth_pfx_path,
    "sii_auth_pfx_password": settings.sii_auth_pfx_password,
    "sii_auth_cert_path": settings.sii_auth_cert_path,
    "sii_auth_key_path": settings.sii_auth_key_path,
    "sii_auth_query_url": settings.sii_auth_query_url,
    "sii_auth_timeout": str(settings.sii_auth_timeout),
    "sii_auth_retries": str(settings.sii_auth_retries),
    "sii_auth_backoff_seconds": str(settings.sii_auth_backoff_seconds),
    "sii_auth_delay_ms": str(settings.sii_auth_delay_ms),
    "sii_auth_batch_size": str(settings.sii_auth_batch_size),
    "smtp_host": settings.smtp_host,
    "smtp_port": str(settings.smtp_port),
    "smtp_username": settings.smtp_username,
    "smtp_password": settings.smtp_password,
    "smtp_tls": "true" if settings.smtp_tls else "false",
    "smtp_from": settings.smtp_from,
    "alert_email_to": settings.alert_email_to,
}


def _to_int(value, default, min_value, max_value):
    try:
        parsed = int(str(value).strip())
    except Exception:
        parsed = default
    return max(min_value, min(max_value, parsed))


def _sanitize_weekdays_csv(raw):
    values = []
    for item in (raw or "").split(","):
        token = item.strip().lower()
        if token in SYNC_WEEKDAY_VALUES and token not in values:
            values.append(token)
    return ",".join(values)


def _sanitize_months_csv(raw):
    values = []
    for item in (raw or "").split(","):
        token = item.strip()
        if token in SYNC_MONTH_VALUES and token not in values:
            values.append(token)
    return ",".join(values)


def _build_sync_cron_kwargs(cfg):
    frequency = (cfg.get("sync_frequency") or "weekly").strip().lower()
    if frequency not in {"daily", "weekly", "monthly", "yearly"}:
        frequency = "weekly"

    timezone_name = (cfg.get("sync_timezone") or "America/Santiago").strip() or "America/Santiago"
    try:
        timezone = ZoneInfo(timezone_name)
    except Exception:
        timezone = ZoneInfo("America/Santiago")

    hour = _to_int(cfg.get("sync_hour"), 3, 0, 23)
    minute = _to_int(cfg.get("sync_minute"), 30, 0, 59)
    day_of_month = _to_int(cfg.get("sync_day_of_month"), 1, 1, 31)
    yearly_month = _to_int(cfg.get("sync_yearly_month"), 1, 1, 12)
    weekdays = _sanitize_weekdays_csv(cfg.get("sync_weekdays")) or "sun"
    months = _sanitize_months_csv(cfg.get("sync_months"))

    cron_kwargs = {
        "hour": hour,
        "minute": minute,
        "timezone": timezone,
    }
    if frequency == "weekly":
        cron_kwargs["day_of_week"] = weekdays
    elif frequency == "monthly":
        cron_kwargs["day"] = str(day_of_month)
        if months:
            cron_kwargs["month"] = months
    elif frequency == "yearly":
        cron_kwargs["day"] = str(day_of_month)
        cron_kwargs["month"] = str(yearly_month)
    return cron_kwargs


def _apply_sync_schedule(cfg):
    cron_kwargs = _build_sync_cron_kwargs(cfg)
    scheduler.add_job(
        _run_scheduled_sync,
        "cron",
        id="scheduled_sync",
        replace_existing=True,
        **cron_kwargs,
    )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_setting(session: Session, key: str, default: str = "") -> str:
    row = session.get(SystemSetting, key)
    if row:
        return row.value
    return default


def set_setting(session: Session, key: str, value: str):
    row = session.get(SystemSetting, key)
    if row is None:
        row = SystemSetting(key=key, value=value or "", updated_at=datetime.utcnow())
        session.add(row)
    else:
        row.value = value or ""
        row.updated_at = datetime.utcnow()


def load_runtime_config(session: Session):
    cfg = {}
    for key, default in DEFAULT_SETTING_KEYS.items():
        cfg[key] = get_setting(session, key, default)
    return cfg


def get_notifier(session: Session):
    cfg = load_runtime_config(session)
    return NotificationService(
        smtp_host=cfg["smtp_host"],
        smtp_port=int(cfg["smtp_port"] or 587),
        smtp_user=cfg["smtp_username"],
        smtp_password=cfg["smtp_password"],
        smtp_tls=(cfg["smtp_tls"].lower() == "true"),
        sender=cfg["smtp_from"],
        target=cfg["alert_email_to"],
    )


def get_sync_service(session: Session):
    notifier = get_notifier(session)
    return SyncService(settings_getter=load_runtime_config, notifier=notifier)


def require_admin(request: Request):
    if not request.session.get("admin_user_id"):
        raise HTTPException(status_code=401, detail="Not authenticated")


def require_api_client(db: Session, api_key: str):
    key_hash = hash_api_key(api_key)
    row = db.scalar(select(ApiClient).where(ApiClient.key_hash == key_hash, ApiClient.is_active.is_(True)))
    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key")


def seed_defaults(session: Session):
    if session.scalar(select(AdminUser).where(AdminUser.username == settings.admin_username)) is None:
        session.add(
            AdminUser(
                username=settings.admin_username,
                password_hash=hash_password(settings.admin_password),
                is_active=True,
            )
        )

    if session.scalar(select(ApiClient).where(ApiClient.name == settings.seed_api_client_name)) is None:
        session.add(
            ApiClient(
                name=settings.seed_api_client_name,
                key_hash=hash_api_key(settings.seed_api_client_key),
                is_active=True,
            )
        )

    for key, default in DEFAULT_SETTING_KEYS.items():
        if session.get(SystemSetting, key) is None:
            session.add(SystemSetting(key=key, value=default or "", updated_at=datetime.utcnow()))


def ensure_schema_compatibility():
    statements = [
        "ALTER TABLE sync_run ADD COLUMN IF NOT EXISTS stage VARCHAR(40) NOT NULL DEFAULT 'queued'",
        "ALTER TABLE sync_run ADD COLUMN IF NOT EXISTS total_rows INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE sync_run ADD COLUMN IF NOT EXISTS processed_rows INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE sync_run ADD COLUMN IF NOT EXISTS progress_percent INTEGER NOT NULL DEFAULT 0",
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def mark_interrupted_runs(session: Session):
    running_runs = session.scalars(select(SyncRun).where(SyncRun.status == "running")).all()
    if not running_runs:
        return
    now = datetime.utcnow()
    for run in running_runs:
        run.status = "error"
        run.stage = "error"
        run.finished_at = now
        run.message = "Interrupted by service restart"


@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)
    ensure_schema_compatibility()
    with SessionLocal() as session:
        mark_interrupted_runs(session)
        seed_defaults(session)
        session.commit()

    with SessionLocal() as session:
        cfg = load_runtime_config(session)
    _apply_sync_schedule(cfg)
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)


def _run_scheduled_sync():
    if not sync_lock.acquire(blocking=False):
        logger.info("Scheduled sync skipped: another sync is running")
        return
    with SessionLocal() as session:
        try:
            sync_service = get_sync_service(session)
            sync_service.run_weekly_sync(session)
        finally:
            sync_lock.release()


@app.get("/health")
def health(db: Session = Depends(get_db)):
    total = db.scalar(select(func.count()).select_from(Taxpayer))
    return {"status": "ok", "taxpayers": total}


def _serialize_actecos_list(row: Taxpayer):
    items = []
    for link in row.activities:
        if not link.activity:
            continue
        code = (link.activity.code or "").strip()
        name = (link.activity.name or "").strip()
        if not (code or name):
            continue
        items.append({"code": code, "name": name})
    return items


def _build_taxpayer_payload(taxpayer: Taxpayer):
    actecos = _serialize_actecos_list(taxpayer)
    actecos_text = " | ".join(
        [
            f"{item['code']} - {item['name']}" if item.get("code") and item.get("name")
            else (item.get("code") or item.get("name") or "")
            for item in actecos
        ]
    )
    return {
        "rut": taxpayer.rut_formatted,
        "razon_social": taxpayer.legal_name,
        "name": taxpayer.legal_name,
        "actecos": actecos,
        "actecos_text": actecos_text,
        "dte_email": taxpayer.dte_email,
        "direccion": taxpayer.address,
        "city": taxpayer.city,
        "comuna": taxpayer.parish,
        "source": taxpayer.source,
        "is_override": taxpayer.is_override,
        "updated_at": taxpayer.updated_at.isoformat() if taxpayer.updated_at else "",
    }


@app.get("/taxpayers/by-rut")
def api_taxpayer_by_rut(
    rut: str = "",
    rut_formatted: str = "",
    x_api_key: str = Header(default="", alias="X-API-Key"),
    db: Session = Depends(get_db),
):
    require_api_client(db, x_api_key)
    lookup = rut or rut_formatted
    rut_clean = clean_rut(lookup)
    if len(rut_clean) < 8:
        raise HTTPException(status_code=400, detail="Invalid RUT")

    taxpayer = db.scalar(
        select(Taxpayer)
        .options(selectinload(Taxpayer.activities).selectinload(TaxpayerActivity.activity))
        .where(Taxpayer.rut_clean == rut_clean)
    )
    if not taxpayer:
        raise HTTPException(status_code=404, detail="Taxpayer not found")

    return {"data": _build_taxpayer_payload(taxpayer)}


@app.get("/taxpayers/search")
def api_taxpayer_search(
    q: str = "",
    limit: int = 30,
    x_api_key: str = Header(default="", alias="X-API-Key"),
    db: Session = Depends(get_db),
):
    require_api_client(db, x_api_key)
    q = (q or "").strip()
    if len(q) < 2:
        return {"rows": []}

    limit = max(1, min(int(limit or 30), 100))
    like = f"%{q.lower()}%"
    rows = db.scalars(
        select(Taxpayer)
        .options(selectinload(Taxpayer.activities).selectinload(TaxpayerActivity.activity))
        .where(
            func.lower(Taxpayer.rut_formatted).like(like)
            | func.lower(Taxpayer.rut_clean).like(like)
            | func.lower(Taxpayer.legal_name).like(like)
        )
        .order_by(desc(Taxpayer.updated_at))
        .limit(limit)
    ).all()

    return {"rows": [_build_taxpayer_payload(row) for row in rows]}


@app.post("/taxpayers/override")
def api_taxpayer_override(
    rut: str = Form(...),
    legal_name: str = Form(...),
    dte_email: str = Form(default=""),
    address: str = Form(default=""),
    city: str = Form(default=""),
    parish: str = Form(default=""),
    x_api_key: str = Header(default="", alias="X-API-Key"),
    db: Session = Depends(get_db),
):
    require_api_client(db, x_api_key)
    rut_clean = clean_rut(rut)
    if len(rut_clean) < 8:
        raise HTTPException(status_code=400, detail="Invalid RUT")

    taxpayer = db.scalar(select(Taxpayer).where(Taxpayer.rut_clean == rut_clean))
    if taxpayer is None:
        taxpayer = Taxpayer(
            rut_clean=rut_clean,
            rut_formatted=format_rut(rut_clean),
            legal_name=legal_name,
            dte_email=dte_email,
            address=address,
            city=city,
            parish=parish,
            source="portal_pdf",
            is_override=True,
            updated_at=datetime.utcnow(),
        )
        db.add(taxpayer)
    else:
        taxpayer.rut_formatted = format_rut(rut_clean)
        taxpayer.legal_name = legal_name
        taxpayer.dte_email = dte_email
        taxpayer.address = address
        taxpayer.city = city
        taxpayer.parish = parish
        taxpayer.source = "portal_pdf"
        taxpayer.is_override = True
        taxpayer.updated_at = datetime.utcnow()

    db.commit()
    return {"status": "ok"}


@app.get("/")
def root(request: Request):
    if request.session.get("admin_user_id"):
        return RedirectResponse(url="/admin", status_code=303)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login")
def login_form(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login")
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.scalar(select(AdminUser).where(AdminUser.username == username, AdminUser.is_active.is_(True)))
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Credenciales inválidas"}, status_code=400
        )

    request.session["admin_user_id"] = user.id
    return RedirectResponse(url="/admin", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/admin")
def admin_dashboard(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    total = db.scalar(select(func.count()).select_from(Taxpayer))
    overrides = db.scalar(select(func.count()).select_from(Taxpayer).where(Taxpayer.is_override.is_(True)))
    last_runs = db.scalars(select(SyncRun).order_by(desc(SyncRun.started_at)).limit(10)).all()
    active_run = db.scalar(select(SyncRun).where(SyncRun.status == "running").order_by(desc(SyncRun.started_at)))
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "total": total,
            "overrides": overrides,
            "last_runs": last_runs,
            "active_run": active_run,
        },
    )


def _run_forced_sync_in_background():
    with SessionLocal() as session:
        try:
            sync_service = get_sync_service(session)
            sync_service.run_weekly_sync(session)
        except Exception:
            logger.exception("Forced sync finished with error")
        finally:
            sync_lock.release()


def _run_auth_enrichment_in_background():
    with SessionLocal() as session:
        try:
            sync_service = get_sync_service(session)
            sync_service.run_authenticated_email_enrichment(session)
        except Exception:
            logger.exception("Authenticated email enrichment finished with error")
        finally:
            auth_email_lock.release()


@app.post("/admin/sync/force")
def admin_force_sync(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    running = db.scalar(select(SyncRun).where(SyncRun.status == "running").order_by(desc(SyncRun.started_at)))
    if running:
        return RedirectResponse(url="/admin", status_code=303)

    if not sync_lock.acquire(blocking=False):
        return RedirectResponse(url="/admin", status_code=303)

    worker = threading.Thread(target=_run_forced_sync_in_background, daemon=True)
    worker.start()
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/sync/enrich-dte-emails")
def admin_enrich_dte_emails(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    running = db.scalar(select(SyncRun).where(SyncRun.status == "running").order_by(desc(SyncRun.started_at)))
    if running:
        return RedirectResponse(url="/admin", status_code=303)

    if not auth_email_lock.acquire(blocking=False):
        return RedirectResponse(url="/admin", status_code=303)

    worker = threading.Thread(target=_run_auth_enrichment_in_background, daemon=True)
    worker.start()
    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/sync/status")
def admin_sync_status(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    run = db.scalar(select(SyncRun).order_by(desc(SyncRun.started_at)).limit(1))
    if not run:
        payload = {
            "running": False,
            "run": None,
        }
        return JSONResponse(
            content=payload,
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache"},
        )

    payload = {
        "running": run.status == "running",
        "run": {
            "id": run.id,
            "stage": run.stage,
            "status": run.status,
            "message": run.message,
            "total_rows": run.total_rows,
            "processed_rows": run.processed_rows,
            "progress_percent": run.progress_percent,
            "inserted_count": run.inserted_count,
            "updated_count": run.updated_count,
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "finished_at": run.finished_at.isoformat() if run.finished_at else "",
            "elapsed_seconds": int((datetime.utcnow() - run.started_at).total_seconds()) if run.started_at else 0,
        },
    }
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache"},
    )


def _render_api_clients(request: Request, db: Session, created_key: str = "", created_name: str = ""):
    clients = db.scalars(select(ApiClient).order_by(ApiClient.name)).all()
    return templates.TemplateResponse(
        "api_clients.html",
        {
            "request": request,
            "clients": clients,
            "created_key": created_key,
            "created_name": created_name,
        },
    )


@app.get("/admin/settings/api-clients")
def settings_api_clients(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    return _render_api_clients(request, db)


@app.post("/admin/settings/api-clients/create")
def settings_api_clients_create(
    request: Request,
    name: str = Form(...),
    api_key: str = Form(default=""),
    db: Session = Depends(get_db),
):
    require_admin(request)
    name = (name or "").strip()
    if not name:
        return _render_api_clients(request, db)

    plain = (api_key or "").strip() or secrets.token_urlsafe(32)
    row = db.scalar(select(ApiClient).where(ApiClient.name == name))
    if row is None:
        row = ApiClient(name=name, key_hash=hash_api_key(plain), is_active=True)
        db.add(row)
    else:
        row.key_hash = hash_api_key(plain)
        row.is_active = True
    db.commit()
    return _render_api_clients(request, db, created_key=plain, created_name=name)


@app.post("/admin/settings/api-clients/rotate/{client_id}")
def settings_api_clients_rotate(request: Request, client_id: int, db: Session = Depends(get_db)):
    require_admin(request)
    row = db.get(ApiClient, client_id)
    if row is None:
        return RedirectResponse(url="/admin/settings/api-clients", status_code=303)

    plain = secrets.token_urlsafe(32)
    row.key_hash = hash_api_key(plain)
    row.is_active = True
    db.commit()
    return _render_api_clients(request, db, created_key=plain, created_name=row.name)


@app.post("/admin/settings/api-clients/toggle/{client_id}")
def settings_api_clients_toggle(request: Request, client_id: int, db: Session = Depends(get_db)):
    require_admin(request)
    row = db.get(ApiClient, client_id)
    if row is not None:
        row.is_active = not row.is_active
        db.commit()
    return RedirectResponse(url="/admin/settings/api-clients", status_code=303)


def _render_admin_users(request: Request, db: Session, message: str = "", error: str = ""):
    users = db.scalars(select(AdminUser).order_by(AdminUser.username)).all()
    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "users": users,
            "message": message,
            "error": error,
            "current_admin_user_id": request.session.get("admin_user_id"),
        },
    )


@app.get("/admin/settings/users")
def settings_admin_users(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    return _render_admin_users(request, db)


@app.post("/admin/settings/users/create")
def settings_admin_users_create(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    require_admin(request)
    username = (username or "").strip()
    password = (password or "").strip()
    if not username or not password:
        return _render_admin_users(request, db, error="Usuario y clave son obligatorios.")

    existing = db.scalar(select(AdminUser).where(AdminUser.username == username))
    if existing is not None:
        return _render_admin_users(request, db, error="El usuario ya existe.")

    db.add(AdminUser(username=username, password_hash=hash_password(password), is_active=True))
    db.commit()
    return _render_admin_users(request, db, message="Usuario creado correctamente.")


@app.post("/admin/settings/users/toggle/{user_id}")
def settings_admin_users_toggle(request: Request, user_id: int, db: Session = Depends(get_db)):
    require_admin(request)
    row = db.get(AdminUser, user_id)
    if row is None:
        return RedirectResponse(url="/admin/settings/users", status_code=303)

    current_admin_id = request.session.get("admin_user_id")
    if current_admin_id == row.id and row.is_active:
        return _render_admin_users(request, db, error="No puedes desactivar tu propio usuario en sesión.")

    row.is_active = not row.is_active
    db.commit()
    return RedirectResponse(url="/admin/settings/users", status_code=303)


@app.post("/admin/settings/users/reset-password/{user_id}")
def settings_admin_users_reset_password(
    request: Request,
    user_id: int,
    new_password: str = Form(...),
    db: Session = Depends(get_db),
):
    require_admin(request)
    row = db.get(AdminUser, user_id)
    if row is None:
        return RedirectResponse(url="/admin/settings/users", status_code=303)

    new_password = (new_password or "").strip()
    if not new_password:
        return _render_admin_users(request, db, error="La nueva clave no puede estar vacía.")

    row.password_hash = hash_password(new_password)
    db.commit()
    return _render_admin_users(request, db, message=f"Clave actualizada para {row.username}.")


def _get_taxpayer_rows(db: Session, q: str = ""):
    query = (
        select(Taxpayer)
        .options(selectinload(Taxpayer.activities).selectinload(TaxpayerActivity.activity))
        .order_by(desc(Taxpayer.updated_at))
        .limit(200)
    )
    if q:
        key = f"%{q.lower()}%"
        query = (
            select(Taxpayer)
            .options(selectinload(Taxpayer.activities).selectinload(TaxpayerActivity.activity))
            .where(
                func.lower(Taxpayer.rut_formatted).like(key)
                | func.lower(Taxpayer.legal_name).like(key)
                | func.lower(Taxpayer.rut_clean).like(key)
            )
            .order_by(desc(Taxpayer.updated_at))
            .limit(200)
        )
    return db.scalars(query).all()


def _serialize_actecos(row: Taxpayer) -> str:
    labels = []
    for link in row.activities:
        if not link.activity:
            continue
        code = (link.activity.code or "").strip()
        name = (link.activity.name or "").strip()
        if code and name:
            labels.append(f"{code} - {name}")
        elif code:
            labels.append(code)
        elif name:
            labels.append(name)
    return " | ".join(labels)


@app.get("/admin/taxpayers")
def admin_taxpayers(request: Request, q: str = "", db: Session = Depends(get_db)):
    require_admin(request)
    rows = _get_taxpayer_rows(db, q)
    return templates.TemplateResponse("taxpayers.html", {"request": request, "rows": rows, "q": q})


@app.get("/admin/taxpayers/search")
def admin_taxpayers_search(request: Request, q: str = "", db: Session = Depends(get_db)):
    require_admin(request)
    rows = _get_taxpayer_rows(db, q)
    payload = []
    for row in rows:
        payload.append(
            {
                "rut_formatted": row.rut_formatted,
                "legal_name": row.legal_name,
                "actecos": _serialize_actecos(row),
                "dte_email": row.dte_email,
                "address": row.address,
                "parish": row.parish,
                "city": row.city,
                "source": row.source,
                "is_override": row.is_override,
                "updated_at": row.updated_at.isoformat(sep=" ", timespec="seconds") if row.updated_at else "",
            }
        )
    return JSONResponse(content={"rows": payload})


@app.get("/admin/settings/sources")
def settings_sources_form(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    cfg = load_runtime_config(db)
    return templates.TemplateResponse(
        "sources.html",
        {
            "request": request,
            "cfg": cfg,
            "weekday_options": [
                ("mon", "Lunes"),
                ("tue", "Martes"),
                ("wed", "Miércoles"),
                ("thu", "Jueves"),
                ("fri", "Viernes"),
                ("sat", "Sábado"),
                ("sun", "Domingo"),
            ],
            "month_options": [
                ("1", "Enero"),
                ("2", "Febrero"),
                ("3", "Marzo"),
                ("4", "Abril"),
                ("5", "Mayo"),
                ("6", "Junio"),
                ("7", "Julio"),
                ("8", "Agosto"),
                ("9", "Septiembre"),
                ("10", "Octubre"),
                ("11", "Noviembre"),
                ("12", "Diciembre"),
            ],
        },
    )


@app.post("/admin/settings/sources")
def settings_sources_save(
    request: Request,
    sii_direcciones_url: str = Form(...),
    sii_actecos_url: str = Form(...),
    sii_base_contribuyentes_url: str = Form(default=""),
    sync_download_timeout: str = Form(default="180"),
    sync_download_retries: str = Form(default="3"),
    sync_download_backoff_seconds: str = Form(default="3"),
    sync_frequency: str = Form(default="weekly"),
    sync_weekdays: list[str] = Form(default=[]),
    sync_months: list[str] = Form(default=[]),
    sync_day_of_month: str = Form(default="1"),
    sync_yearly_month: str = Form(default="1"),
    sync_yearly_day_of_month: str = Form(default="1"),
    sync_hour: str = Form(default="3"),
    sync_minute: str = Form(default="30"),
    sii_auth_enabled: str = Form(default="0"),
    sii_auth_cert_mode: str = Form(default="pfx"),
    sii_auth_pfx_path: str = Form(default=""),
    sii_auth_pfx_password: str = Form(default=""),
    sii_auth_cert_path: str = Form(default=""),
    sii_auth_key_path: str = Form(default=""),
    sii_auth_query_url: str = Form(default=""),
    sii_auth_timeout: str = Form(default="30"),
    sii_auth_retries: str = Form(default="2"),
    sii_auth_backoff_seconds: str = Form(default="2"),
    sii_auth_delay_ms: str = Form(default="250"),
    sii_auth_batch_size: str = Form(default="250"),
    db: Session = Depends(get_db),
):
    require_admin(request)
    set_setting(db, "sii_direcciones_url", sii_direcciones_url)
    set_setting(db, "sii_actecos_url", sii_actecos_url)
    set_setting(db, "sii_base_contribuyentes_url", sii_base_contribuyentes_url)
    set_setting(db, "sync_download_timeout", sync_download_timeout)
    set_setting(db, "sync_download_retries", sync_download_retries)
    set_setting(db, "sync_download_backoff_seconds", sync_download_backoff_seconds)
    normalized_frequency = (sync_frequency or "weekly").strip().lower()
    if normalized_frequency not in {"daily", "weekly", "monthly", "yearly"}:
        normalized_frequency = "weekly"
    set_setting(db, "sync_frequency", normalized_frequency)
    set_setting(db, "sync_weekdays", _sanitize_weekdays_csv(",".join(sync_weekdays or [])) or "sun")
    set_setting(db, "sync_months", _sanitize_months_csv(",".join(sync_months or [])))
    day_of_month_value = sync_day_of_month
    if normalized_frequency == "yearly":
        day_of_month_value = sync_yearly_day_of_month
    set_setting(db, "sync_day_of_month", str(_to_int(day_of_month_value, 1, 1, 31)))
    set_setting(db, "sync_yearly_month", str(_to_int(sync_yearly_month, 1, 1, 12)))
    set_setting(db, "sync_hour", str(_to_int(sync_hour, 3, 0, 23)))
    set_setting(db, "sync_minute", str(_to_int(sync_minute, 30, 0, 59)))
    set_setting(db, "sync_timezone", "America/Santiago")
    set_setting(db, "sii_auth_enabled", "true" if sii_auth_enabled in {"1", "true", "yes", "on"} else "false")
    set_setting(db, "sii_auth_cert_mode", sii_auth_cert_mode)
    set_setting(db, "sii_auth_pfx_path", sii_auth_pfx_path)
    if (sii_auth_pfx_password or "").strip():
        set_setting(db, "sii_auth_pfx_password", sii_auth_pfx_password)
    set_setting(db, "sii_auth_cert_path", sii_auth_cert_path)
    set_setting(db, "sii_auth_key_path", sii_auth_key_path)
    set_setting(db, "sii_auth_query_url", sii_auth_query_url)
    set_setting(db, "sii_auth_timeout", sii_auth_timeout)
    set_setting(db, "sii_auth_retries", sii_auth_retries)
    set_setting(db, "sii_auth_backoff_seconds", sii_auth_backoff_seconds)
    set_setting(db, "sii_auth_delay_ms", sii_auth_delay_ms)
    set_setting(db, "sii_auth_batch_size", sii_auth_batch_size)
    db.commit()
    cfg = load_runtime_config(db)
    _apply_sync_schedule(cfg)
    return RedirectResponse(url="/admin/settings/sources", status_code=303)


@app.get("/admin/settings/smtp")
def settings_smtp_form(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    cfg = load_runtime_config(db)
    return templates.TemplateResponse("smtp.html", {"request": request, "cfg": cfg, "saved": False})


@app.post("/admin/settings/smtp")
def settings_smtp_save(
    request: Request,
    smtp_host: str = Form(default=""),
    smtp_port: str = Form(default="587"),
    smtp_username: str = Form(default=""),
    smtp_password: str = Form(default=""),
    smtp_tls: str = Form(default="true"),
    smtp_from: str = Form(default=""),
    alert_email_to: str = Form(default=""),
    send_test: str = Form(default="0"),
    db: Session = Depends(get_db),
):
    require_admin(request)
    set_setting(db, "smtp_host", smtp_host)
    set_setting(db, "smtp_port", smtp_port)
    set_setting(db, "smtp_username", smtp_username)
    set_setting(db, "smtp_password", smtp_password)
    set_setting(db, "smtp_tls", "true" if smtp_tls.lower() in {"1", "true", "yes", "on"} else "false")
    set_setting(db, "smtp_from", smtp_from)
    set_setting(db, "alert_email_to", alert_email_to)
    db.commit()

    cfg = load_runtime_config(db)
    if send_test == "1":
        notifier = get_notifier(db)
        notifier.send_failure_email("Taxpayer Hub SMTP test", "SMTP test from Taxpayer Hub")

    return templates.TemplateResponse("smtp.html", {"request": request, "cfg": cfg, "saved": True})
