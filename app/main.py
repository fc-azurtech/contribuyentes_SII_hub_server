import logging
import secrets
import threading
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from .config import settings
from .db import Base, SessionLocal, engine
from .models import AdminUser, ApiClient, SyncRun, SystemSetting, Taxpayer
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
logger = logging.getLogger(__name__)


DEFAULT_SETTING_KEYS = {
    "sii_direcciones_url": settings.sii_direcciones_url,
    "sii_actecos_url": settings.sii_actecos_url,
    "sii_base_contribuyentes_url": settings.sii_base_contribuyentes_url,
    "sync_download_timeout": str(settings.sync_download_timeout),
    "sync_download_retries": str(settings.sync_download_retries),
    "sync_download_backoff_seconds": str(settings.sync_download_backoff_seconds),
    "smtp_host": settings.smtp_host,
    "smtp_port": str(settings.smtp_port),
    "smtp_username": settings.smtp_username,
    "smtp_password": settings.smtp_password,
    "smtp_tls": "true" if settings.smtp_tls else "false",
    "smtp_from": settings.smtp_from,
    "alert_email_to": settings.alert_email_to,
}


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

    scheduler.add_job(
        _run_scheduled_sync,
        "cron",
        day_of_week=settings.sync_weekday,
        hour=settings.sync_hour,
        minute=settings.sync_minute,
        id="weekly_sync",
        replace_existing=True,
    )
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

    taxpayer = db.scalar(select(Taxpayer).where(Taxpayer.rut_clean == rut_clean))
    if not taxpayer:
        raise HTTPException(status_code=404, detail="Taxpayer not found")

    return {
        "data": {
            "rut": taxpayer.rut_formatted,
            "razon_social": taxpayer.legal_name,
            "name": taxpayer.legal_name,
            "dte_email": taxpayer.dte_email,
            "direccion": taxpayer.address,
            "city": taxpayer.city,
            "comuna": taxpayer.parish,
            "source": taxpayer.source,
            "is_override": taxpayer.is_override,
            "updated_at": taxpayer.updated_at.isoformat() if taxpayer.updated_at else "",
        }
    }


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


@app.get("/admin/taxpayers")
def admin_taxpayers(request: Request, q: str = "", db: Session = Depends(get_db)):
    require_admin(request)
    query = select(Taxpayer).order_by(desc(Taxpayer.updated_at)).limit(200)
    if q:
        key = f"%{q.lower()}%"
        query = (
            select(Taxpayer)
            .where(
                func.lower(Taxpayer.rut_formatted).like(key)
                | func.lower(Taxpayer.legal_name).like(key)
                | func.lower(Taxpayer.rut_clean).like(key)
            )
            .order_by(desc(Taxpayer.updated_at))
            .limit(200)
        )
    rows = db.scalars(query).all()
    return templates.TemplateResponse("taxpayers.html", {"request": request, "rows": rows, "q": q})


@app.get("/admin/settings/sources")
def settings_sources_form(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    cfg = load_runtime_config(db)
    return templates.TemplateResponse("sources.html", {"request": request, "cfg": cfg})


@app.post("/admin/settings/sources")
def settings_sources_save(
    request: Request,
    sii_direcciones_url: str = Form(...),
    sii_actecos_url: str = Form(...),
    sii_base_contribuyentes_url: str = Form(default=""),
    sync_download_timeout: str = Form(default="180"),
    sync_download_retries: str = Form(default="3"),
    sync_download_backoff_seconds: str = Form(default="3"),
    db: Session = Depends(get_db),
):
    require_admin(request)
    set_setting(db, "sii_direcciones_url", sii_direcciones_url)
    set_setting(db, "sii_actecos_url", sii_actecos_url)
    set_setting(db, "sii_base_contribuyentes_url", sii_base_contribuyentes_url)
    set_setting(db, "sync_download_timeout", sync_download_timeout)
    set_setting(db, "sync_download_retries", sync_download_retries)
    set_setting(db, "sync_download_backoff_seconds", sync_download_backoff_seconds)
    db.commit()
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
