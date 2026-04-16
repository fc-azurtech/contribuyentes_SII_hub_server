from datetime import datetime

from sqlalchemy import select

from .models import ActivityCatalog, SyncRun, Taxpayer, TaxpayerActivity
from .security import clean_rut, format_rut
from .sii_sources import fetch_zip_rows, normalize_actecos_rows, normalize_direcciones_rows


class SyncService:
    def __init__(self, settings_getter, notifier):
        self.settings_getter = settings_getter
        self.notifier = notifier

    def run_weekly_sync(self, session):
        started = datetime.utcnow()
        run = SyncRun(started_at=started, status="running", message="Starting weekly sync")
        session.add(run)
        session.flush()

        inserted = 0
        updated = 0

        try:
            cfg = self.settings_getter(session)
            dir_rows = fetch_zip_rows(cfg["sii_direcciones_url"], timeout=90)
            act_rows = fetch_zip_rows(cfg["sii_actecos_url"], timeout=90)

            dir_data = normalize_direcciones_rows(dir_rows)
            act_data = normalize_actecos_rows(act_rows)

            for raw_rut, info in dir_data.items():
                rut_clean = clean_rut(raw_rut)
                if len(rut_clean) < 8:
                    continue

                taxpayer = session.scalar(select(Taxpayer).where(Taxpayer.rut_clean == rut_clean))
                if taxpayer is None:
                    taxpayer = Taxpayer(
                        rut_clean=rut_clean,
                        rut_formatted=format_rut(rut_clean),
                        legal_name=info.get("legal_name", ""),
                        dte_email=info.get("dte_email", ""),
                        address=info.get("address", ""),
                        city=info.get("city", ""),
                        parish=info.get("parish", ""),
                        source="sii_weekly",
                        is_override=False,
                        updated_at=datetime.utcnow(),
                    )
                    session.add(taxpayer)
                    session.flush()
                    inserted += 1
                else:
                    if taxpayer.is_override:
                        continue
                    taxpayer.rut_formatted = format_rut(rut_clean)
                    taxpayer.legal_name = info.get("legal_name", taxpayer.legal_name)
                    taxpayer.dte_email = info.get("dte_email", taxpayer.dte_email)
                    taxpayer.address = info.get("address", taxpayer.address)
                    taxpayer.city = info.get("city", taxpayer.city)
                    taxpayer.parish = info.get("parish", taxpayer.parish)
                    taxpayer.source = "sii_weekly"
                    taxpayer.updated_at = datetime.utcnow()
                    updated += 1

                for act_item in act_data.get(raw_rut, []):
                    code = (act_item.get("code") or "").strip()
                    if not code:
                        continue
                    catalog = session.scalar(select(ActivityCatalog).where(ActivityCatalog.code == code))
                    if catalog is None:
                        catalog = ActivityCatalog(code=code, name=(act_item.get("name") or "").strip())
                        session.add(catalog)
                        session.flush()
                    elif act_item.get("name"):
                        catalog.name = (act_item.get("name") or "").strip()

                    existing_link = session.scalar(
                        select(TaxpayerActivity).where(
                            TaxpayerActivity.taxpayer_id == taxpayer.id,
                            TaxpayerActivity.activity_id == catalog.id,
                        )
                    )
                    if existing_link is None:
                        session.add(TaxpayerActivity(taxpayer_id=taxpayer.id, activity_id=catalog.id))

            run.finished_at = datetime.utcnow()
            run.status = "ok"
            run.message = f"Rows direcciones={len(dir_rows)} actecos={len(act_rows)}"
            run.inserted_count = inserted
            run.updated_count = updated
            return run
        except Exception as exc:
            run.finished_at = datetime.utcnow()
            run.status = "error"
            run.message = str(exc)
            run.inserted_count = inserted
            run.updated_count = updated
            self.notifier.send_failure_email(
                "Taxpayer Hub sync failure",
                f"Weekly sync failed at {datetime.utcnow().isoformat()}\n\nError: {exc}",
            )
            raise
