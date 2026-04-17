import logging
from collections import defaultdict
from datetime import datetime

from sqlalchemy import select

from .models import (
    ActivityCatalog,
    StagingActecos,
    StagingDirecciones,
    StagingNombresPJ,
    SyncRun,
    Taxpayer,
    TaxpayerActivity,
)
from .security import clean_rut, format_rut
from .sii_sources import fetch_zip_rows


logger = logging.getLogger(__name__)


def _col_value(row, aliases):
    alias_norm = {a.strip().lower() for a in aliases}
    for key, value in row.items():
        if (key or "").strip().lower() in alias_norm:
            return (value or "").strip()
    return ""


def _extract_rut_clean(row):
    rut = _col_value(row, ["rut", "rut_contribuyente", "rutcntr", "rutcntrb"])
    dv = _col_value(row, ["dv", "digito_verificador", "d.v."])
    if rut and dv and not str(rut).upper().endswith(str(dv).upper()):
        return clean_rut(f"{rut}{dv}")
    return clean_rut(rut)


def _compose_address(row):
    direct = _col_value(row, ["direccion", "domicilio", "direccion_tributaria"])
    if direct:
        return direct
    parts = [
        _col_value(row, ["calle"]),
        _col_value(row, ["numero"]),
        _col_value(row, ["bloque"]),
        _col_value(row, ["departamento"]),
        _col_value(row, ["villa_poblacion"]),
    ]
    return " ".join([p for p in parts if p]).strip()


def _pick_best_address(current, candidate):
    if current is None:
        return candidate
    curr_score = (1 if current.get("vigencia") == "S" else 0) + (
        1 if current.get("tipo_direccion") == "DOMICILIO" else 0
    )
    cand_score = (1 if candidate.get("vigencia") == "S" else 0) + (
        1 if candidate.get("tipo_direccion") == "DOMICILIO" else 0
    )
    return candidate if cand_score > curr_score else current


class SyncService:
    def __init__(self, settings_getter, notifier):
        self.settings_getter = settings_getter
        self.notifier = notifier

    def run_weekly_sync(self, session):
        started = datetime.utcnow()
        run = SyncRun(
            started_at=started,
            status="running",
            stage="downloading",
            message="Starting weekly sync",
            total_rows=0,
            processed_rows=0,
            progress_percent=0,
        )
        session.add(run)
        session.flush()
        session.commit()
        logger.info("sync_run=%s stage=%s message=%s", run.id, run.stage, run.message)

        inserted = 0
        updated = 0

        try:
            cfg = self.settings_getter(session)
            run.stage = "downloading"
            run.message = "Downloading datasets"
            session.commit()

            dir_rows = fetch_zip_rows(cfg["sii_direcciones_url"], timeout=90)
            act_rows = fetch_zip_rows(cfg["sii_actecos_url"], timeout=90)
            name_rows = fetch_zip_rows(cfg.get("sii_base_contribuyentes_url", ""), timeout=90)

            run.stage = "staging"
            run.message = f"Loading raw rows dir={len(dir_rows)} act={len(act_rows)} names={len(name_rows)}"
            session.commit()

            stg_dir = []
            for row in dir_rows:
                rut_clean = _extract_rut_clean(row)
                if len(rut_clean) < 8:
                    continue
                stg_dir.append(
                    {
                        "run_id": run.id,
                        "rut_clean": rut_clean,
                        "rut_formatted": format_rut(rut_clean),
                        "vigencia": (_col_value(row, ["vigencia"]).upper() or ""),
                        "tipo_direccion": (_col_value(row, ["tipo_direccion"]).upper() or ""),
                        "legal_name": _col_value(row, ["razon_social", "razon social", "nombre", "nombre_razon_social"]),
                        "dte_email": _col_value(row, ["dte_email", "correo", "mail", "email"]),
                        "address": _compose_address(row),
                        "city": _col_value(row, ["ciudad"]),
                        "parish": _col_value(row, ["comuna", "parish"]),
                    }
                )

            stg_act = []
            for row in act_rows:
                rut_clean = _extract_rut_clean(row)
                code = _col_value(row, ["acteco", "codigo actividad", "codigo_acteco", "codigo", "cod_acteco"])
                if len(rut_clean) < 8 or not code:
                    continue
                stg_act.append(
                    {
                        "run_id": run.id,
                        "rut_clean": rut_clean,
                        "code": code,
                        "name": _col_value(row, ["glosa", "actividad", "desc. actividad economica", "descripcion", "nombre_actividad"]),
                    }
                )

            stg_names = []
            for row in name_rows:
                rut_clean = _extract_rut_clean(row)
                legal_name = _col_value(row, ["razon_social", "razon social", "nombre", "nombre_razon_social"])
                if len(rut_clean) < 8 or not legal_name:
                    continue
                stg_names.append({"run_id": run.id, "rut_clean": rut_clean, "legal_name": legal_name})

            if stg_dir:
                session.bulk_insert_mappings(StagingDirecciones, stg_dir)
            if stg_act:
                session.bulk_insert_mappings(StagingActecos, stg_act)
            if stg_names:
                session.bulk_insert_mappings(StagingNombresPJ, stg_names)
            session.commit()

            run.stage = "merging"
            run.message = "Consolidating staging into taxpayer tables"

            dir_by_rut = {}
            for row in session.scalars(select(StagingDirecciones).where(StagingDirecciones.run_id == run.id)):
                candidate = {
                    "rut_formatted": row.rut_formatted,
                    "legal_name": row.legal_name,
                    "dte_email": row.dte_email,
                    "address": row.address,
                    "city": row.city,
                    "parish": row.parish,
                    "vigencia": row.vigencia,
                    "tipo_direccion": row.tipo_direccion,
                }
                dir_by_rut[row.rut_clean] = _pick_best_address(dir_by_rut.get(row.rut_clean), candidate)

            names_by_rut = {
                row.rut_clean: row.legal_name
                for row in session.scalars(select(StagingNombresPJ).where(StagingNombresPJ.run_id == run.id))
                if row.legal_name
            }

            act_by_rut = defaultdict(list)
            seen_pairs = set()
            for row in session.scalars(select(StagingActecos).where(StagingActecos.run_id == run.id)):
                key = (row.rut_clean, row.code)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                act_by_rut[row.rut_clean].append({"code": row.code, "name": row.name})

            all_ruts = sorted(set(dir_by_rut.keys()) | set(names_by_rut.keys()) | set(act_by_rut.keys()))
            run.total_rows = len(all_ruts)
            run.processed_rows = 0
            run.progress_percent = 0
            session.commit()

            for idx, rut_clean in enumerate(all_ruts, start=1):
                taxpayer = session.scalar(select(Taxpayer).where(Taxpayer.rut_clean == rut_clean))
                info = dir_by_rut.get(rut_clean, {})
                legal_name = names_by_rut.get(rut_clean) or info.get("legal_name", "")

                if taxpayer is None:
                    taxpayer = Taxpayer(
                        rut_clean=rut_clean,
                        rut_formatted=info.get("rut_formatted") or format_rut(rut_clean),
                        legal_name=legal_name,
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
                        run.processed_rows = idx
                        if run.total_rows:
                            run.progress_percent = int((idx * 100) / run.total_rows)
                        if idx % 500 == 0:
                            run.message = f"Merging taxpayers: {idx}/{run.total_rows}"
                            session.commit()
                        continue
                    taxpayer.rut_formatted = info.get("rut_formatted") or format_rut(rut_clean)
                    taxpayer.legal_name = legal_name or taxpayer.legal_name
                    taxpayer.dte_email = info.get("dte_email", taxpayer.dte_email)
                    taxpayer.address = info.get("address", taxpayer.address)
                    taxpayer.city = info.get("city", taxpayer.city)
                    taxpayer.parish = info.get("parish", taxpayer.parish)
                    taxpayer.source = "sii_weekly"
                    taxpayer.updated_at = datetime.utcnow()
                    updated += 1

                for act_item in act_by_rut.get(rut_clean, []):
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

                run.processed_rows = idx
                if run.total_rows:
                    run.progress_percent = int((idx * 100) / run.total_rows)
                if idx % 500 == 0:
                    run.message = f"Merging taxpayers: {idx}/{run.total_rows}"
                    session.commit()

            run.finished_at = datetime.utcnow()
            run.status = "ok"
            run.stage = "finished"
            run.progress_percent = 100
            run.message = (
                f"Rows dir={len(dir_rows)} act={len(act_rows)} names={len(name_rows)} "
                f"merged={run.total_rows}"
            )
            run.inserted_count = inserted
            run.updated_count = updated
            session.commit()
            logger.info(
                "sync_run=%s status=ok inserted=%s updated=%s total=%s",
                run.id,
                inserted,
                updated,
                run.total_rows,
            )
            return run
        except Exception as exc:
            run.finished_at = datetime.utcnow()
            run.status = "error"
            run.stage = "error"
            run.message = str(exc)
            run.inserted_count = inserted
            run.updated_count = updated
            session.commit()
            self.notifier.send_failure_email(
                "Taxpayer Hub sync failure",
                f"Weekly sync failed at {datetime.utcnow().isoformat()}\n\nError: {exc}",
            )
            logger.exception("sync_run=%s status=error message=%s", run.id, exc)
            raise
