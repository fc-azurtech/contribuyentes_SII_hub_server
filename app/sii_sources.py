import csv
import io
import zipfile

import requests


def _find_column(columns, aliases):
    alias_norm = {a.strip().lower() for a in aliases}
    for col in columns:
        if (col or "").strip().lower() in alias_norm:
            return col
    return ""


def _parse_text_dataset(text_data: str):
    sample = text_data[:4000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    reader = csv.DictReader(io.StringIO(text_data), delimiter=delimiter)
    return [dict(row) for row in reader]


def fetch_zip_rows(url: str, timeout: int = 60):
    if not url:
        return []

    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()

    rows = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if not (lower.endswith(".csv") or lower.endswith(".txt")):
                continue
            raw = zf.read(name)
            text_data = ""
            for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
                try:
                    text_data = raw.decode(enc)
                    break
                except Exception:
                    continue
            if not text_data:
                continue
            rows.extend(_parse_text_dataset(text_data))
    return rows


def normalize_direcciones_rows(rows):
    result = {}
    for row in rows:
        cols = list(row.keys())
        rut_col = _find_column(cols, ["rut", "rut_contribuyente", "rutcntr", "rutcntrb"])
        name_col = _find_column(cols, ["razon_social", "razon social", "nombre", "nombre_razon_social"])
        addr_col = _find_column(cols, ["direccion", "domicilio", "direccion_tributaria"])
        city_col = _find_column(cols, ["ciudad"])
        parish_col = _find_column(cols, ["comuna", "parish"])
        email_col = _find_column(cols, ["dte_email", "correo", "mail", "email"])

        rut = (row.get(rut_col) or "").strip()
        if not rut:
            continue
        result[rut] = {
            "rut": rut,
            "legal_name": (row.get(name_col) or "").strip(),
            "address": (row.get(addr_col) or "").strip(),
            "city": (row.get(city_col) or "").strip(),
            "parish": (row.get(parish_col) or "").strip(),
            "dte_email": (row.get(email_col) or "").strip(),
        }
    return result


def normalize_actecos_rows(rows):
    result = {}
    for row in rows:
        cols = list(row.keys())
        rut_col = _find_column(cols, ["rut", "rut_contribuyente", "rutcntr", "rutcntrb"])
        code_col = _find_column(cols, ["acteco", "codigo_acteco", "codigo", "cod_acteco"])
        name_col = _find_column(cols, ["glosa", "actividad", "descripcion", "nombre_actividad"])

        rut = (row.get(rut_col) or "").strip()
        code = (row.get(code_col) or "").strip()
        name = (row.get(name_col) or "").strip()
        if not rut or not code:
            continue
        result.setdefault(rut, []).append({"code": code, "name": name})
    return result
