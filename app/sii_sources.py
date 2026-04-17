import csv
import io
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urljoin
import zipfile

import requests


logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


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


def fetch_zip_rows(
    url: str,
    timeout: int = 60,
    retries: int = 2,
    backoff_seconds: float = 2.0,
    dataset_label: str = "dataset",
):
    if not url:
        return []

    max_attempts = max(1, int(retries) + 1)
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
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
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break

            wait_seconds = float(backoff_seconds) * (2 ** (attempt - 1))
            logger.warning(
                "Dataset '%s' download failed at attempt %s/%s: %s. Retrying in %.1fs",
                dataset_label,
                attempt,
                max_attempts,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise last_exc


def _build_cert_tuple(cert_mode: str, cert_path: str, key_path: str, pfx_path: str, pfx_password: str):
    mode = (cert_mode or "").strip().lower()
    if mode == "pem":
        if not cert_path or not key_path:
            raise ValueError("PEM mode requires cert_path and key_path")
        return (cert_path, key_path), None

    if not pfx_path:
        raise ValueError("PFX mode requires pfx_path")

    temp_dir = tempfile.mkdtemp(prefix="sii-pfx-")
    cert_pem = os.path.join(temp_dir, "cert.pem")
    key_pem = os.path.join(temp_dir, "key.pem")
    password_arg = f"pass:{pfx_password or ''}"

    cert_cmd = [
        "openssl",
        "pkcs12",
        "-in",
        pfx_path,
        "-clcerts",
        "-nokeys",
        "-out",
        cert_pem,
        "-passin",
        password_arg,
    ]
    key_cmd = [
        "openssl",
        "pkcs12",
        "-in",
        pfx_path,
        "-nocerts",
        "-nodes",
        "-out",
        key_pem,
        "-passin",
        password_arg,
    ]

    try:
        subprocess.run(cert_cmd, check=True, capture_output=True, text=True)
        subprocess.run(key_cmd, check=True, capture_output=True, text=True)
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Could not extract PFX certificate with openssl: {exc}") from exc

    return (cert_pem, key_pem), temp_dir


def _extract_form_payload(html: str):
    forms = re.findall(r"<form\b[^>]*>.*?</form>", html or "", flags=re.IGNORECASE | re.DOTALL)
    if not forms:
        return "", {}, "", ""

    best_form = forms[0]
    for item in forms:
        if "consulta" in item.lower() or "rut" in item.lower():
            best_form = item
            break

    action_match = re.search(r'action\s*=\s*["\']([^"\']+)["\']', best_form, flags=re.IGNORECASE)
    action = action_match.group(1).strip() if action_match else ""

    hidden_payload = {}
    for input_match in re.finditer(r"<input\b[^>]*>", best_form, flags=re.IGNORECASE):
        input_tag = input_match.group(0)
        name_match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', input_tag, flags=re.IGNORECASE)
        type_match = re.search(r'type\s*=\s*["\']([^"\']+)["\']', input_tag, flags=re.IGNORECASE)
        value_match = re.search(r'value\s*=\s*["\']([^"\']*)["\']', input_tag, flags=re.IGNORECASE)
        if not name_match:
            continue
        name = name_match.group(1).strip()
        typ = (type_match.group(1).strip().lower() if type_match else "text")
        val = value_match.group(1) if value_match else ""
        if typ in {"hidden", "submit"}:
            hidden_payload[name] = val

    text_names = []
    for input_match in re.finditer(r"<input\b[^>]*>", best_form, flags=re.IGNORECASE):
        input_tag = input_match.group(0)
        name_match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', input_tag, flags=re.IGNORECASE)
        type_match = re.search(r'type\s*=\s*["\']([^"\']+)["\']', input_tag, flags=re.IGNORECASE)
        if not name_match:
            continue
        name = name_match.group(1).strip()
        typ = (type_match.group(1).strip().lower() if type_match else "text")
        if typ in {"text", "search", "number", ""}:
            text_names.append(name)

    rut_field = text_names[0] if text_names else ""
    dv_field = text_names[1] if len(text_names) > 1 else ""
    return action, hidden_payload, rut_field, dv_field


def _extract_contact_email(html: str):
    if not html:
        return ""

    row_match = re.search(
        r"Mail\s+de\s+contacto\s*</[^>]+>\s*<[^>]+>([^<]+)</",
        html,
        flags=re.IGNORECASE,
    )
    if row_match:
        candidate = (row_match.group(1) or "").strip()
        if EMAIL_REGEX.fullmatch(candidate):
            return candidate

    all_emails = EMAIL_REGEX.findall(html)
    if not all_emails:
        return ""
    return all_emails[0].strip()


class AuthenticatedSIIEmailClient:
    def __init__(
        self,
        query_url: str,
        cert_mode: str,
        cert_path: str,
        key_path: str,
        pfx_path: str,
        pfx_password: str,
        timeout: int = 30,
        retries: int = 2,
        backoff_seconds: float = 2.0,
    ):
        self.query_url = (query_url or "").strip()
        if not self.query_url:
            raise ValueError("Authenticated SII query URL is required")
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "TaxpayerHub/1.0"})
        self.session.cert, self._temp_dir = _build_cert_tuple(cert_mode, cert_path, key_path, pfx_path, pfx_password)
        self._form_action = ""
        self._hidden_payload = {}
        self._rut_field = ""
        self._dv_field = ""

    def close(self):
        self.session.close()
        if self._temp_dir:
            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def _ensure_form(self):
        if self._rut_field and self._dv_field:
            return

        resp = self.session.get(self.query_url, timeout=self.timeout)
        resp.raise_for_status()
        action, hidden_payload, rut_field, dv_field = _extract_form_payload(resp.text)
        if not rut_field or not dv_field:
            raise RuntimeError("Could not detect RUT/DV fields from SII query form")

        self._form_action = urljoin(self.query_url, action) if action else self.query_url
        self._hidden_payload = hidden_payload
        self._rut_field = rut_field
        self._dv_field = dv_field

    def fetch_email_for_rut(self, rut_clean: str):
        self._ensure_form()
        if len(rut_clean or "") < 8:
            return ""

        body = rut_clean[:-1]
        dv = rut_clean[-1]
        payload = dict(self._hidden_payload)
        payload[self._rut_field] = body
        payload[self._dv_field] = dv

        max_attempts = max(1, int(self.retries) + 1)
        last_exc = None

        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.post(
                    self._form_action,
                    data=payload,
                    timeout=self.timeout,
                    headers={"Referer": self.query_url},
                )
                resp.raise_for_status()
                return _extract_contact_email(resp.text)
            except Exception as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                wait_seconds = float(self.backoff_seconds) * (2 ** (attempt - 1))
                time.sleep(wait_seconds)

        logger.warning("Authenticated SII lookup failed for RUT %s: %s", rut_clean, last_exc)
        return ""


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
