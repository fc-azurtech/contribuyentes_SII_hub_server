import hashlib

from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return pwd_context.verify(password, password_hash)
    except Exception:
        return False


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256((api_key or "").encode("utf-8")).hexdigest()


def clean_rut(value: str) -> str:
    if not value:
        return ""
    txt = str(value).strip().upper()
    if txt.startswith("CL"):
        txt = txt[2:]
    txt = "".join(ch for ch in txt if ch.isdigit() or ch == "K")
    return txt


def format_rut(clean: str) -> str:
    rut = clean_rut(clean)
    if len(rut) < 2:
        return rut
    body = rut[:-1]
    dv = rut[-1]
    parts = []
    while body:
        parts.append(body[-3:])
        body = body[:-3]
    return f"{'.'.join(reversed(parts))}-{dv}"
