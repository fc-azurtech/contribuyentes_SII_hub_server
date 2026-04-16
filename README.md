# Taxpayer Hub Server

Servicio central para consulta de contribuyentes por RUT, actualizado semanalmente desde fuentes SII, con:

- API para Odoo (`GET /taxpayers/by-rut`)
- Login web de administrador
- Consulta web de contribuyentes
- Forzar actualización manual
- Configuración de fuentes SII desde web
- Configuración SMTP y alertas por fallo de sync
- Persistencia de overrides manuales/PDF (`is_override=true`) sin sobreescritura en sync semanal

## API para Odoo

### `GET /taxpayers/by-rut`
Headers:
- `X-API-Key: <api-key-tenant>`

Query params:
- `rut` o `rut_formatted`

Respuesta 200:
```json
{
  "data": {
    "rut": "76.857.295-K",
    "name": "EMPRESA SPA",
    "razon_social": "EMPRESA SPA",
    "dte_email": "dte@empresa.cl",
    "direccion": "CALLE 123",
    "city": "SANTIAGO",
    "comuna": "PROVIDENCIA",
    "source": "sii_weekly",
    "is_override": false,
    "updated_at": "2026-04-16T22:00:00"
  }
}
```

### `POST /taxpayers/override`
Para registrar override desde PDF/manual (por ejemplo portal Odoo).

## Inicio rápido local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8787
```

## Instalación Debian

Usa:

```bash
sudo bash scripts/install_debian.sh
```

El script:
- instala dependencias del sistema
- crea usuario de servicio
- crea DB y usuario PostgreSQL
- instala servicio systemd `taxpayer-hub`
- levanta el servicio y habilita arranque automático

## Login administrador

Se siembra automáticamente en primer inicio con:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

## Seguridad API

Se siembra un cliente API inicial con:
- `SEED_API_CLIENT_NAME`
- `SEED_API_CLIENT_KEY`

La API valida por hash SHA-256 de la key.
