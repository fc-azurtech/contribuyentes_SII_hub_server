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

## Sync semanal y progreso

- La actualización usa staging raw (`stg_direcciones`, `stg_actecos`, `stg_nombres_pj`) y luego consolida en tablas de servicio.
- El botón **Forzar actualización ahora** se ejecuta en segundo plano.
- Estado en vivo para UI: `GET /admin/sync/status`.

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

Para desplegar cambios sin romper el entorno virtual ni el `.env`:

```bash
sudo rsync -av --delete --exclude '.venv/' --exclude '.env' --exclude '.git/' /azursoft/contribuyentes_SII_hub_server/ /opt/taxpayer_hub/
sudo chown -R taxpayerhub:taxpayerhub /opt/taxpayer_hub
sudo systemctl restart taxpayer-hub
```

## Desinstalación Debian

Desinstalación básica (mantiene DB/usuario/nginx):

```bash
sudo bash scripts/uninstall_debian.sh
```

Desinstalación completa:

```bash
sudo bash scripts/uninstall_debian.sh --purge-db --purge-user --purge-nginx
```

## Login administrador

Se siembra automáticamente en primer inicio con:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

Notas:
- `bcrypt` queda fijado en versión compatible con `passlib` para evitar fallas de arranque/reset de clave.
- Mantener `ADMIN_PASSWORD` por debajo de 72 caracteres por limitación de bcrypt.

## Seguridad API

Se siembra un cliente API inicial con:
- `SEED_API_CLIENT_NAME`
- `SEED_API_CLIENT_KEY`

La API valida por hash SHA-256 de la key.
