#!/usr/bin/env bash
set -euo pipefail

APP_USER="${APP_USER:-taxpayerhub}"
APP_DIR="${APP_DIR:-/opt/taxpayer_hub}"
SERVICE_NAME="${SERVICE_NAME:-taxpayer-hub}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
DB_NAME="${DB_NAME:-taxpayer_hub}"
DB_USER="${DB_USER:-taxpayer_hub}"

PURGE_DB=0
PURGE_USER=0
PURGE_NGINX=0

action_help() {
  cat <<'EOF'
Usage: sudo bash scripts/uninstall_debian.sh [options]

Options:
  --purge-db       Drop PostgreSQL database and role used by the service.
  --purge-user     Remove the OS service user and home directory.
  --purge-nginx    Remove nginx site config (taxpayer-hub) and reload nginx.
  -h, --help       Show this help message.

Examples:
  sudo bash scripts/uninstall_debian.sh
  sudo bash scripts/uninstall_debian.sh --purge-db --purge-user --purge-nginx
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --purge-db)
      PURGE_DB=1
      ;;
    --purge-user)
      PURGE_USER=1
      ;;
    --purge-nginx)
      PURGE_NGINX=1
      ;;
    -h|--help)
      action_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      action_help
      exit 1
      ;;
  esac
  shift
done

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root: sudo bash scripts/uninstall_debian.sh"
  exit 1
fi

echo "[1/6] Stopping and disabling systemd service"
if systemctl list-unit-files | grep -q "^${SERVICE_NAME}\.service"; then
  systemctl stop "$SERVICE_NAME" || true
  systemctl disable "$SERVICE_NAME" || true
else
  echo "Service ${SERVICE_NAME} not found in systemd unit files."
fi

echo "[2/6] Removing systemd unit file"
if [ -f "$SERVICE_FILE" ]; then
  rm -f "$SERVICE_FILE"
fi
systemctl daemon-reload
systemctl reset-failed || true

echo "[3/6] Removing application directory"
if [ -d "$APP_DIR" ]; then
  rm -rf "$APP_DIR"
fi

echo "[4/6] Optional PostgreSQL cleanup"
if [ "$PURGE_DB" = "1" ]; then
  if command -v psql >/dev/null 2>&1; then
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS ${DB_NAME};" || true
    sudo -u postgres psql -c "DROP ROLE IF EXISTS ${DB_USER};" || true
  else
    echo "psql not found; skipped database purge."
  fi
else
  echo "Skipped (use --purge-db to drop DB and role)."
fi

echo "[5/6] Optional service user cleanup"
if [ "$PURGE_USER" = "1" ]; then
  if id "$APP_USER" >/dev/null 2>&1; then
    userdel -r "$APP_USER" 2>/dev/null || userdel "$APP_USER" || true
  else
    echo "User ${APP_USER} does not exist."
  fi
else
  echo "Skipped (use --purge-user to remove OS user)."
fi

echo "[6/6] Optional nginx cleanup"
if [ "$PURGE_NGINX" = "1" ]; then
  rm -f /etc/nginx/sites-enabled/taxpayer-hub
  rm -f /etc/nginx/sites-available/taxpayer-hub
  if command -v nginx >/dev/null 2>&1; then
    nginx -t && systemctl reload nginx || true
  fi
else
  echo "Skipped (use --purge-nginx to remove nginx config)."
fi

echo "Done. Verification commands:"
echo "  systemctl status ${SERVICE_NAME}"
echo "  ss -lntp | grep 8787"
