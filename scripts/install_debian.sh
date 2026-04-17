#!/usr/bin/env bash
set -euo pipefail

APP_USER="taxpayerhub"
APP_GROUP="taxpayerhub"
APP_DIR="/opt/taxpayer_hub"
SERVICE_FILE="/etc/systemd/system/taxpayer-hub.service"
PYTHON_BIN="/usr/bin/python3"
VENV_DIR="$APP_DIR/.venv"
ENV_FILE="$APP_DIR/.env"
INSTALL_NGINX="${INSTALL_NGINX:-0}"
NGINX_SERVER_NAME="${NGINX_SERVER_NAME:-_}"

DB_NAME="${DB_NAME:-taxpayer_hub}"
DB_USER="${DB_USER:-taxpayer_hub}"
DB_PASSWORD="${DB_PASSWORD:-taxpayer_hub_change_me}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root: sudo bash scripts/install_debian.sh"
  exit 1
fi

echo "[1/9] Installing system packages"
apt-get update
apt-get install -y python3 python3-venv python3-pip postgresql postgresql-contrib rsync curl
if [ "$INSTALL_NGINX" = "1" ]; then
  apt-get install -y nginx
fi

echo "[2/9] Creating service user"
if ! id "$APP_USER" >/dev/null 2>&1; then
  useradd --system --create-home --shell /bin/bash "$APP_USER"
fi

echo "[3/9] Deploying application files"
mkdir -p "$APP_DIR"
rsync -a --delete "$PROJECT_ROOT/" "$APP_DIR/"
chown -R "$APP_USER:$APP_GROUP" "$APP_DIR"

echo "[4/9] Preparing PostgreSQL database"
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};"

echo "[5/9] Creating Python virtual environment"
sudo -u "$APP_USER" "$PYTHON_BIN" -m venv "$VENV_DIR"
sudo -u "$APP_USER" "$VENV_DIR/bin/pip" install --upgrade pip
sudo -u "$APP_USER" "$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "[6/9] Creating environment file"
if [ ! -f "$ENV_FILE" ]; then
  cp "$APP_DIR/.env.example" "$ENV_FILE"
fi

sed -i "s|^DATABASE_URL=.*|DATABASE_URL=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@localhost:5432/${DB_NAME}|" "$ENV_FILE"
chown "$APP_USER:$APP_GROUP" "$ENV_FILE"
chmod 600 "$ENV_FILE"

echo "[7/9] Installing systemd service"
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Taxpayer Hub API and Web
After=network.target postgresql.service

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${VENV_DIR}/bin/uvicorn app.main:app --host 0.0.0.0 --port 8787
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable taxpayer-hub
systemctl restart taxpayer-hub

echo "[8/9] Optional Nginx reverse proxy"
if [ "$INSTALL_NGINX" = "1" ]; then
  cat > /etc/nginx/sites-available/taxpayer-hub <<EOF
server {
    listen 80;
    server_name ${NGINX_SERVER_NAME};

    location / {
        proxy_pass http://127.0.0.1:8787;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF
  ln -sf /etc/nginx/sites-available/taxpayer-hub /etc/nginx/sites-enabled/taxpayer-hub
  nginx -t
  systemctl restart nginx
fi

echo "[9/9] Done"
echo "Service status: systemctl status taxpayer-hub"
echo "Logs: journalctl -u taxpayer-hub -f"
echo "Open: http://<server-ip>:8787/login"
echo "IMPORTANT: edit ${ENV_FILE} with secure ADMIN_PASSWORD / SEED_API_CLIENT_KEY and restart service."
