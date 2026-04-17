#!/usr/bin/env bash
set -euo pipefail

APP_USER="${APP_USER:-taxpayerhub}"
APP_GROUP="${APP_GROUP:-taxpayerhub}"
SERVICE_NAME="${SERVICE_NAME:-taxpayer-hub}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"

SOURCE_DIR="${SOURCE_DIR:-/azursoft/contribuyentes_SII_hub_server}"
TARGET_DIR="${TARGET_DIR:-/opt/taxpayer_hub}"
VENV_DIR="${VENV_DIR:-$TARGET_DIR/.venv}"
ENV_FILE="${ENV_FILE:-$TARGET_DIR/.env}"

RUN_APT=0
SKIP_PIP=0

show_help() {
  cat <<'EOF'
Usage: sudo bash scripts/update_debian.sh [options]

Options:
  --source <path>      Source project path (default: /azursoft/contribuyentes_SII_hub_server)
  --target <path>      Target deploy path (default: /opt/taxpayer_hub)
  --run-apt            Run apt-get install for base packages before update
  --skip-pip           Skip pip install -r requirements.txt
  -h, --help           Show this help

Examples:
  sudo bash scripts/update_debian.sh
  sudo bash scripts/update_debian.sh --source /srv/contribuyentes_SII_hub_server --target /opt/taxpayer_hub
  sudo bash scripts/update_debian.sh --run-apt
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --source)
      SOURCE_DIR="$2"
      shift
      ;;
    --target)
      TARGET_DIR="$2"
      VENV_DIR="$TARGET_DIR/.venv"
      ENV_FILE="$TARGET_DIR/.env"
      shift
      ;;
    --run-apt)
      RUN_APT=1
      ;;
    --skip-pip)
      SKIP_PIP=1
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
  shift
done

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root: sudo bash scripts/update_debian.sh"
  exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
  echo "Source directory not found: $SOURCE_DIR"
  exit 1
fi

if [ ! -d "$TARGET_DIR" ]; then
  echo "Target directory not found: $TARGET_DIR"
  echo "Install first with: sudo bash scripts/install_debian.sh"
  exit 1
fi

if [ "$RUN_APT" = "1" ]; then
  echo "[1/7] Installing system packages"
  apt-get update
  apt-get install -y python3 python3-venv python3-pip rsync curl
else
  echo "[1/7] Skipping system package install"
fi

echo "[2/7] Syncing project files"
rsync -av --delete \
  --exclude '.venv/' \
  --exclude '.env' \
  --exclude '.git/' \
  "$SOURCE_DIR/" "$TARGET_DIR/"

echo "[3/7] Fixing ownership"
chown -R "$APP_USER:$APP_GROUP" "$TARGET_DIR"

if [ ! -d "$VENV_DIR" ]; then
  echo "[4/7] Creating virtual environment"
  sudo -u "$APP_USER" "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  echo "[4/7] Reusing existing virtual environment"
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "[5/7] Creating .env from .env.example"
  cp "$TARGET_DIR/.env.example" "$ENV_FILE"
  chown "$APP_USER:$APP_GROUP" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
else
  echo "[5/7] Keeping existing .env"
fi

if [ "$SKIP_PIP" = "1" ]; then
  echo "[6/7] Skipping pip install"
else
  echo "[6/7] Installing Python dependencies"
  sudo -u "$APP_USER" "$VENV_DIR/bin/python" -m pip install --upgrade pip
  sudo -u "$APP_USER" "$VENV_DIR/bin/python" -m pip install -r "$TARGET_DIR/requirements.txt"
fi

echo "[7/7] Restarting service"
systemctl daemon-reload
systemctl restart "$SERVICE_NAME"
systemctl status "$SERVICE_NAME" --no-pager

echo "Done. Helpful commands:"
echo "  sudo journalctl -u $SERVICE_NAME -f"
echo "  sudo journalctl -u $SERVICE_NAME -n 120 --no-pager"
