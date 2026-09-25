#!/bin/bash
set -euo pipefail

APP_DIR="/home/deploy/gwadm"
DOMAIN="gwadm.ru"
DEPLOY_USER="deploy"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/setup.sh"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq nginx certbot python3-certbot-nginx

if [[ ! -x "${APP_DIR}/venv/bin/gunicorn" ]]; then
  echo "venv/gunicorn not found in ${APP_DIR}; run app setup first"
  exit 1
fi

install -m 644 "${APP_DIR}/deploy/nginx-gwadm.conf" /etc/nginx/sites-available/gwadm
ln -sf /etc/nginx/sites-available/gwadm /etc/nginx/sites-enabled/gwadm
rm -f /etc/nginx/sites-enabled/default

install -m 644 "${APP_DIR}/deploy/gwadm.service" /etc/systemd/system/gwadm.service
systemctl daemon-reload
systemctl enable gwadm
systemctl restart gwadm

nginx -t
systemctl reload nginx

if getent hosts "${DOMAIN}" >/dev/null; then
  certbot --nginx -d "${DOMAIN}" -d "www.${DOMAIN}" --non-interactive --agree-tos --register-unsafely-without-email --redirect || true
  systemctl reload nginx
else
  echo "WARN: DNS for ${DOMAIN} not ready; skipped certbot. Run later:"
  echo "  sudo certbot --nginx -d ${DOMAIN} -d www.${DOMAIN}"
fi

ufw allow OpenSSH >/dev/null 2>&1 || true
ufw allow 'Nginx Full' >/dev/null 2>&1 || true
ufw --force enable >/dev/null 2>&1 || true

echo "OK: gwadm service + nginx configured for ${DOMAIN}"
