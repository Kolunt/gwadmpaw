#!/bin/bash
# Run once on the server: sudo bash deploy/setup-nginx-only.sh
set -euo pipefail
APP_DIR="/home/deploy/gwadm"
DOMAIN="gwadm.ru"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run: sudo bash deploy/setup-nginx-only.sh"
  exit 1
fi

install -m 644 "${APP_DIR}/deploy/nginx-gwadm.conf" /etc/nginx/sites-available/gwadm
ln -sf /etc/nginx/sites-available/gwadm /etc/nginx/sites-enabled/gwadm
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx

if getent ahostsv4 "${DOMAIN}" | grep -q .; then
  apt-get update -qq
  apt-get install -y -qq certbot python3-certbot-nginx
  certbot --nginx -d "${DOMAIN}" -d "www.${DOMAIN}" \
    --non-interactive --agree-tos --register-unsafely-without-email --redirect || true
  systemctl reload nginx
else
  echo "DNS for ${DOMAIN} not ready. After A-record -> this server IP, run:"
  echo "  sudo certbot --nginx -d ${DOMAIN} -d www.${DOMAIN}"
fi

echo "nginx OK for ${DOMAIN}"
