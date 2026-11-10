#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p reports
if [ -f .env ]; then
  echo '.env already exists; kept existing settings.'
  exit 0
fi
command -v openssl >/dev/null 2>&1 || { echo 'Install openssl first.' >&2; exit 1; }
umask 077
jwt_secret=$(openssl rand -hex 32)
pg_password=$(openssl rand -hex 24)
redis_password=$(openssl rand -hex 24)
sed -e "s/__JWT_SECRET__/$jwt_secret/" \
    -e "s/__POSTGRES_PASSWORD__/$pg_password/" \
    -e "s/__REDIS_PASSWORD__/$redis_password/" \
    -e "s/__LOCAL_UID__/$(id -u)/" \
    -e "s/__LOCAL_GID__/$(id -g)/" .env.example > .env
echo 'Generated .env with random secrets. Start: docker compose up --build -d'
