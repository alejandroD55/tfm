#!/usr/bin/env bash

set -euo pipefail

if [ -z "${DB_HOST:-}" ] || [ -z "${DB_PORT:-}" ] || [ -z "${DB_NAME:-}" ] || [ -z "${DB_USER:-}" ] || [ -z "${DB_PASSWORD:-}" ]; then
  echo "Missing required DB environment variables."
  echo "Required: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is not installed."
  exit 1
fi

echo "Running database migrations against ${DB_HOST}:${DB_PORT}/${DB_NAME}"
export PGPASSWORD="${DB_PASSWORD}"

psql \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --username "${DB_USER}" \
  --dbname "${DB_NAME}" \
  --set ON_ERROR_STOP=1 \
  --file database_schema.sql

echo "Database migrations completed successfully."
