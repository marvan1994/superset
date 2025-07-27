#!/bin/bash
set -e
trap 'echo "🛑 Caught SIGTERM, exporting Superset data..."; superset export-all --path /app/superset_home/export || echo "Export failed"' SIGTERM

# Apply database migrations
superset db upgrade

# Create admin user if it doesn't exist
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@example.com \
    --password admin123 || true

# Initialize Superset
superset init


# 👉 Автоимпорт при старте
if [ -d "/app/superset_home/export" ] && [ "$(ls -A /app/superset_home/export)" ]; then
  echo "📥 Found export files, importing dashboards..."
  superset import-dashboards --path /app/superset_home/export || echo "Import failed"
else
  echo "📭 No exports found, skipping import"
fi

# Run the original Superset entrypoint (gunicorn server)
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
