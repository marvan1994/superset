# superset_config.py
import os
ROW_LIMIT = 5000
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
ENABLE_PROXY_FIX = True

# Иногда помогает это:
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
SQLALCHEMY_DATABASE_URI = f"sqlite:///{os.environ.get('SUPERSET_HOME', '/app/superset_home')}/superset.db"
