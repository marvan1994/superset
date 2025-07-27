# Start from the official Apache Superset image
FROM apache/superset:latest

# Switch to root user to install packages
USER root

# Install the DuckDB database driver for Superset
# The sqlalchemy-duckdb package allows SQLAlchemy (used by Superset) to talk to DuckDB.
RUN pip install duckdb-engine

COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Switch back to the non-privileged superset user
USER superset
ENTRYPOINT ["/app/entrypoint.sh"]

