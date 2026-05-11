#!/bin/bash
set -e

echo "Waiting for PostgreSQL..."

until pg_isready -h db -p 5432 -U dataengineeringuser; do
  echo "Postgres is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL started"

echo "Installing Python requirements..."
pip install --no-cache-dir -r /opt/airflow/requirements.txt

echo "Initializing Airflow DB..."
airflow db init

echo "Creating admin user..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Starting Airflow webserver..."
exec airflow webserver