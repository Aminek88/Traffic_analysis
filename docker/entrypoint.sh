#!/bin/bash
until pg_isready -h postgres -p 5432 -U airflow; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done
echo "PostgreSQL is ready, proceeding with Airflow initialization."
airflow db init
chown 50000:0 /opt/airflow/output
chmod 775 /opt/airflow/output
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver & airflow scheduler