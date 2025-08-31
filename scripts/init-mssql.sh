#!/bin/bash

echo "Waiting for MSSQL containers to be ready..."
sleep 30

echo "Initializing Source Database..."
docker exec airflow3-mssql-source-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -i /docker-entrypoint-initdb.d/init-source.sql

echo "Initializing Target Database..."
docker exec airflow3-mssql-target-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -i /docker-entrypoint-initdb.d/init-target.sql

echo "MSSQL databases initialized successfully!"