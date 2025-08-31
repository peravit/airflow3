# Airflow 3.0 Docker Setup

This project sets up Apache Airflow 3.0 using Docker containers.

## Quick Start

1. Set the Airflow user ID:
   ```bash
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

2. Initialize the database:
   ```bash
   docker-compose up airflow-init
   ```

3. Start all services:
   ```bash
   docker-compose up -d
   ```

4. Access the Airflow UI at http://localhost:8080
   - Username: airflow
   - Password: airflow

## Services

- **Webserver**: Airflow UI (port 8080)
- **Scheduler**: Task scheduling
- **Worker**: Task execution (Celery)
- **Triggerer**: Deferred task handling
- **Redis**: Message broker
- **PostgreSQL**: Metadata database
- **Flower**: Celery monitoring (port 5555, optional)

## Directory Structure

```
airflow3/
├── dags/           # DAG files
├── logs/           # Airflow logs
├── plugins/        # Custom plugins
├── config/         # Airflow configuration
├── Dockerfile      # Custom Airflow image
├── docker-compose.yml
├── requirements.txt
└── .env           # Environment variables
```

## Commands

- Start: `docker-compose up -d`
- Stop: `docker-compose down`
- View logs: `docker-compose logs -f [service_name]`
- CLI access: `docker-compose run --rm airflow-cli bash`