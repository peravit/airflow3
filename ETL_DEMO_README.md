# MSSQL ETL Demo with Airflow 3.0

This demo shows how to build ETL pipelines from MSSQL to MSSQL using Apache Airflow 3.0.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Source    │───▶│   Airflow   │───▶│   Target    │
│   MSSQL     │    │   3.0.1     │    │   MSSQL     │
│ (Port 1433) │    │             │    │ (Port 1434) │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Database Setup

### Source Database (SourceDB)
- **Tables**: `customers`, `orders`  
- **Port**: 1433
- **Sample data**: 5 customers, 7 orders

### Target Database (TargetDB)
- **Tables**: `dim_customers`, `fact_orders`, `etl_control`
- **Port**: 1434
- **Schema**: Data warehouse format

## Connection Methods

### 1. Environment Variables (Recommended)
Connections are automatically created from environment variables:

```bash
AIRFLOW_CONN_MSSQL_SOURCE=mssql+pyodbc://sa:YourStrong%40Passw0rd@mssql-source:1433/SourceDB?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
AIRFLOW_CONN_MSSQL_TARGET=mssql+pyodbc://sa:YourStrong%40Passw0rd@mssql-target:1433/TargetDB?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

### 2. Airflow UI Connections
Go to Admin → Connections → Add Connection:

**Source Connection:**
- Connection ID: `mssql_source`
- Connection Type: `Microsoft SQL Server`
- Host: `mssql-source`
- Database: `SourceDB`
- Login: `sa`
- Password: `YourStrong@Passw0rd`
- Port: `1433`
- Extra: `{"TrustServerCertificate": "yes", "driver": "ODBC Driver 18 for SQL Server"}`

**Target Connection:**
- Connection ID: `mssql_target`  
- Connection Type: `Microsoft SQL Server`
- Host: `mssql-target`
- Database: `TargetDB`
- Login: `sa`
- Password: `YourStrong@Passw0rd`
- Port: `1433`
- Extra: `{"TrustServerCertificate": "yes", "driver": "ODBC Driver 18 for SQL Server"}`

## ETL Pipeline Features

### DAG: `mssql_etl_demo`
1. **Connection Tests**: Verify source and target connectivity
2. **Customer ETL**: Extract → Transform → Load with full ETL pattern
3. **Order ETL**: Simplified extract and load
4. **Control Table**: Track last ETL run timestamps
5. **Quality Checks**: Validate data after load

### Key ETL Patterns
- **Incremental Loading**: Only process new/updated records
- **MERGE Operations**: Upsert logic for data warehousing
- **Error Handling**: Retries and failure notifications
- **Data Quality**: Automated validation checks

## Commands

### Start Everything
```bash
# Build with MSSQL support
docker compose build --no-cache

# Start all services (including MSSQL databases)
docker compose up -d

# Initialize MSSQL databases
./scripts/init-mssql.sh
```

### Monitor
```bash
# Check container status
docker compose ps

# View logs
docker compose logs mssql-source
docker compose logs mssql-target
docker compose logs airflow-apiserver
```

### Database Access
```bash
# Connect to source database
docker exec -it airflow3-mssql-source-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd"

# Connect to target database  
docker exec -it airflow3-mssql-target-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd"
```

## Production Deployment

### Security Best Practices
1. **Environment Variables**: Store all secrets in `.env` files
2. **Strong Passwords**: Replace default passwords
3. **Network Security**: Use internal networks, restrict ports
4. **Encryption**: Enable SSL/TLS for database connections

### Production Checklist
- [ ] Update passwords in production `.env`
- [ ] Configure backup strategies for both databases
- [ ] Set up monitoring and alerting
- [ ] Configure log retention policies
- [ ] Test failover scenarios
- [ ] Document connection details securely

## Troubleshooting

### Common Issues
1. **Connection Failed**: Check if containers are healthy
2. **ODBC Driver**: Verify driver installation in Dockerfile
3. **SSL Issues**: Use `TrustServerCertificate=yes` for dev
4. **Port Conflicts**: Ensure ports 1433/1434 are available

### Debug Commands
```bash
# Test MSSQL connectivity
docker exec airflow3-mssql-source-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -Q "SELECT @@VERSION"

# Check Airflow logs
docker compose logs airflow-apiserver | grep -i error
```