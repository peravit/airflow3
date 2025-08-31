#!/usr/bin/env python3
"""
Script to create MSSQL connections in Airflow via CLI
Run this after Airflow is started
"""

import subprocess
import time

def create_connection(conn_id, conn_type, host, login, password, schema, port, extra=""):
    """Create an Airflow connection using CLI"""
    cmd = [
        "docker", "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "connections", "add", conn_id,
        "--conn-type", conn_type,
        "--conn-host", host,
        "--conn-login", login,
        "--conn-password", password,
        "--conn-schema", schema,
        "--conn-port", str(port)
    ]
    
    if extra:
        cmd.extend(["--conn-extra", extra])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print(f"✅ Created connection: {conn_id}")
        else:
            print(f"❌ Failed to create {conn_id}: {result.stderr}")
    except subprocess.TimeoutExpired:
        print(f"⏱️ Timeout creating {conn_id}")
    except Exception as e:
        print(f"❌ Error creating {conn_id}: {e}")

def main():
    print("Creating MSSQL connections...")
    
    # Wait for scheduler to be ready
    print("Waiting for Airflow scheduler to be ready...")
    time.sleep(10)
    
    # Create source connection
    create_connection(
        conn_id="mssql_source",
        conn_type="mssql",
        host="mssql-source",
        login="sa",
        password="YourStrong@Passw0rd",
        schema="SourceDB",
        port=1433,
        extra='{"TrustServerCertificate": "yes"}'
    )
    
    # Create target connection  
    create_connection(
        conn_id="mssql_target",
        conn_type="mssql",
        host="mssql-target",
        login="sa", 
        password="YourStrong@Passw0rd",
        schema="TargetDB",
        port=1433,
        extra='{"TrustServerCertificate": "yes"}'
    )
    
    print("✅ Connection setup complete!")

if __name__ == "__main__":
    main()