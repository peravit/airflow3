from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load_customers():
    """Simple ETL: Extract from source and load to target"""
    try:
        # Connect to source
        source_hook = MsSqlHook(mssql_conn_id='mssql_source')
        
        # Extract data
        extract_sql = """
        SELECT 
            customer_id,
            first_name + ' ' + last_name as full_name,
            email,
            phone,
            created_at
        FROM customers
        """
        
        results = source_hook.get_records(extract_sql)
        print(f"Extracted {len(results)} customers from source")
        
        # Connect to target
        target_hook = MsSqlHook(mssql_conn_id='mssql_target')
        
        # Load data
        for row in results:
            customer_id, full_name, email, phone, created_at = row
            
            insert_sql = """
            IF NOT EXISTS (SELECT 1 FROM dim_customers WHERE customer_id = %s)
            BEGIN
                INSERT INTO dim_customers (customer_id, full_name, email, phone, created_at)
                VALUES (%s, %s, %s, %s, %s)
            END
            ELSE
            BEGIN
                UPDATE dim_customers 
                SET full_name = %s, email = %s, phone = %s, etl_updated_at = GETDATE()
                WHERE customer_id = %s
            END
            """
            
            target_hook.run(insert_sql, parameters=[customer_id, customer_id, full_name, email, phone, created_at, full_name, email, phone, customer_id])
        
        print(f"Loaded {len(results)} customers to target")
        return f"Successfully processed {len(results)} customers"
        
    except Exception as e:
        print(f"Error in ETL: {str(e)}")
        raise

with DAG(
    'simple_mssql_etl',
    default_args=default_args,
    description='Simple MSSQL to MSSQL ETL Demo',
    schedule=timedelta(hours=6),
    catchup=False,
    tags=['etl', 'mssql', 'simple'],
) as dag:

    # Test source connection
    test_source = MsSqlOperator(
        task_id='test_source_connection',
        mssql_conn_id='mssql_source',
        sql="SELECT 'Source OK' as status, COUNT(*) as customer_count FROM customers",
    )

    # Test target connection
    test_target = MsSqlOperator(
        task_id='test_target_connection',
        mssql_conn_id='mssql_target',
        sql="SELECT 'Target OK' as status, GETDATE() as timestamp",
    )

    # ETL Task
    etl_task = PythonOperator(
        task_id='extract_load_customers',
        python_callable=extract_and_load_customers,
    )

    # Verification
    verify_data = MsSqlOperator(
        task_id='verify_loaded_data',
        mssql_conn_id='mssql_target',
        sql="SELECT COUNT(*) as loaded_customers FROM dim_customers",
    )

    # Dependencies
    [test_source, test_target] >> etl_task >> verify_data