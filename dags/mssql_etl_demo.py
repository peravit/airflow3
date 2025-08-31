from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_customers():
    """Extract customers from source MSSQL database"""
    source_hook = MsSqlHook(mssql_conn_id='mssql_source')
    
    sql = """
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        created_at,
        updated_at
    FROM customers 
    WHERE updated_at > (
        SELECT ISNULL(last_etl_run, '1900-01-01') 
        FROM TargetDB.dbo.etl_control 
        WHERE table_name = 'customers'
    )
    """
    
    df = source_hook.get_pandas_df(sql)
    print(f"Extracted {len(df)} customers")
    return df.to_json(orient='records')

def transform_customers(**context):
    """Transform customer data"""
    import json
    
    customers_json = context['task_instance'].xcom_pull(task_ids='extract_customers')
    customers = json.loads(customers_json)
    
    transformed = []
    for customer in customers:
        transformed.append({
            'customer_id': customer['customer_id'],
            'full_name': f"{customer['first_name']} {customer['last_name']}",
            'email': customer['email'],
            'phone': customer['phone'],
            'created_at': customer['created_at']
        })
    
    print(f"Transformed {len(transformed)} customers")
    return json.dumps(transformed)

def load_customers(**context):
    """Load customers into target MSSQL database"""
    import json
    
    customers_json = context['task_instance'].xcom_pull(task_ids='transform_customers')
    customers = json.loads(customers_json)
    
    target_hook = MsSqlHook(mssql_conn_id='mssql_target')
    
    for customer in customers:
        sql = """
        MERGE dim_customers AS target
        USING (SELECT %(customer_id)s AS customer_id, 
                      %(full_name)s AS full_name,
                      %(email)s AS email,
                      %(phone)s AS phone,
                      %(created_at)s AS created_at) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET 
                full_name = source.full_name,
                email = source.email,
                phone = source.phone,
                etl_updated_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (customer_id, full_name, email, phone, created_at)
            VALUES (source.customer_id, source.full_name, source.email, source.phone, source.created_at);
        """
        target_hook.run(sql, parameters=customer)
    
    print(f"Loaded {len(customers)} customers")

def extract_and_load_orders():
    """Extract orders and load into target (simple approach)"""
    source_hook = MsSqlHook(mssql_conn_id='mssql_source')
    target_hook = MsSqlHook(mssql_conn_id='mssql_target')
    
    # Extract
    extract_sql = """
    SELECT 
        order_id,
        customer_id,
        product_name,
        quantity,
        unit_price,
        (quantity * unit_price) as total_amount,
        order_date,
        status
    FROM orders 
    WHERE order_date > (
        SELECT ISNULL(last_etl_run, '1900-01-01') 
        FROM TargetDB.dbo.etl_control 
        WHERE table_name = 'orders'
    )
    """
    
    df = source_hook.get_pandas_df(extract_sql)
    print(f"Extracted {len(df)} orders")
    
    # Load
    if len(df) > 0:
        for _, row in df.iterrows():
            load_sql = """
            MERGE fact_orders AS target
            USING (SELECT %(order_id)s AS order_id) AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN
                UPDATE SET 
                    customer_id = %(customer_id)s,
                    product_name = %(product_name)s,
                    quantity = %(quantity)s,
                    unit_price = %(unit_price)s,
                    total_amount = %(total_amount)s,
                    order_date = %(order_date)s,
                    status = %(status)s,
                    etl_updated_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, product_name, quantity, unit_price, total_amount, order_date, status)
                VALUES (%(order_id)s, %(customer_id)s, %(product_name)s, %(quantity)s, %(unit_price)s, %(total_amount)s, %(order_date)s, %(status)s);
            """
            target_hook.run(load_sql, parameters=row.to_dict())
        
        print(f"Loaded {len(df)} orders")

def update_etl_control():
    """Update ETL control table with last run timestamp"""
    target_hook = MsSqlHook(mssql_conn_id='mssql_target')
    
    sql = """
    UPDATE etl_control 
    SET last_etl_run = GETDATE(), 
        status = 'completed',
        updated_at = GETDATE()
    WHERE table_name IN ('customers', 'orders')
    """
    
    target_hook.run(sql)
    print("Updated ETL control table")

with DAG(
    'mssql_etl_demo',
    default_args=default_args,
    description='ETL Demo: MSSQL Source to MSSQL Target',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['etl', 'mssql', 'demo'],
) as dag:

    # Test connections
    test_source_connection = MsSqlOperator(
        task_id='test_source_connection',
        mssql_conn_id='mssql_source',
        sql="SELECT 'Source DB Connected' AS status, GETDATE() AS timestamp",
    )

    test_target_connection = MsSqlOperator(
        task_id='test_target_connection',
        mssql_conn_id='mssql_target',
        sql="SELECT 'Target DB Connected' AS status, GETDATE() AS timestamp",
    )

    # Customer ETL Pipeline
    extract_customers_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers,
    )

    transform_customers_task = PythonOperator(
        task_id='transform_customers',
        python_callable=transform_customers,
    )

    load_customers_task = PythonOperator(
        task_id='load_customers',
        python_callable=load_customers,
    )

    # Orders ETL (simpler approach)
    extract_load_orders_task = PythonOperator(
        task_id='extract_load_orders',
        python_callable=extract_and_load_orders,
    )

    # Update ETL control
    update_control_task = PythonOperator(
        task_id='update_etl_control',
        python_callable=update_etl_control,
    )

    # Data quality checks
    quality_check_customers = MsSqlOperator(
        task_id='quality_check_customers',
        mssql_conn_id='mssql_target',
        sql="""
        SELECT 
            COUNT(*) as total_customers,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(CASE WHEN email IS NULL THEN 1 END) as missing_emails
        FROM dim_customers
        """,
    )

    quality_check_orders = MsSqlOperator(
        task_id='quality_check_orders',
        mssql_conn_id='mssql_target',
        sql="""
        SELECT 
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT customer_id) as customers_with_orders
        FROM fact_orders
        """,
    )

    # Task dependencies
    [test_source_connection, test_target_connection] >> extract_customers_task
    extract_customers_task >> transform_customers_task >> load_customers_task
    load_customers_task >> extract_load_orders_task
    extract_load_orders_task >> update_control_task
    update_control_task >> [quality_check_customers, quality_check_orders]