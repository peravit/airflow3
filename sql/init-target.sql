-- Target Database Setup
USE master;
GO

-- Create Target Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TargetDB')
BEGIN
    CREATE DATABASE TargetDB;
END
GO

USE TargetDB;
GO

-- Create Target Tables (Data Warehouse Schema)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_customers' AND xtype='U')
BEGIN
    CREATE TABLE dim_customers (
        customer_key INT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT NOT NULL,
        full_name NVARCHAR(101),
        email NVARCHAR(100),
        phone NVARCHAR(20),
        created_at DATETIME2,
        etl_created_at DATETIME2 DEFAULT GETDATE(),
        etl_updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_orders' AND xtype='U')
BEGIN
    CREATE TABLE fact_orders (
        fact_order_key INT IDENTITY(1,1) PRIMARY KEY,
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        product_name NVARCHAR(100),
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        order_date DATETIME2,
        status NVARCHAR(20),
        etl_created_at DATETIME2 DEFAULT GETDATE(),
        etl_updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Create ETL Control Table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='etl_control' AND xtype='U')
BEGIN
    CREATE TABLE etl_control (
        table_name NVARCHAR(50) PRIMARY KEY,
        last_etl_run DATETIME2,
        last_processed_id INT,
        status NVARCHAR(20),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Initialize ETL Control
IF NOT EXISTS (SELECT * FROM etl_control)
BEGIN
    INSERT INTO etl_control (table_name, last_etl_run, last_processed_id, status) VALUES
    ('customers', '1900-01-01', 0, 'ready'),
    ('orders', '1900-01-01', 0, 'ready');
END
GO