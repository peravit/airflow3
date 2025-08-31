-- Source Database Setup
USE master;
GO

-- Create Source Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SourceDB')
BEGIN
    CREATE DATABASE SourceDB;
END
GO

USE SourceDB;
GO

-- Create Source Tables
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
BEGIN
    CREATE TABLE customers (
        customer_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name NVARCHAR(50) NOT NULL,
        last_name NVARCHAR(50) NOT NULL,
        email NVARCHAR(100) UNIQUE,
        phone NVARCHAR(20),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='orders' AND xtype='U')
BEGIN
    CREATE TABLE orders (
        order_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT NOT NULL,
        product_name NVARCHAR(100) NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_amount AS (quantity * unit_price),
        order_date DATETIME2 DEFAULT GETDATE(),
        status NVARCHAR(20) DEFAULT 'pending',
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
END
GO

-- Insert Sample Data
IF NOT EXISTS (SELECT * FROM customers)
BEGIN
    INSERT INTO customers (first_name, last_name, email, phone) VALUES
    ('John', 'Doe', 'john.doe@email.com', '+1-555-0101'),
    ('Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102'),
    ('Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103'),
    ('Alice', 'Williams', 'alice.williams@email.com', '+1-555-0104'),
    ('Charlie', 'Brown', 'charlie.brown@email.com', '+1-555-0105');
END
GO

IF NOT EXISTS (SELECT * FROM orders)
BEGIN
    INSERT INTO orders (customer_id, product_name, quantity, unit_price, status) VALUES
    (1, 'Laptop', 1, 999.99, 'completed'),
    (1, 'Mouse', 2, 25.99, 'completed'),
    (2, 'Keyboard', 1, 75.50, 'pending'),
    (3, 'Monitor', 1, 299.99, 'shipped'),
    (3, 'Webcam', 1, 89.99, 'completed'),
    (4, 'Headphones', 1, 199.99, 'pending'),
    (5, 'Tablet', 1, 449.99, 'completed');
END
GO