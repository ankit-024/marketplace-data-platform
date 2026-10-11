-- ==============================================================================
-- OLTP Database Initialization Script (PostgreSQL)
-- Simulates the transactional database of the marketplace.
-- Supports Debezium Logical Replication / CDC.
-- ==============================================================================

-- 1. Customers Table (Operational profile)
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    segment VARCHAR(50) NOT NULL,
    signup_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Sellers Table (Operational merchant registry)
CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    rating NUMERIC(3, 2) DEFAULT 4.00,
    is_top_seller BOOLEAN DEFAULT FALSE,
    registration_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Products Table (Catalog)
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(32) PRIMARY KEY,
    seller_id VARCHAR(32) REFERENCES sellers(seller_id),
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    cost NUMERIC(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    created_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Orders Table (Primary transaction)
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) REFERENCES customers(customer_id),
    seller_id VARCHAR(32) REFERENCES sellers(seller_id),
    product_id VARCHAR(32) REFERENCES products(product_id),
    order_ts TIMESTAMP NOT NULL,
    ingested_ts TIMESTAMP NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL,
    order_amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(50) NOT NULL, -- PENDING, COMPLETED, CANCELLED
    is_late_arrival BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Payments Table (Payment lifecycle)
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) REFERENCES orders(order_id),
    customer_id VARCHAR(32) REFERENCES customers(customer_id),
    payment_ts TIMESTAMP NOT NULL,
    payment_method VARCHAR(50) NOT NULL, -- UPI, CREDIT_CARD, etc.
    payment_amount NUMERIC(12, 2) NOT NULL,
    payment_status VARCHAR(50) NOT NULL, -- SUCCESS, FAILED, REFUNDED
    failure_reason VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Inventory Delta / Event Log
CREATE TABLE IF NOT EXISTS inventory_events (
    inventory_event_id VARCHAR(32) PRIMARY KEY,
    product_id VARCHAR(32) REFERENCES products(product_id),
    seller_id VARCHAR(32) REFERENCES sellers(seller_id),
    event_ts TIMESTAMP NOT NULL,
    quantity_delta INT NOT NULL,
    event_type VARCHAR(50) NOT NULL -- STOCK_RESERVED, STOCK_RESTOCKED
);

-- Performance Indexes for OLTP Queries
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_seller ON orders(seller_id);
CREATE INDEX IF NOT EXISTS idx_orders_ts ON orders(order_ts);
CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory_events(product_id);
