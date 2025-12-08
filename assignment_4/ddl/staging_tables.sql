-- 1. Staging Customers
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id     INTEGER,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    signup_date     DATE,
    country         VARCHAR(100)
);


-- 2. Staging Products
CREATE TABLE IF NOT EXISTS stg_products (
    product_id      INTEGER,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    unit_price      NUMERIC(10,2)
);

-- 3. Staging Orders
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id        INTEGER,
    order_timestamp TIMESTAMP,
    customer_id     INTEGER,
    product_id      INTEGER,
    quantity        INTEGER,
    total_amount    NUMERIC(10,2),
    currency        VARCHAR(10),
    status          VARCHAR(50)
);
