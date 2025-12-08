
-- 1. Dimension Customers

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    signup_date     DATE,
    country         VARCHAR(100),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 2. Dimension Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    unit_price      NUMERIC(10,2),
    updated_at      TIMESTAMP DEFAULT NOW()
);
