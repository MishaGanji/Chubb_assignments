-- Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id               INTEGER PRIMARY KEY,
    order_timestamp        TIMESTAMP,
    customer_id            INTEGER,
    product_id             INTEGER,
    quantity               INTEGER,
    total_amount_usd       NUMERIC(10,2),
    currency_mismatch_flag BOOLEAN,
    status                 VARCHAR(50),
    updated_at             TIMESTAMP DEFAULT NOW(),
);