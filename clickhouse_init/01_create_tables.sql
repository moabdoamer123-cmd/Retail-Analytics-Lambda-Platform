CREATE DATABASE IF NOT EXISTS ecommerce;

-- BATCH path (first 70%): filled by spark_batch_to_hive_clickhouse.py (Hive -> ClickHouse)
CREATE TABLE IF NOT EXISTS ecommerce.orders_batch
(
    order_id        String,
    customer_id     String,
    stock_code      String,
    product_name    String,
    category        LowCardinality(String),
    price           Float64,
    quantity        Int32,
    total_amount    Float64,
    country         LowCardinality(String),
    payment_method  LowCardinality(String),
    invoice_date    DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(invoice_date)
ORDER BY (invoice_date, order_id);

-- STREAMING path (last 30%): filled by streaming_to_clickhouse.py (Kafka -> Spark Streaming -> ClickHouse)
CREATE TABLE IF NOT EXISTS ecommerce.orders_realtime
(
    order_id        String,
    customer_id     String,
    stock_code      String,
    product_name    String,
    category        LowCardinality(String),
    price           Float64,
    quantity        Int32,
    total_amount    Float64,
    country         LowCardinality(String),
    payment_method  LowCardinality(String),
    invoice_date    DateTime,   
    event_time      DateTime   
)
ENGINE = MergeTree
ORDER BY (event_time, order_id);

-- ONE table for Power BI: history + live data together
CREATE VIEW IF NOT EXISTS ecommerce.orders_all AS
SELECT order_id, customer_id, stock_code, product_name, category, price, quantity,
       total_amount, country, payment_method, invoice_date, 'batch' AS source
FROM ecommerce.orders_batch
UNION ALL
SELECT order_id, customer_id, stock_code, product_name, category, price, quantity,
       total_amount, country, payment_method, invoice_date, 'stream' AS source
FROM ecommerce.orders_realtime;
