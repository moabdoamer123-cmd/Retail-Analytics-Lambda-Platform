import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin123")
CH_TABLE = "ecommerce.orders_realtime"
CHECKPOINT = "/tmp/checkpoints/orders_realtime"


schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("stock_code", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("country", StringType()),
    StructField("payment_method", StringType()),
    StructField("invoice_date", StringType()),
    StructField("event_time", StringType()),
])

spark = SparkSession.builder.appName("orders-streaming-to-clickhouse").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Read the stream from Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")      
    .option("maxOffsetsPerTrigger", 5000)
    .load()
)

# 2) Parse JSON + basic validation
orders = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .filter(col("order_id").isNotNull() & (col("total_amount") > 0))
)


# 3) Write each micro-batch to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    rows = batch_df.toJSON().collect()
    if not rows:
        return
    params = urllib.parse.urlencode({
        "query": "INSERT INTO {} FORMAT JSONEachRow".format(CH_TABLE),
        "date_time_input_format": "best_effort",
    })
    req = urllib.request.Request(
        "{}/?{}".format(CH_URL, params),
        data="\n".join(rows).encode("utf-8"),
        method="POST",
    )
    req.add_header("X-ClickHouse-User", CH_USER)
    req.add_header("X-ClickHouse-Key", CH_PASSWORD)
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
    print("batch {}: inserted {} rows".format(batch_id, len(rows)), flush=True)


query = (
    orders.writeStream
    .foreachBatch(write_to_clickhouse)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()