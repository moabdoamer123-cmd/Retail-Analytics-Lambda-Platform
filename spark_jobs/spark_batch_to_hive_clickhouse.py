import json
import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_common import spark_category_col, spark_payment_col

#  config (env vars)
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
RAW_PATH = os.getenv("RAW_PATH", HDFS_URI + "/user/hadoop/raw_batch/historical_orders_70.csv")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HIVE_METASTORE_VERSION = os.getenv("HIVE_METASTORE_VERSION", "") 
HIVE_DB = os.getenv("HIVE_DB", "ecommerce")
HIVE_TABLE = os.getenv("HIVE_TABLE", "orders_batch")
HIVE_TABLE_PATH = os.getenv("HIVE_TABLE_PATH", HDFS_URI + "/user/hive/warehouse/" + HIVE_DB + ".db/" + HIVE_TABLE)

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin123")
CH_TABLE = "ecommerce.orders_batch"

#  Spark session 
builder = (
    SparkSession.builder.appName("batch-historical-orders")
    .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
    .config("spark.sql.warehouse.dir", HDFS_URI + "/user/hive/warehouse")
)

if HIVE_METASTORE_VERSION:
    builder = (builder
               .config("spark.sql.hive.metastore.version", HIVE_METASTORE_VERSION)
               .config("spark.sql.hive.metastore.jars", "maven"))
spark = builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) read raw 
print("📥 Reading raw batch data from " + RAW_PATH)
raw = spark.read.option("header", "true").csv(RAW_PATH)          
print("   raw rows: {:,}".format(raw.count()))

# 2) clean + enrich 
typed = raw.select(
    F.col("Invoice").alias("order_id"),
    F.col("StockCode").alias("stock_code"),
    F.regexp_replace(F.col("Description"), r"^\s+|\s+$", "").alias("product_name"),
    F.col("Quantity").cast("int").alias("quantity"),
    F.to_timestamp(F.col("InvoiceDate")).alias("invoice_date"),
    F.col("Price").cast("double").alias("price"),
    F.col("`Customer ID`").cast("double").cast("long").cast("string").alias("customer_id"),   
    F.col("Country").alias("country"),
)

cleaned = typed.filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_name").isNotNull() & (F.col("product_name") != "")
    & F.col("invoice_date").isNotNull()
    & ~F.col("order_id").startswith("C")                      
    & (F.col("quantity") > 0) & (F.col("price") > 0)           
    & F.col("stock_code").rlike(r"^[0-9]{5}")                 
)

orders = (
    cleaned
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("price"), 2))
    .withColumn("category", spark_category_col(F.col("product_name")))
    .withColumn("payment_method", spark_payment_col(F.col("order_id")))
    .select("order_id", "customer_id", "stock_code", "product_name", "category",
            "price", "quantity", "total_amount", "country", "payment_method", "invoice_date")
    .cache()
)

total_rows = orders.count()
print("🧹 clean rows: {:,}".format(total_rows))

# 3) write to Hive 
spark.sql("CREATE DATABASE IF NOT EXISTS " + HIVE_DB)


spark.sql("DROP TABLE IF EXISTS {}.{}".format(HIVE_DB, HIVE_TABLE))
_jvm = spark._jvm
_path = _jvm.org.apache.hadoop.fs.Path(HIVE_TABLE_PATH)
_fs = _path.getFileSystem(spark._jsc.hadoopConfiguration())
if _fs.exists(_path):
    _fs.delete(_path, True)

(
    orders.write.mode("overwrite")
    .format("hive").option("fileFormat", "parquet")
    .option("path", HIVE_TABLE_PATH)                            
    .saveAsTable(HIVE_DB + "." + HIVE_TABLE)
)
print("🐝 Hive table {}.{} written ({:,} rows)".format(HIVE_DB, HIVE_TABLE, spark.table(HIVE_DB + "." + HIVE_TABLE).count()))

# 4) Hive -> ClickHouse 

def ch_call(query, body=None, settings=None):
    params = {"query": query}
    params.update(settings or {})
    req = urllib.request.Request("{}/?{}".format(CH_URL, urllib.parse.urlencode(params)),
                                 data=body if body is not None else b"", method="POST")
    req.add_header("X-ClickHouse-User", CH_USER)
    req.add_header("X-ClickHouse-Key", CH_PASSWORD)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8")


def make_partition_writer(url, user, password, table, chunk_size=10000):
    # Built with plain values so it can be shipped to the Spark workers.
    def write_partition(rows):
       
        import json
        import urllib.parse
        import urllib.request

        def flush(lines):
            params = urllib.parse.urlencode({"query": "INSERT INTO {} FORMAT JSONEachRow".format(table),
                                             "date_time_input_format": "best_effort"})
            req = urllib.request.Request("{}/?{}".format(url, params),
                                         data="\n".join(lines).encode("utf-8"), method="POST")
            req.add_header("X-ClickHouse-User", user)
            req.add_header("X-ClickHouse-Key", password)
            with urllib.request.urlopen(req, timeout=120) as resp:
                resp.read()

        buf = []
        for r in rows:
            buf.append(json.dumps(r.asDict()))
            if len(buf) >= chunk_size:
                flush(buf)
                buf = []
        if buf:
            flush(buf) 
    return write_partition


hive_orders = (
    spark.table(HIVE_DB + "." + HIVE_TABLE)
    .withColumn("invoice_date", F.date_format("invoice_date", "yyyy-MM-dd HH:mm:ss"))
)

ch_call("TRUNCATE TABLE IF EXISTS " + CH_TABLE)                 
hive_orders.coalesce(4).foreachPartition(
    make_partition_writer(CH_URL, CH_USER, CH_PASSWORD, CH_TABLE))

in_clickhouse = int(ch_call("SELECT count() FROM " + CH_TABLE).strip())
print("⚡ ClickHouse {} now has {:,} rows".format(CH_TABLE, in_clickhouse))
if in_clickhouse != total_rows:
    raise SystemExit("❌ Row count mismatch: Spark={} ClickHouse={}".format(total_rows, in_clickhouse))

print("✅ Spark Batch Job completed: HDFS -> Spark -> Hive -> ClickHouse")