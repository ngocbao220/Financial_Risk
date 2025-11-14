from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# =======================
# 1. Spark session
# =======================
spark = SparkSession.builder \
    .appName("BinanceRealtime") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =======================
# 2. Kafka bootstrap
# =======================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # hoặc localhost:9092 nếu chạy local

# =======================
# 3. Schema cho các stream
# =======================
trade_schema = StructType([
    StructField("e", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("m", StringType(), True)
])

ticker_schema = StructType([
    StructField("e", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("p", StringType(), True),
    StructField("P", StringType(), True),
    StructField("o", StringType(), True),
    StructField("h", StringType(), True),
    StructField("l", StringType(), True),
    StructField("c", StringType(), True),
    StructField("v", StringType(), True),
    StructField("q", StringType(), True)
])

orderbook_schema = StructType([
    StructField("lastUpdateId", LongType(), True),
    StructField("bids", StringType(), True),
    StructField("asks", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True)
])

# =======================
# 4. Read Kafka streams
# =======================
def read_kafka_stream(topic, schema):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    df = df.selectExpr("CAST(value AS STRING) as json_str") \
           .select(from_json(col("json_str"), schema).alias("data")) \
           .select("data.*")
    return df

trade_df = read_kafka_stream("trade_stream", trade_schema)
ticker_df = read_kafka_stream("ticker_stream", ticker_schema)
orderbook_df = read_kafka_stream("orderbook_stream", orderbook_schema)

# =======================
# 5. Convert timestamps
# =======================
trade_df = trade_df.withColumn("ts", (col("E")/1000).cast(TimestampType()))
ticker_df = ticker_df.withColumn("ts", (col("E")/1000).cast(TimestampType()))
orderbook_df = orderbook_df.withColumn("ts", (col("E")/1000).cast(TimestampType()))

# =======================
# 6. Write to ClickHouse
# =======================
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

def write_to_clickhouse(df, table_name, checkpoint_location):
    return df.writeStream \
        .format("jdbc") \
        .option("driver", CLICKHOUSE_DRIVER) \
        .option("url", CLICKHOUSE_URL) \
        .option("dbtable", table_name) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()

# =======================
# 7. Start streams
# =======================
trade_query = write_to_clickhouse(trade_df, "trades", "/tmp/checkpoints/trade")
ticker_query = write_to_clickhouse(ticker_df, "tickers", "/tmp/checkpoints/ticker")
orderbook_query = write_to_clickhouse(orderbook_df, "orderbooks", "/tmp/checkpoints/orderbook")

# =======================
# 8. Await termination
# =======================
spark.streams.awaitAnyTermination()
