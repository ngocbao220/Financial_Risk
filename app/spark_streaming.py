from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_json
from pyspark.sql.functions import lit, posexplode
from pyspark.sql.types import *

# ======================================================
# KHỞI TẠO SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("BinanceStreaming_MultiTopic")
    .config("spark.sql.caseSensitive", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================================================
# CẤU HÌNH KAFKA
# ======================================================
KAFKA_BROKER = "kafka:9092"
TOPIC_TRADES = "binance_trades"
TOPIC_TICKERS = "binance_tickers_1h"
TOPIC_ORDERBOOK = "binance_orderbook"

# ======================================================
# ĐỊNH NGHĨA CÁC SCHEMAS
# ======================================================

# Schema cho Topic 'binance_trades'
trade_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("t", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("b", LongType()),
    StructField("a", LongType()),
    StructField("T", LongType()),
    StructField("m", BooleanType())
])

# Schema cho Topic 'binance_tickers'
ticker_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("O", LongType()),
    StructField("C", LongType()),
    StructField("o", StringType()),
    StructField("h", StringType()),
    StructField("l", StringType()),
    StructField("c", StringType()),
    StructField("v", StringType()),
    StructField("q", StringType())
])

# Schema cho Topic 'binance_orderbook'
orderbook_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("b", ArrayType(ArrayType(StringType()))),
    StructField("a", ArrayType(ArrayType(StringType())))
])

# ======================================================
# LUỒNG 1: XỬ LÝ TRADES (TỪ TOPIC_TRADES)
# ======================================================
trades_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_TRADES)
    .option("startingOffsets", "latest")
    .load()
)

trades_df = (
    trades_raw_df
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .select(
        col("data.s").alias("Symbol"),
        col("data.t").alias("TradeID"),
        col("data.p").cast(DoubleType()).alias("Price"),
        col("data.q").cast(DoubleType()).alias("Quantity"),
        to_timestamp(col("data.E") / 1000).alias("EventTime"),
        to_timestamp(col("data.T") / 1000).alias("TradeTime"),
        col("data.m").alias("IsBuyerMaker")
    )
)

# ======================================================
# LUỒNG 2: XỬ LÝ TICKERS (TỪ TOPIC_TICKERS)
# ======================================================
tickers_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_TICKERS)
    .option("startingOffsets", "latest")
    .load()
)

tickers_df = (
    tickers_raw_df
    .select(from_json(col("value").cast("string"), ticker_schema).alias("data"))
    .select(
        col("data.s").alias("Symbol"),
        to_timestamp(col("data.O") / 1000).alias("OpenTime"),
        to_timestamp(col("data.C") / 1000).alias("CloseTime"),
        col("data.o").cast(DoubleType()).alias("OpenPrice"),
        col("data.h").cast(DoubleType()).alias("HighPrice"),
        col("data.l").cast(DoubleType()).alias("LowPrice"),
        col("data.c").cast(DoubleType()).alias("ClosePrice"),
        col("data.v").cast(DoubleType()).alias("BaseVolume"),
        col("data.q").cast(DoubleType()).alias("QuoteVolume"),
        to_timestamp(col("data.E") / 1000).alias("EventTime")
    )
)

# ======================================================
# LUỒNG 3: XỬ LÝ ORDERBOOK (TỪ TOPIC_ORDERBOOK)
# ======================================================
orderbook_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_ORDERBOOK)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON
parsed_orderbook_df = orderbook_raw_df.select(
    from_json(col("value").cast("string"), orderbook_schema).alias("data")
).select(
    col("data.s").alias("Symbol"),
    to_timestamp(col("data.E") / 1000).alias("EventTime"),
    col("data.b").alias("Bids"),
    col("data.a").alias("Asks")
)

# Explode Bids
bids_df = parsed_orderbook_df.select(
    "Symbol", "EventTime", posexplode(col("Bids")).alias("Level0", "Bid")
).select(
    "Symbol",
    "EventTime",
    lit("BID").alias("Side"),
    col("Bid")[0].cast(DoubleType()).alias("Price"),
    col("Bid")[1].cast(DoubleType()).alias("Qty"),
    (col("Level0") + 1).alias("Level")
)

# Explode Asks
asks_df = parsed_orderbook_df.select(
    "Symbol", "EventTime", posexplode(col("Asks")).alias("Level0", "Ask")
).select(
    "Symbol",
    "EventTime",
    lit("ASK").alias("Side"),
    col("Ask")[0].cast(DoubleType()).alias("Price"),
    col("Ask")[1].cast(DoubleType()).alias("Qty"),
    (col("Level0") + 1).alias("Level")
)

# Union cả hai
orderbook_df = bids_df.unionByName(asks_df)


# ======================================================
# VIẾT CÁC LUỒNG RA (GHI RA CONSOLE ĐỂ TEST)
# ======================================================
checkpoint_dir = "/tmp/spark_checkpoints"

query_trades = (
    trades_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{checkpoint_dir}/trades")
    .start()
)

query_tickers = (
    tickers_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{checkpoint_dir}/tickers")
    .start()
)

query_orderbook = (
    orderbook_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{checkpoint_dir}/orderbook")
    .start()
)

spark.streams.awaitAnyTermination()